from RSS import RSS
import pandas as pd
import datetime
import time
import threading
from IBApi import IBapi
from ibapi.order import *
from math import ceil
import logging

# https://algotrading101.com/learn/interactive-brokers-python-api-native-guide/

print(datetime.datetime.now())
rss = RSS(False)


def runStrategy():
    # Create contract object
    app = IBapi()

    # populates contract dict and ids remaining list
    app.createContracts(rss.halts_current)

    start = datetime.datetime.now()

    # Live login
    # app.connect('127.0.0.1', 7496, 123)

    # Sim login
    app.connect('127.0.0.1', 7497, 123)

    def run_loop():
        app.run()

    # number of tickers
    print("Number of tickers to process: " + str(len(app.ids_remaining)))

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    time.sleep(1)  # Sleep interval to allow time for connection to server

    for i in app.ids_remaining:
        # creates a tuple containing symbol and halt time
        symbol = app.contract_dict[i].symbol
        contract = app.contract_dict[i]
        app.symbol_dict[i] = (symbol, rss.halts_current.loc[symbol]['time'])
        app.reqHistoricalData(i, contract, '', '1 D', '1 min', 'TRADES', 0, 1, False, [])

    # allows for historical data to come through
    time.sleep(len(app.ids_remaining))

    # TODO: Change this to validate contracts for speed
    ids_to_remove = []
    for i in app.ids_remaining:
        if i not in app.historical_data_dict.keys():
            print("Cannot find historical data for " + app.symbol_dict[i][0])
            ids_to_remove.append(i)
    for i in ids_to_remove:
        app.ids_remaining.remove(i)

    # check volume data before halt, finally check if halt time is still within 5 mins
    for i in app.historical_data_dict.keys():
        symbol = app.symbol_dict[i][0]
        halt_time = rss.halts_current.loc[symbol]['time']
        halt_minute = halt_time.strftime("%Y-%m-%d %H:%M")
        hist_data = app.historical_data_dict[i].set_index('date')
        # find the halt price from historical
        try:
            print(str(symbol) + ": Historical data:")
            prev_minute_open_price = hist_data['open'].loc[:halt_minute][-2]
            halt_open_price = hist_data['open'].loc[halt_minute]
            print("Number of halts prior: " + str(rss.halt_counter_dict[symbol]))
            print("Minute prior open: " + str(prev_minute_open_price))
            print("Halt minute open: " + str(halt_open_price))
            halt_price = hist_data['close'].loc[halt_minute]
            did_halt_up = True if (halt_price - prev_minute_open_price) > 0 else False
            print(str(symbol) + " halted up: " + str(did_halt_up) + ", Halted at: " + str(halt_price))
            rss.halts_current.loc[app.symbol_dict[i][0], 'halt_price'] = halt_price
            vol_before_halt = (hist_data['volume'].loc[halt_time - datetime.timedelta(minutes=5):
                                                       halt_time + datetime.timedelta(minutes=1)]).sum()
            dollar_vol_before_halt = vol_before_halt * halt_price * 100
            print("Ticker: " + str(symbol) + ", Dollar volume before halt: " + str(dollar_vol_before_halt))
            # 100*n volume
            if dollar_vol_before_halt < 1000000 or halt_time < (datetime.datetime.now() - datetime.timedelta(minutes=5)) \
                    or (not did_halt_up) or halt_price > 20.01 or rss.halt_counter_dict[symbol] > 2:
                # removes the halts from the rss
                reason = ""
                if halt_time < datetime.datetime.now() - datetime.timedelta(minutes=5):
                    reason = "> 5 Mins"
                elif dollar_vol_before_halt < 1000000:
                    reason = "Low Volume"
                elif not did_halt_up:
                    reason = "Halted Down"
                elif halt_price > 20.01:
                    reason = "Price too high"
                elif rss.halt_counter_dict[symbol] > 2:
                    reason = "Too many halts"
                rss.remove_halt(symbol, reason)
                del app.contract_dict[i]
                app.ids_remaining.remove(i)
            else:
                # sets app price data to halted price
                app.price_dict[i] = rss.halts_current.loc[app.symbol_dict[i][0]]['halt_price']
        except IndexError:
            reason = "No price data before halt: IndexError"
            rss.remove_halt(symbol, reason)
            del app.contract_dict[i]
            app.ids_remaining.remove(i)
        except KeyError:
            reason = "No price data before halt: KeyError"
            rss.remove_halt(symbol, reason)
            del app.contract_dict[i]
            app.ids_remaining.remove(i)
    # requests the market data from IB
    for i in app.ids_remaining:
        contract = app.contract_dict[i]
        app.reqMktData(i, contract, '', False, False, [])

    # while loop until all halts are processed
    while len(app.ids_remaining) != 0:
        # can mess with this if latency issues
        time.sleep(.01)
        for i in app.ids_remaining:
            symbol = app.symbol_dict[i][0]
            last = app.price_dict[i]
            halt_price = rss.halts_current.loc[app.symbol_dict[i][0]]['halt_price']
            try:
                print("Flatten Time: " + str(app.flatten_time_dict[i]))
            except KeyError:
                pass
            # check if order has been placed for current ticker
            if i in app.flatten_time_dict.keys():
                if datetime.datetime.now() > app.flatten_time_dict[i]:
                    app.reqPositions()
                    for o in app.order_dict[i]:
                        if o in app.open_order_list:
                            app.cancelOrder(o)
                    # place a market order to get out of open positions
                    if app.position_dict.get(app.symbol_dict[i][0]):
                        market_out = Order()
                        market_out.action = "SELL" if app.position_dict[app.symbol_dict[i][0]] > 0 else "BUY"
                        market_out.totalQuantity = app.position_dict[app.symbol_dict[i][0]]
                        market_out.orderType = "MKT"
                        market_out.orderId = app.next_order_id
                        app.next_order_id += 1
                        market_out.transmit = True
                        app.placeOrder(market_out.orderId, app.contract_dict[i], market_out)
                    app.ids_remaining.remove(i)
                    print("Removing Ticker: " + str(symbol) + ". Two minutes have passed.")
            else:
                if last < halt_price:
                    print("Unhalted: Gap Down")
                    app.ids_remaining.remove(i)
                    app.cancelMktData(i)
                # case where it gaps up
                elif last > halt_price:
                    print("Unhalted: Gap Up")
                    if last < 1.01 * halt_price:
                        print("Gap too small.")
                        app.ids_remaining.remove(i)
                        app.cancelMktData(i)
                    else:
                        # can change this to be any percent of the gap
                        entry_level_ = round(halt_price + .2 * (last - halt_price), 2)
                        profit_level_ = round(last * 1.01, 2)
                        stop_level_ = round(halt_price * .995, 2)
                        print("Entry level: " + str(entry_level_))
                        print("Profit level: " + str(entry_level_))
                        print("Stop level:  " + str(stop_level_))
                        quantity_ = 4
                        bracket = bracket_order(app=app, action="BUY", quantity=quantity_, entry_level=entry_level_,
                                                profit_level=profit_level_, stop_level=stop_level_)

                        app.placeOrder(bracket[0].orderId, app.contract_dict[i], bracket[0])
                        app.placeOrder(bracket[1].orderId, app.contract_dict[i], bracket[1])
                        app.placeOrder(bracket[2].orderId, app.contract_dict[i], bracket[2])

                        app.order_dict[i] = []
                        app.order_dict[i].append(bracket[0].orderId)
                        app.order_dict[i].append(bracket[1].orderId)
                        app.order_dict[i].append(bracket[2].orderId)

                        app.flatten_time_dict[i] = datetime.datetime.now() + datetime.timedelta(minutes=2)
                    app.cancelMktData(i)
                elif last == halt_price:
                    print("Still Halted!")
    end = datetime.datetime.now()
    if (end - start).seconds < 270:
        print("Halts processed!")
        time.sleep(270 - int((end - start).seconds))
    # clear rss and disconnect the app
    app.disconnect()
    rss.remove_all_halts()


'''V3: After the open, keep track of relative max and min, reset min = max every time max is changed. Keep updating for
two minutes or until a trade is filled. Set the stop loss to a minimum price level between halt open and halt price 
(* 75%) so we can confirm a bounce. If minimum ever is set to below halt price, scrub trade. Set a profit target to 
2*(max - stop_loss) + max. Keep feeding in market data until order is filled or two minutes has passed after halt 
open. Flatten after five minutes if open position.'''


def bracket_order(app, action, quantity, entry_level, profit_level, stop_level):
    entry_order = Order()
    entry_order.action = "BUY"
    entry_order.totalQuantity = quantity
    entry_order.orderType = "LMT"
    entry_order.lmtPrice = entry_level
    entry_order.orderId = app.next_order_id
    app.next_order_id += 1
    entry_order.transmit = False

    profit_order = Order()
    profit_order.action = "SELL"
    profit_order.totalQuantity = ceil(3 / 4 * quantity)
    profit_order.orderType = "LMT"
    profit_order.lmtPrice = profit_level
    profit_order.orderId = app.next_order_id
    app.next_order_id += 1
    profit_order.parentId = entry_order.orderId
    profit_order.transmit = False

    stop_order = Order()
    stop_order.action = "SELL"
    stop_order.totalQuantity = quantity
    stop_order.orderType = "STP"
    stop_order.auxPrice = stop_level
    stop_order.orderId = app.next_order_id
    app.next_order_id += 1
    stop_order.parentId = entry_order.orderId
    stop_order.transmit = True

    return [entry_order, profit_order, stop_order]


if __name__ == '__main__':
    # during specific length of time
    while datetime.datetime.now().hour < 16:
        rss.fetch_halts()
        print(rss.halts_current.index)
        # only runs strategy if scraper picks up halts
        if len(rss.halts_current.index) != 0:
            print("Running strategy!")
            runStrategy()
        else:
            print("No halts found!")
            time.sleep(270)
        rss.remove_all_halts()
