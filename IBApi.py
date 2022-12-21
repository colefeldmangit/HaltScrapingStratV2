from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import *
import pandas as pd


class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        # next valid order id
        self.next_order_id = None

        # bool to keep track of any lookup errors
        self.lookup_error = False

        # dict that maps req_id's to their corresponding symbols and halt times (tuple(id, time))
        self.symbol_dict = {}

        # dict that maps symbol to open positions
        self.position_dict = {}

        # dict that maps req_id's to their corresponding contracts
        self.contract_dict = {}

        # dict that maps req_id's to last price
        self.price_dict = {}

        # dict that maps req_id's to order id's
        self.order_dict = {}
        self.open_order_list = []

        # dict that maps req_id's to order placement times for flattening purposes
        self.flatten_time_dict = {}

        # list of ids remaining
        self.ids_remaining = []

        # dict of dataframes that hold historical volume data
        self.historical_data_dict = {}

    def nextValidId(self, order_id):
        super().nextValidId(order_id)
        self.next_order_id = order_id
        print('The next valid order id is: ', self.next_order_id)

    def createContracts(self, halt_list):
        req_id = 1
        for i in halt_list.index:
            contract = Contract()
            contract.symbol = str(i)
            contract.secType = 'STK'
            contract.exchange = 'SMART'
            contract.currency = 'USD'
            # appends the contract to a list with an ID #
            self.contract_dict[req_id] = contract
            self.ids_remaining.append(req_id)
            req_id += 1

    def historicalData(self, req_id, bar):
        # print(f'Time: {bar.date} Volume: {bar.volume}')
        if req_id not in self.historical_data_dict.keys():
            self.historical_data_dict[req_id] = pd.DataFrame(columns=['date', 'open', 'close', 'volume'])
        self.historical_data_dict[req_id].loc[len(self.historical_data_dict[req_id])] = [pd.to_datetime(bar.date),
                                                                                         bar.open, bar.close,
                                                                                         bar.volume]

    def tickPrice(self, req_id, tick_type, price, attrib):
        if tick_type == 4:
            self.price_dict[req_id] = price

    def orderStatus(self, order_id, status, filled, remaining, avg_fill_price, perm_id, parent_id, last_fill_price,
                    client_id, why_held, mkt_cap_price):
        print('orderStatus - orderid:', order_id, 'status:', status, 'filled', filled, 'remaining', remaining,
              'lastFillPrice', last_fill_price)

    def openOrder(self, order_id, contract, order, order_state):
        print('openOrder id:', order_id, contract.symbol, contract.secType, '@', contract.exchange, ':', order.action,
              order.orderType, order.totalQuantity, 'price', order.lmtPrice, order_state.status)
        if order_id not in self.open_order_list:
            self.open_order_list.append(order_id)

    def execDetails(self, req_id, contract, execution):
        print('Order Executed: ', req_id, contract.symbol, contract.secType, contract.currency, execution.execId,
              execution.orderId, execution.shares, execution.lastLiquidity)
        if req_id not in self.position_dict.keys():
            self.position_dict[contract.symbol] = execution.shares
        else:
            self.position_dict[contract.symbol] += execution.shares

    def position(self, account, contract, position, avg_cost):
        super().position(account, contract, position, avg_cost)
        print("Position.", "Account:", account, "Symbol:", contract.symbol, "SecType:",
              contract.secType, "Currency:", contract.currency, "Position:", position,
              "Avg cost:", avg_cost)

    '''def error(self, req_id, error_code, error_string):
        # TODO: make no ticker found remove from RSS/contract list, reqContractDetails
        if error_code == 200:
            print('No ticker found!')
            self.lookup_error = True
        if error_code == 202:
            print('Order cancelled.')'''
