from RSS import RSS
import pandas as pd
import datetime
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import time
import threading

#https://algotrading101.com/learn/interactive-brokers-python-api-native-guide/

# run code until
alarm = datetime.time(12, 0, 0)

class NoHaltsLeftError(Exception):
    """ Thrown when all halts in the RSS are taken care of"""
    pass

rss = RSS(True)

# creates contracts
def createContracts(halt_list):
    contract_list = []
    for i in halt_list:
        contract = Contract()
        contract.symbol = str(i)
        contract.secType = 'STK'
        contract.exchange = 'SMART'
        contract.currency = 'USD'
        contract_list.append(contract)
    return contract_list


#print(market_data_df)
def runStrategy():
    rss.fetch_halts()
    print(rss.halts_current)
    contract_list = createContracts(rss.halts_current.index)
    app = IBapi()
    app.connect('127.0.0.1', 7496, 123)

    def run_loop():
        app.run()

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    time.sleep(1)  # Sleep interval to allow time for connection to server

    # Create contract object
    req_id = 1
    for i in contract_list:
        app.reqMktData(req_id, i, '', False, False, [])
        req_id+=1

    time.sleep(10)  # Sleep interval to allow time for incoming price data
    app.disconnect()


class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)

    def tickPrice(self, reqId, tickType, price, attrib):
        if tickType == 2 and reqId == 1:
            print('The current ask price is: ', price)

if __name__ == '__main__':
    while datetime.datetime.now().time() < alarm:
        runStrategy()


