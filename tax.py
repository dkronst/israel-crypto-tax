#!/usr/bin/env python3

import csv
from os.path import *

NOT_A_BFX_EXCHANGE = ['transfer', 'deposit', 'withdrawal']
YEAR_SECONDS = 365*24*3600
MIN_YEAR = 1451606400 + YEAR_SECONDS

PRACTICALLY_ZERO = 0.000001

def create_deposit_tx(asset, amount, date, time_stamp):
    from dateutil.parser import parse
    d = {}
    d['asset_base'] = "USD"
    d['asset_tgt'] = asset
    d['type'] = 'buy'
    p = asset_price_translator(asset)

    d['date'] = tx_date = parse(date)
    d['unix_time'] = time_stamp

    new_price = p[tx_date]['open']
    d['rate'] = new_price
    d['amount'] = amount*new_price
    d['augmented'] = "\u001b[34;1mDEPOSIT\u001b[0m"
    return d

def create_user_deposit(asset, amount, date, time_stamp):
    d = {}
    from dateutil.parser import parse
    d = {}
    d['asset_base'] = "USD"
    d['asset_tgt'] = asset
    d['type'] = 'buy'
    p = input("Enter price in USD for {} on {} for an amount of {}: ".format(asset, date, amount))

    d['date'] = tx_date = parse(date)
    d['unix_time'] = time_stamp

    new_price = float(p)
    d['rate'] = new_price
    d['amount'] = amount*new_price
    d['augmented'] = "\u001b[34;1mUSER-DEPOSIT\u001b[0m"
    return d

def bfx_other_tx(line):
    try:
        if line[3].startswith("Deposit") and line[0] != "USD":
            return create_deposit_tx(line[0], float(line[1]), line[-1], float(line[-2]))
    except Exception as e:
        print("Unable to understand deposit tx. TX data: {}.".format(line))
        return create_user_deposit(line[0], float(line[1]), line[-1], float(line[-2]))

def bitfinex_tx_translate(line):
    if not line[3].startswith("Exchange"):
        return bfx_other_tx(line)

    exchange_details = line[3].split(" ")
    
    d = {}
    d['asset_base'] = exchange_details[4]
    d['asset_tgt'] = exchange_details[2]
    amount = float(line[1])
    d['amount'] = abs(amount)   # Amount of base asset used
    d['type'] = "buy" if amount < 0 else "sell"
    rate = line[3].split("@")[-1].strip().split(" ")[0]
    d['rate'] = float(rate)   # base/target e.g. USD/BTC, BTC/DASH
    d['date'] = line[-1]
    d['unix_time'] = float(line[-2])
    d['augmented'] = False  # Augmented means that the tx is an added swap tx (e.g. buy dash with btc)
    return d

def bitstamp_tx_translate(line):
    from dateutil.parser import parse
    if line[0] != 'Market':
        return None
    d = {}
    d['asset_base'] = 'USD'
    d['asset_tgt'] = 'BTC'
    amount = float(line[4].split(" ")[0])
    d['type'] = line[-1].lower()
    d['rate'] = float(line[5].split(" ")[0])
    d['date'] = line[1]
    d['unix_time'] = parse(line[1]).timestamp()
    d['augmented'] = False
    d['amount'] = float(line[3].split(" ")[0])*d['rate']
    return d


def exchange_transactions(csv_filename, translate_fn):
    with open(csv_filename, "rt") as r:
        csv_read = csv.reader(r)
        for line in reversed(list(csv_read)):
            l = translate_fn(line)
            if l:
                yield l

def bitfinex_transactions(csv_filename):
    return exchange_transactions(csv_filename, bitfinex_tx_translate)

def bitstamp_transactions(csv_filename):
    return exchange_transactions(csv_filename, bitstamp_tx_translate)

def apply_sell_transaction(tx, fifo):
    a = tx["amount"]/tx["rate"]
    r = tx["rate"]

    profit = 0.0

    print("apply [{}]:".format(tx['asset_tgt']), fifo[0] if fifo else None, a, r)
    s = False

    #if tx['asset_tgt'] == 'BTC':
    #    print("\u001b[33;1mFIFO:\u001b[0m", fifo)

    while fifo and a > fifo[0][0]:
        f_a, f_r = fifo.pop(0)
        profit += f_a*(r-f_r)
        a -= f_a

    if fifo:  # It is guaranteed that a <= f_a if it exists.
        f_a, f_r = fifo.pop(0)
    else:
        if a > 0:
            print("{} fifo is empty. Assume price 0.0? [enter to continue, ctrl+c to end]".format(tx['asset_tgt']))
        f_a = a
        f_r = 0.0  # not ideal. Probably wrong too but probably means you are safe from a judicial perspective.
    
    profit += a*(r-f_r)
    f_a -= a
    
    if f_a > PRACTICALLY_ZERO:
        fifo.insert(0, (f_a, f_r))

    print("profit-final:", profit, tx['date'])

    return profit

def calculate_tax(pl, tax_rate, losses):
    tax = pl*tax_rate
    print("TAX:", tax, "with loss: {}".format(0 if losses <= 0 else "\u001b[31;1m{}\u001b[0m".format(losses)))
    return tax

def augment_transactions(orig_transactions):
    """
    augment the transactions with sell/buy transactions caused due to asset swap.
    I.e. A buy of an asset with another asset is a sell transaction of original asset and then buy with the
    secondary asset and vise versa.
    """
    for tx in orig_transactions:
        if tx['asset_base'] != 'USD':
            augmented_sell, augmented_buy = swap_tx_translate(tx)
            yield augmented_sell
            yield augmented_buy
        else:
            yield tx

def create_swap_buy(tx, new_price):
    # e.g. 
    sell_tx = tx.copy()
    sell_tx['asset_base'] = 'USD'
    sell_tx['asset_tgt'] = tx['asset_base']
    sell_tx['augmented'] = "\u001b[31;1mBUY\u001b[0m"
    sell_tx['rate'] = new_price
    sell_tx['type'] = 'sell'
    sell_tx['amount'] = tx['amount']*new_price
    
    buy_tx = sell_tx.copy()
    buy_tx['type'] = 'buy'
    buy_tx['asset_tgt'] = tx['asset_tgt']
    buy_tx['rate'] = new_price*tx['rate']

    return buy_tx, sell_tx

def create_swap_sale(tx, new_price):
    sell_tx = tx.copy()

    sell_tx['asset_base'] = 'USD'
    sell_tx['augmented'] = "\u001b[32;1mSELL\u001b[0m"
    sell_tx['rate'] = new_price*tx['rate']
    sell_tx['amount'] = tx['amount']*new_price
    sell_tx['type'] = 'sell'

    buy_tx = sell_tx.copy()
    buy_tx['asset_tgt'] = tx['asset_base']
    buy_tx['amount'] = tx['amount']*new_price
    buy_tx['rate'] = new_price
    buy_tx['type'] = 'buy'

    return sell_tx, buy_tx


def create_swap_transactions(tx, new_price):
    # e.g. Sell 1 DASH denominated in BTC.
    # So pretend a DASH sell tx denominated in USD with daily BTC price
    # then pretend to buy the same amount of BTC in USD.
    # OTOH: Buy 1 DASH denominated in BTC, pretend selling denominated BTC then buy DASH with that price in USD.
    if tx['type'] == 'sell':
        return create_swap_sale(tx, new_price)
    else:
        return create_swap_buy(tx, new_price)

def load_asset_hitsory(asset):
    import pickle
    p = pickle.load(open("{}-history.pickle".format(asset.lower()), "rb"))

    return p

def asset_price_translator(asset):
    # Translate asset price to a price in USD at a certain date.
    if not hasattr(asset_price_translator, "asset_prices"):
        p = load_asset_hitsory(asset)
        asset_price_translator.asset_prices = {}
        asset_price_translator.asset_prices[asset] = p
    elif asset in asset_price_translator.asset_prices:
        p = asset_price_translator.asset_prices[asset]
    else:
        p = load_asset_hitsory(asset)

    asset_price_translator.asset_prices[asset] = asset_price_translator.asset_prices.get(asset, p)
    return asset_price_translator.asset_prices[asset]

def swap_tx_translate(tx):
    from dateutil.parser import parse
    
    asset = tx['asset_base']
    p = asset_price_translator(asset)

    tx_date = parse(tx['date'])
    new_price = p[tx_date]['open']
    return create_swap_transactions(tx, new_price)

def simple_tax_calc(transactions, tax_rate = 0.25, initial_losses = 0, start_year = 0):
    def _fs(fifo):
        return sum((f[0] for f in fifo))
    d = {}
    losses = initial_losses
    tax = 0
    started = False
    for tx in transactions:
        asset = tx['asset_tgt']
        if asset not in d:
            d[asset] = []
        fifo = d[asset]

        if not started and tx['unix_time'] >= start_year:
            losses = 0
            started = True
        if tx['unix_time'] > start_year + YEAR_SECONDS:
            break
        if tx['type'] == 'buy':
            print("TX: BUY:", tx['amount'], tx['asset_base'], 'to buy', tx['amount']/tx['rate'],
                    tx['asset_tgt'], "@", tx['rate'], "items in FIFO:", len(fifo), "sum:", _fs(fifo), 'augmented:', tx['augmented'], tx['date'])
            fifo.append((tx['amount']/tx['rate'], tx['rate']))
        else:
            print("TX: SELL:", tx['amount'], tx['asset_base'], 'to sell', tx['amount']/tx['rate'],
                    tx['asset_tgt'], "@", tx['rate'], "items in FIFO:", len(fifo), "sum:", _fs(fifo), 'augmented:', tx['augmented'], tx['date'])
            p_l = apply_sell_transaction(tx, fifo)
            if p_l < 0:
                losses -= p_l
            elif start_year < tx['unix_time'] and tx['unix_time'] <= start_year + YEAR_SECONDS:
                t = calculate_tax(p_l, 1.0, losses)
                tax += t
    print(tax, losses)
    return (tax - losses)*tax_rate

def load_transactions(v):
    if not basename(v).startswith("bitstamp"):
        transactions = bitfinex_transactions(v)
    else:
        transactions = bitstamp_transactions(v)
    augment_tx = augment_transactions(transactions)
    return list(augment_tx)

def dedup(l):
    new_l = list(l)
    last = {'unix_time':0.0, 'type':None}
    while new_l:
        line = new_l.pop()
        if line['unix_time'] != last['unix_time'] or line['type'] != last['type']:
            last = line
            yield line

def main(argv):
    all_transactions = list(dedup(sorted(sum([load_transactions(v) for v in sys.argv[1:]], []), key=lambda x:-int(x['unix_time']))))
    print(simple_tax_calc(all_transactions, 0.25, 0, MIN_YEAR))

if __name__ == '__main__':
    import sys
    main(sys.argv)

