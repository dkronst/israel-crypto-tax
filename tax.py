#!/usr/bin/env python3

import csv

NOT_A_BFX_EXCHANGE = ['transfer', 'deposit', 'withdrawal']

def bitfinex_tx_translate(line):
    if not line[3].startswith("Exchange"):
        return None

    exchange_details = line[3].split(" ")
    
    d = {}
    d['asset_base'] = exchange_details[4]
    d['asset_tgt'] = exchange_details[2]
    amount = float(line[1])
    d['amount'] = abs(amount)
    d['type'] = "buy" if amount < 0 else "sell"
    rate = line[3].split("@")[-1].strip().split(" ")[0]
    d['rate'] = float(rate)
    d['date'] = line[-1]
    d['unix_time'] = line[-2]
    d['augmented'] = False  # Augmented means that the tx is an added swap tx (e.g. buy dash with btc)
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
    
    while fifo and a > fifo[0][0]:
        f_a = fifo[0][0]
        f_r = fifo[0][1]

        profit += a*r - f_a*f_r
        #print("profit:", profit, fifo)
        a -= f_a
        fifo.pop(0)

    if fifo:
        fifo[0] = (fifo[0][0] - max(a, fifo[0][0]), fifo[0][1])
        profit += a*(r-fifo[0][1])
        if fifo[0][0] <= 0:
            profit += abs(fifo[0][0]*r)
            fifo.pop(0)
    else:
        profit += a*r
        

    print("profit-final:", profit, tx['date'])

    return profit

def calculate_tax(pl, tax_rate, losses):
    tax = pl*tax_rate - min(pl, losses)*tax_rate
    print("TAX:", tax, "with loss:",  losses)
    return tax, max(0, losses-pl)

def augment_transactions(orig_transactions):
    """
    augment the transactions with sell/buy transactions caused due to asset swap.
    I.e. A buy of an asset with another asset is a sell transaction of original asset and then buy with the
    secondary asset and vise versa.
    """
    for tx in orig_transactions:
        if tx['asset_base'] != 'USD':
            augmented_sell, augmented_buy = asset_price_translator(tx)
            yield augmented_sell
            yield augmented_buy
        else:
            yield tx

def asset_price_translator(tx):
    from dateutil.parser import parse
    asset = tx['asset_base']

    # Translate asset price to a price in USD at a certain date.
    if not hasattr(asset_price_translator, "asset_prices"):
        import pickle
        p = pickle.load(open("{}-history.pickle".format(asset.lower), "rb"))
        asset_price_translator.asset_prices = {}
        asset_price_translator.asset_prices[asset] = p
    p = 

    tx_date = parse(tx['date'])
    new_price = p[tx_date]['open']
    new_tx = tx.copy()

def base_asset_translator(tx):
    return TRANSLATE[tx['asset_base']](tx)

def simple_tax_calc(transactions, tax_rate = 0.25, initial_losses = 0):
    d = {}
    losses = initial_losses
    tax = 0
    for tx in transactions:
        asset = tx['asset_tgt']
        if asset not in d:
            d[asset] = []
        fifo = d[asset]
        
        if tx['type'] == 'buy':
            fifo.append((tx['amount']/tx['rate'], tx['rate']))
        else:
            p_l = apply_sell_transaction(tx, fifo)
            if p_l < 0:
                losses -= p_l
                print("loss:", losses)
            else:
                t, losses = calculate_tax(p_l, tax_rate, losses)
                tax += t
    return tax, losses

def load_transactions(v):
    transactions = bitfinex_transactions(v)
    augment_tx = augment_transactions(transactions)
    return list(augment_tx)

def dedup(l):
    new_l = list(l)
    last = {'unix_time':0.0}
    while new_l:
        line = new_l.pop()
        if line['unix_time'] != last['unix_time']:
            last = line
            yield line

def main(argv):
    all_transactions = dedup(sorted(sum([load_transactions(v) for v in sys.argv[1:]], []), key=lambda x:x['unix_time']))
    print(simple_tax_calc(all_transactions))

TRANSLATE = {
        "DSH":asset_price_translator,
        "DASH":asset_price_translator,
        "BTC":asset_price_translator,
        "USD":lambda x: x
}

if __name__ == '__main__':
    import sys
    main(sys.argv)

