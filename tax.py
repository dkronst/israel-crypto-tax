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
        print("profit:", profit, fifo)
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

def simple_tax_calc(transactions, initial_asset_balance = 0, initial_asset_value = 0, initial_losses = 0, 
        tax_rate = 0.25):
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
            #print("buy:", tx['amount']/tx['rate'], tx['rate']) 
        else:
            #print("sell:", tx['amount']/tx['rate'], tx['rate']) 
            p_l = apply_sell_transaction(tx, fifo)
            if p_l < 0:
                losses -= p_l
                print("loss:", losses)
            else:
                t, losses = calculate_tax(p_l, tax_rate, losses)
                tax += t
    return tax, losses

def main(argv):
    transactions = list(bitfinex_transactions(sys.argv[1]))
    #print(simple_tax_calc(transactions, "BTC"))
    print(simple_tax_calc(transactions))

if __name__ == '__main__':
    import sys
    main(sys.argv)
