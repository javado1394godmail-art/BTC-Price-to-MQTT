#!/usr/bin/env python3
"""Publish four timeframes of real Kraken OHLC via MQTT retained messages.

Keeps existing CoinGecko price publisher untouched. Python 3 stdlib only.
GitHub ubuntu-latest needs mosquitto-clients installed by workflow.
"""
import json
import os
import subprocess
import sys
import time
from urllib.request import urlopen, Request

INTERVALS = {'1H':60,'4H':240,'1D':1440,'1W':10080}
HOST = os.getenv('MQTT_HOST','broker.emqx.io')
BASE_TOPIC = os.getenv('MQTT_CANDLE_TOPIC','btc/javado13_0922709/candles')
MAX_CANDLES = 24


def convert_kraken(raw, timeframe):
    """Map Kraken rows to pre-existing small OHLC array format."""
    if raw.get('error'):
        raise ValueError('Kraken API errors: '+str(raw['error']))
    result = raw.get('result') or {}
    pair = next((k for k,v in result.items() if k!='last' and isinstance(v,list)),None)
    if pair is None:
        raise ValueError('Missing Kraken OHLC pair')
    source_rows = result[pair]
    if len(source_rows)<2:
        raise ValueError('Not enough OHLC history')
    rows=[]
    for r in source_rows[-MAX_CANDLES:]:
        if len(r)<5:
            raise ValueError('Malformed Kraken row')
        candle=(int(r[0])*1000,float(r[1]),float(r[2]),float(r[3]),float(r[4]))
        if not (1<candle[1]<1e8 and candle[2]>=max(candle[1],candle[4]) and
                candle[3]<=min(candle[1],candle[4]) and candle[2]>=candle[3]):
            raise ValueError('Invalid OHLC price')
        if rows and candle[0]<=rows[-1][0]:
            raise ValueError('Timestamps not increasing')
        rows.append(candle)
    # Kraken REST always returns a not-yet-committed candle as its last row.
    return {'tf':timeframe,'src':'KRAKEN','open':True,
            't':[r[0] for r in rows],
            'o':[round(r[1],2) for r in rows],
            'h':[round(r[2],2) for r in rows],
            'l':[round(r[3],2) for r in rows],
            'c':[round(r[4],2) for r in rows]}


def fetch(interval):
    url='https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval='+str(interval)
    req=Request(url,headers={'User-Agent':'ESP32-BTC-Monitor/3.0',
                             'Accept':'application/json'})
    with urlopen(req,timeout=15) as r:
        return json.load(r)


def publish(tf, doc):
    topic=BASE_TOPIC + '/' + tf.lower()
    payload=json.dumps(doc,separators=(',',':'),ensure_ascii=True)
    if len(payload.encode())>=6000:
        raise ValueError('Payload too large: '+str(len(payload)))
    command=['mosquitto_pub','-h',HOST,'-p','1883',
             '-t',topic,'-m',payload,'-r','-q','1']
    subprocess.run(command,check=True,timeout=15)
    print('PUBLISHED',topic,'bytes=',len(payload),'candles=',len(doc['c']))


def main():
    success=0
    for tf,interval in INTERVALS.items():
        try:
            doc=convert_kraken(fetch(interval),tf)
            publish(tf,doc)
            success+=1
        except Exception as e:
            print('FAILED',tf,repr(e),file=sys.stderr)
        time.sleep(1.3)  # limit requests to less than 1/sec per Kraken guidance
    if success==0:
        raise SystemExit('No new timeframes published')
    if success<4:
        print('WARNING: some timeframes retain their last good data')


if __name__=='__main__':
    main()

