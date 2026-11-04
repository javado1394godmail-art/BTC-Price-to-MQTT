#!/usr/bin/env python3
"""Publish validated BTC/USD spot quotes to the exact MQTT topics used by BTC Monitor v6.

GitHub Actions uses its own HTTPS stack, not TLS on ESP32. No external Python packages.
If CoinGecko fails or sends invalid data, use Kraken's public last-trade price.
Never publish a null/invalid price. Both messages are retained, QoS 1.
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

BASE_TOPIC = os.environ.get("MQTT_TOPIC_BASE", "btc/javado13_0922709").rstrip("/")
HOST = os.environ.get("MQTT_HOST", "broker.emqx.io")
PORT = int(os.environ.get("MQTT_PORT", "1883"))
TIMEOUT = 12


def request_json(url):
    request = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "User-Agent": "BTC-Monitor-Publisher/1.0 (GitHub Actions)",
    })
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        raw = response.read(8193)
        if len(raw) > 8192:
            raise ValueError("API response too large")
        return json.loads(raw.decode("utf-8"))


def valid_price(value):
    try:
        n = float(value)
        return 100 < n < 100_000_000 and n == n
    except (TypeError, ValueError, OverflowError):
        return False


def valid_change(value):
    try:
        n = float(value)
        return n == n and -100 <= n <= 10000
    except (TypeError, ValueError, OverflowError):
        return False


def parse_coingecko(data):
    item = data["bitcoin"]
    price = item["usd"]
    if not valid_price(price):
        raise ValueError("CoinGecko returned no valid BTC/USD price")
    change = item.get("usd_24h_change")
    return float(price), float(change) if valid_change(change) else None, "coingecko"


def parse_kraken(data):
    if data.get("error"):
        raise ValueError("Kraken API error: " + repr(data["error"]))
    records = data.get("result", {})
    if not records:
        raise ValueError("Kraken returned no ticker")
    ticker = next(iter(records.values()))
    price = ticker["c"][0]  # last trade price; NOT 24h percent change
    if not valid_price(price):
        raise ValueError("Kraken returned invalid BTC/USD price")
    return float(price), None, "kraken"


def fetch_price():
    sources = [
        ("CoinGecko", "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true", parse_coingecko),
        ("Kraken", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD", parse_kraken),
    ]
    errors = []
    for name, url, parser in sources:
        try:
            return parser(request_json(url))
        except Exception as e:
            errors.append(name + ": " + str(e))
            print("WARN", errors[-1], flush=True)
    raise RuntimeError("All quote sources failed. Refusing to publish null: " + " | ".join(errors))


def publish(topic, message):
    cmd = ["mosquitto_pub", "-h", HOST, "-p", str(PORT),
           "-q", "1", "-r", "-t", topic, "-m", message]
    subprocess.run(cmd, timeout=20, check=True)


def main():
    price, change24, source = fetch_price()
    timestamp = int(time.time())
    live = json.dumps({"p": price, "ch": change24, "ts": timestamp}, separators=(",", ":"))
    price_text = "{:.2f}".format(price)
    # Both topics are subscribed by btc_monitor_v6.py. Retained messages
    # are sent immediately when the ESP32 connects or reconnects.
    publish(BASE_TOPIC + "/live", live)
    publish(BASE_TOPIC + "/price", price_text)
    print("OK: published BTC/USD {} from {} to {}/live and {}/price (retained, QoS1)".format(
        price_text, source, BASE_TOPIC, BASE_TOPIC), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("ERROR:", error, file=sys.stderr)
        sys.exit(1)
