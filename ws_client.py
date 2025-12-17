import json
import threading
import websocket

from datastore import write_tick, init_storage

WS_URL = "wss://fstream.binance.com/ws"


class BinanceWSClient:
    def __init__(self, symbols):
        self.symbols = [s.lower() for s in symbols]
        self._threads = []
        init_storage()

    def _connect(self, symbol):
        stream = f"{symbol}@trade"
        url = f"{WS_URL}/{stream}"

        def on_message(ws, msg):
            data = json.loads(msg)
            if data.get("e") == "trade":
                price = float(data["p"])
                write_tick(data["s"].lower(), price)

        def on_open(ws):
            print(f"[WS OPEN] {symbol}")

        def on_error(ws, error):
            print(f"[WS ERROR] {symbol} → {error}")

        def on_close(ws):
            print(f"[WS CLOSED] {symbol}")

        ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever()

    def start(self):
        for sym in self.symbols:
            t = threading.Thread(
                target=self._connect,
                args=(sym,),
                daemon=True
            )
            t.start()
            self._threads.append(t)
