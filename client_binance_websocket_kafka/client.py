import websocket
import json
from confluent_kafka import Producer
import time

def read_config():
    config = {}
    with open("client.properties") as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                k, v = line.strip().split("=", 1)
                config[k.strip()] = v.strip()
    return config

def main():
    config = read_config()
    producer = Producer(config)
    topic = "binance-trades"

    start_time = time.time()
    max_duration = 10 * 60  # 10 minutes en secondes

    def on_message(ws, message):
        # Arrêter après 10 minutes
        if time.time() - start_time > max_duration:
            print("Temps maximal atteint, fermeture de la connexion.")
            ws.close()
            return

        print("Message reçu de Binance WebSocket.")
        producer.produce(topic, value=message)
        producer.poll(0)

    def on_error(ws, error):
        print("Erreur WebSocket:", error)

    def on_close(ws, close_status_code, close_msg):
     print(f"Connexion WebSocket fermée : {close_status_code}, {close_msg}")

    def on_open(ws):
        print("Connexion WebSocket ouverte.")

    ws_url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    ws = websocket.WebSocketApp(ws_url,
                                 on_message=on_message,
                                 on_error=on_error,
                                 on_close=on_close,
                                 on_open=on_open)
    ws.run_forever()

if __name__ == "__main__":
    main()