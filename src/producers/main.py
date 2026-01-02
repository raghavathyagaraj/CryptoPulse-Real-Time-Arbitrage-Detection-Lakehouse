import json
import time
import os
import websocket
from confluent_kafka import Producer
from dotenv import load_dotenv

# 1. Load environment variables
load_dotenv()

# 2. Configure Kafka Producer
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'client.id': 'python-producer-crypto'
}

# Create the Producer instance
try:
    producer = Producer(conf)
except Exception as e:
    print(f"Failed to create producer: {e}")
    exit(1)

def delivery_report(err, msg):
    """Callback: Called once for each message to indicate delivery result."""
    if err is not None:
        print(f'Delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def on_message(ws, message):
    """Callback: Triggered when a message is received from Coinbase."""
    data = json.loads(message)
    
    # We only care about "ticker" updates (price changes)
    if data.get('type') == 'ticker':
        # Add a timestamp for when we received it
        data['ingest_ts'] = time.time()
        
        # Print price to console so we know it's working
        print(f"{data.get('product_id')} Price: {data.get('price')}")
        
        # Send to Confluent Kafka
        try:
            producer.produce(
                topic='crypto_market_data',
                key='coinbase',           # Key allows for partitioning
                value=json.dumps(data),   # Payload must be a string
                callback=delivery_report  # Acknowledge receipt
            )
            producer.poll(0) # Trigger the callback
        except BufferError:
            print("Local buffer full, waiting...")
            producer.poll(1)

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### WebSocket Closed ###")

def on_open(ws):
    """Callback: Triggered when connection opens. We must subscribe immediately."""
    print("### Connection Opened ###")
    
    # THIS WAS THE FIX: 'product_ids' must be a list of strings
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channels": ["ticker"]
    }
    ws.send(json.dumps(subscribe_message))

if __name__ == "__main__":
    # Coinbase WebSocket URL
    socket = "wss://ws-feed.exchange.coinbase.com"
    
    ws = websocket.WebSocketApp(socket,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)

    # Run forever
    print("Starting Crypto Stream...")
    ws.run_forever()