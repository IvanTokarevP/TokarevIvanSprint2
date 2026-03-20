import json
import logging
import os
import threading
import time
from datetime import datetime

from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Конфигурация Kafka
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka:9092').split(',')
MOVIE_TOPIC = 'movie-events'
USER_TOPIC = 'user-events'
PAYMENT_TOPIC = 'payment-events'

# Глобальные переменные для producer и consumer
producer = None
consumers_running = True

def create_producer():
    """Создание Kafka producer с повторными попытками."""
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                acks='all'
            )
            logger.info("Kafka producer connected")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready, retrying in 5s... ({retries} retries left)")
            retries -= 1
            time.sleep(5)
    raise Exception("Could not connect to Kafka after multiple attempts")

def create_consumer(topic, group_id):
    """Создание Kafka consumer."""
    retries = 5
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            logger.info(f"Kafka consumer connected to topic {topic}")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready, retrying for consumer {topic}...")
            retries -= 1
            time.sleep(5)
    raise Exception(f"Could not connect to Kafka for consumer {topic}")

def consume_messages(consumer, topic_name):
    """Функция для бесконечного чтения сообщений из топика."""
    logger.info(f"Starting consumer for topic {topic_name}")
    for msg in consumer:
        if not consumers_running:
            break
        logger.info(f"Consumed from {topic_name}: partition={msg.partition}, offset={msg.offset}, value={msg.value}")

def start_consumers():
    """Запускаем потребителей для всех топиков в отдельных потоках."""
    global consumers_running
    consumers = [
        (MOVIE_TOPIC, 'movie-consumer-group'),
        (USER_TOPIC, 'user-consumer-group'),
        (PAYMENT_TOPIC, 'payment-consumer-group')
    ]
    for topic, group in consumers:
        try:
            consumer = create_consumer(topic, group)
            t = threading.Thread(target=consume_messages, args=(consumer, topic), daemon=True)
            t.start()
        except Exception as e:
            logger.error(f"Failed to start consumer for {topic}: {e}")

# Эндпоинты API

@app.route('/api/events/health', methods=['GET'])
def health():
    return jsonify({"status": True}), 200

@app.route('/api/events/movie', methods=['POST'])
def create_movie_event():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Missing JSON body"}), 400

    # Простейшая валидация
    required = ['movie_id', 'title', 'action']
    for field in required:
        if field not in data:
            return jsonify({"error": f"Missing field: {field}"}), 400

    event = {
        "id": f"movie-{data['movie_id']}-{int(time.time())}",
        "type": "movie",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "payload": data
    }

    try:
        future = producer.send(MOVIE_TOPIC, value=event)
        record_metadata = future.get(timeout=10)
        return jsonify({
            "status": "success",
            "partition": record_metadata.partition,
            "offset": record_metadata.offset,
            "event": event
        }), 201
    except Exception as e:
        logger.exception("Failed to send movie event")
        return jsonify({"error": str(e)}), 500

@app.route('/api/events/user', methods=['POST'])
def create_user_event():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Missing JSON body"}), 400

    required = ['user_id', 'action', 'timestamp']
    for field in required:
        if field not in data:
            return jsonify({"error": f"Missing field: {field}"}), 400

    event = {
        "id": f"user-{data['user_id']}-{int(time.time())}",
        "type": "user",
        "timestamp": data['timestamp'],
        "payload": data
    }

    try:
        future = producer.send(USER_TOPIC, value=event)
        record_metadata = future.get(timeout=10)
        return jsonify({
            "status": "success",
            "partition": record_metadata.partition,
            "offset": record_metadata.offset,
            "event": event
        }), 201
    except Exception as e:
        logger.exception("Failed to send user event")
        return jsonify({"error": str(e)}), 500

@app.route('/api/events/payment', methods=['POST'])
def create_payment_event():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Missing JSON body"}), 400

    required = ['payment_id', 'user_id', 'amount', 'status', 'timestamp']
    for field in required:
        if field not in data:
            return jsonify({"error": f"Missing field: {field}"}), 400

    event = {
        "id": f"payment-{data['payment_id']}-{int(time.time())}",
        "type": "payment",
        "timestamp": data['timestamp'],
        "payload": data
    }

    try:
        future = producer.send(PAYMENT_TOPIC, value=event)
        record_metadata = future.get(timeout=10)
        return jsonify({
            "status": "success",
            "partition": record_metadata.partition,
            "offset": record_metadata.offset,
            "event": event
        }), 201
    except Exception as e:
        logger.exception("Failed to send payment event")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Инициализируем producer
    producer = create_producer()
    # Запускаем потребителей в фоновых потоках
    start_consumers()
    port = int(os.getenv('PORT', 8082))
    app.run(host='0.0.0.0', port=port)