#!/usr/bin/env python3

# Часть кода с бизнес-логикой реализована через ИИ 
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import sys
import os

# Конфигурация
DEFAULT_BOOTSTRAP_SERVERS = [
    'kafka1:9092',
    'kafka2:9092',
    'kafka3:9092',
]

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    'KAFKA_BOOTSTRAP_SERVERS',
    ','.join(DEFAULT_BOOTSTRAP_SERVERS)
).split(',')

TOPIC = 'filtered_messages'
TIMEOUT_SECONDS = 10


def create_consumer():
    """Создаёт Kafka consumer."""
    print(f"\nПодключение к Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=TIMEOUT_SECONDS * 1000,
    )


def format_message(msg):
    """Форматирует сообщение для красивого вывода."""
    value = msg.value
    
    # Статус
    status = "ЗАБЛОКИРОВАНО" if value.get('is_blocked', False) else "ПРОПУЩЕНО"
    
    # Цензура
    censored = value.get('censored_words', [])
    if censored:
        censor_info = f"Зацензурено: {', '.join(censored)}"
    else:
        censor_info = ""
    
    # Форматирование
    output = []
    output.append("=" * 70)
    output.append(f"ID: {value.get('message_id', 'N/A')}")
    output.append(f"Статус: {status}")
    output.append(f"От: {value.get('sender_id', 'N/A')} → Кому: {value.get('receiver_id', 'N/A')}")
    output.append(f"Содержимое: {value.get('content', 'N/A')}")
    
    if value.get('is_blocked', False):
        output.append(f"Причина: {value.get('blocked_reason', 'N/A')}")
    
    if censor_info:
        output.append(censor_info)
    
    output.append("")
    
    return "\n".join(output)


def main():
    """Основная функция."""
    print("\n" + "="*70)
    print(f"ЧТЕНИЕ СООБЩЕНИЙ ИЗ ТОПИКА '{TOPIC}'")
    print("="*70)
    
    try:
        consumer = create_consumer()
        print(f"Успешное подключение к Kafka кластеру")
        print(f"  Топик: {TOPIC}")
        print(f"  Таймаут: {TIMEOUT_SECONDS} сек\n")
        
    except KafkaError as e:
        print(f"\nОшибка подключения к Kafka: {e}")
        print("\nУбедитесь, что Kafka кластер запущен:")
        print("  docker-compose -f docker-compose-kafka.yml up -d")
        print("\nИли укажите правильный адрес через переменную:")
        print("  export KAFKA_BOOTSTRAP_SERVERS='localhost:9094,localhost:9095'")
        print("  python read_filtered_messages.py")
        sys.exit(1)
    
    try:
        message_count = 0
        
        print("Ожидание сообщений...\n")
        
        for msg in consumer:
            message_count += 1
            print(format_message(msg))
        
        if message_count == 0:
            print(" Сообщений не найдено в топике filtered_messages")
            print("="*70)
            print(f"ВСЕГО СООБЩЕНИЙ: {message_count}")
            print("="*70)
        
    except KafkaError as e:
        print(f"\nОшибка чтения: {e}")
    finally:
        consumer.close()
        print("\nConsumer закрыт")


if __name__ == '__main__':
    main()