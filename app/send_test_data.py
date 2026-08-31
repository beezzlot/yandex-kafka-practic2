#!/usr/bin/env python3
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
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

# Топики
TOPIC_MESSAGES = 'messages'
TOPIC_BLOCKED_USERS = 'blocked_users'
TOPIC_CENSORED_WORDS = 'censored_words'


def create_producer():
    """Создаёт Kafka producer."""
    print(f"\nПодключение к Kafka: {KAFKA_BOOTSTRAP_SERVERS}")

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        # Увеличиваем таймауты
        request_timeout_ms=30000,
        delivery_timeout_ms=60000,
        max_in_flight_requests_per_connection=1,
        # Пробуем получить метаданные
        metadata_max_age_ms=10000,
    )


def send_blocked_users(producer, blocked_users):
    """Отправляет данные о блокировках пользователей."""
    print("\n" + "="*60)
    print("ОТПРАВКА ДАННЫХ О БЛОКИРОВКАХ ПОЛЬЗОВАТЕЛЕЙ")
    print("="*60)

    for blocked_user in blocked_users:
        user_id = blocked_user['user_id']
        blocked_id = blocked_user['blocked_user_id']

        try:
            future = producer.send(TOPIC_BLOCKED_USERS, value=blocked_user)
            record_metadata = future.get(timeout=30)

            print(f"   Отправлено: {user_id} заблокировал {blocked_id}")
            print(f"    Топик: {record_metadata.topic}, Партиция: {record_metadata.partition}, "
                  f"Офсет: {record_metadata.offset}")
        except Exception as e:
            print(f"  Ошибка: {e}")

    producer.flush()
    print(f"\n  Всего отправлено блокировок: {len(blocked_users)}")


def send_censored_words(producer, censored_words):
    """Отправляет данные о запрещённых словах."""
    print("\n" + "="*60)
    print("ОТПРАВКА ДАННЫХ О ЗАПРЕЩЁННЫХ СЛОВАХ")
    print("="*60)

    for censored_word in censored_words:
        word = censored_word['word']

        try:
            future = producer.send(TOPIC_CENSORED_WORDS, value=censored_word)
            record_metadata = future.get(timeout=30)

            status = "активно" if censored_word['is_active'] else "неактивно"
            print(f"  Отправлено: '{word}' -> '{censored_word['replacement']}' ({status})")
            print(f"    Топик: {record_metadata.topic}, Партиция: {record_metadata.partition}, "
                  f"Офсет: {record_metadata.offset}")
        except Exception as e:
            print(f" Ошибка: {e}")

    producer.flush()
    print(f"\n  Всего отправлено запрещённых слов: {len(censored_words)}")


def send_messages(producer, messages):
    """Отправляет тестовые сообщения."""
    print("\n" + "="*60)
    print("ОТПРАВКА ТЕСТОВЫХ СООБЩЕНИЙ")
    print("="*60)

    # Сначала проверяем доступность топика
    print("\n Проверка метаданных топика...")
    try:
        cluster_metadata = producer.list_topics(timeout=30)
        print(f"  Доступные топики: {list(cluster_metadata.topics.keys())}")

        if TOPIC_MESSAGES not in cluster_metadata.topics:
            print(f"    Топик '{TOPIC_MESSAGES}' не найден!")
            return
        else:
            print(f"  Топик '{TOPIC_MESSAGES}' существует")
    except Exception as e:
        print(f"    Не удалось получить метаданные: {e}")

    for i, msg in enumerate(messages, 1):
        message_id = msg['message_id']
        sender_id = msg['sender_id']
        content_preview = msg['content'][:40] + "..." if len(msg['content']) > 40 else msg['content']

        try:
            print(f"\n[{i}/{len(messages)}] Отправка: {message_id} от {sender_id}")

            future = producer.send(TOPIC_MESSAGES, value=msg)
            record_metadata = future.get(timeout=30)

            print(f"    Отправлено: {message_id}")
            print(f"    Содержимое: {content_preview}")
            print(f"    Топик: {record_metadata.topic}, Партиция: {record_metadata.partition}, "
                  f"Офсет: {record_metadata.offset}")
            print(f"    Описание: {msg['description']}")

        except Exception as e:
            print(f"  Ошибка отправки {message_id}: {e}")
            print(f"  Попробуем следующий...")
            continue

        # Небольшая задержка
        time.sleep(0.5)

    producer.flush()
    print(f"\n  Всего отправлено сообщений: {len(messages)}")


def main():
    """Основная функция."""
    print("\n" + "="*60)
    print("ТЕСТИРОВАНИЕ СИСТЕМЫ ОБРАБОТКИ СООБЩЕНИЙ")
    print("="*60)

    # Загружаем тестовые данные
    with open('test_data.json', 'r', encoding='utf-8') as f:
        test_data = json.load(f)

    blocked_users = test_data['blocked_users']
    censored_words = test_data['censored_words']
    test_messages = test_data['test_messages']

    # Создаём producer
    try:
        producer = create_producer()
        print(f" Успешное подключение к Kafka кластеру")
    except KafkaError as e:
        print(f"\n Ошибка подключения к Kafka: {e}")
        print("\n Убедитесь, что Kafka кластер запущен:")
        print("  docker-compose -f docker-compose-kafka.yml up -d")
        return

    try:
        # 1. Отправляем данные о блокировках
        send_blocked_users(producer, blocked_users)

        # 2. Отправляем запрещённые слова
        send_censored_words(producer, censored_words)

        # Задержка для обработки
        print("\n Ожидание обработки блокировок и запрещённых слов (3 сек)...")
        time.sleep(3)

        # 3. Отправляем тестовые сообщения
        send_messages(producer, test_messages)

        print("\n" + "="*60)
        print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
        print("="*60)
        print("\nПроверьте топик 'filtered_messages':")
        print("  python3 read_filtered_messages.py")

    except KafkaError as e:
        print(f"\nОшибка: {e}")
    finally:
        producer.close()
        print("\nProducer закрыт")


if __name__ == '__main__':
    main()