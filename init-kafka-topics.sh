#!/bin/bash
set -e

BOOTSTRAP_SERVER=${KAFKA_BOOTSTRAP_SERVER:-"kafka1:9092"}
PARTITIONS=${PARTITIONS:-8}
REPLICATION_FACTOR=${REPLICATION_FACTOR:-3}

echo "========================================"
echo "Инициализация Kafka топиков"
echo "========================================"
echo "Bootstrap: $BOOTSTRAP_SERVER"
echo "Partitions: $PARTITIONS"
echo "Replication: $REPLICATION_FACTOR"
echo ""

# Ждём Kafka
echo "Ожидание Kafka..."
for i in $(seq 1 30); do
    if kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list > /dev/null 2>&1; then
        echo "✓ Kafka доступна"
        break
    fi
    echo "  Попытка $i/30..."
    sleep 2
done

# Создаём топики
echo ""
echo "Создание топиков..."

for topic in messages filtered_messages blocked_users censored_words; do
    echo "  Топик: $topic"
    kafka-topics \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --create \
        --if-not-exists \
        --topic "$topic" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION_FACTOR"
done

echo ""
echo "Готово!"
echo "========================================"

# Показываем список топиков
echo ""
echo "Созданные топики:"
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list | grep -E "^(messages|filtered_messages|blocked_users|censored_words)$"

echo ""
echo "Детали топиков:"
for topic in messages filtered_messages blocked_users censored_words; do
    echo ""
    echo "--- $topic ---"
    kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --describe --topic "$topic"
done

echo ""
echo "========================================"
echo "✓ Инициализация завершена успешно"
echo "========================================"