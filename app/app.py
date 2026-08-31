#!/usr/bin/env python3
import faust
from faust import Record, App
from typing import List, Set, Optional
import re
import os

# Получаем адрес Kafka broker из переменных окружения
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')

# Создаём приложение Faust
app = App(
    'message-processor',
    broker=f'kafka://{KAFKA_BROKER}',
    store='rocksdb://',  # Персистентное хранилище состояния
    value_serializer='json',
    autodiscover=True,
    topic_partitions=8,
)


class Message(Record):
    message_id: str # message_id: Уникальный идентификатор сообщения
    sender_id: str # sender_id: ID отправителя
    receiver_id: str # receiver_id: ID получателя  
    content: str # content: Текст сообщения
    timestamp: float # timestamp: Время отправки (Unix timestamp)

class FilteredMessage(Record):
 #  Модель сообщения после фильтрации.
    message_id: str # message_id: Уникальный идентификатор сообщения
    sender_id: str # sender_id: ID отправителя
    receiver_id: str # receiver_id: ID получателя 
    content: str # content: Текст сообщения (после цензуры)
    timestamp: float #  timestamp: Время отправки
    is_blocked: bool = False # is_blocked: Было ли сообщение заблокировано
    blocked_reason: str = "" # blocked_reason: Причина блокировки (если есть)
    censored_words: List[str] = [] # censored_words: Список заменённых слов


class BlockedUser(Record):
# Модель заблокированного пользователя.
    user_id: str # user_id: ID пользователя, который блокирует
    blocked_user_id: str # blocked_user_id: ID блокируемого пользователя
    timestamp: float # timestamp: Время блокировки


class CensoredWord(Record):
# Модель запрещённого слова.
    word: str # word: Запрещённое слово
    replacement: str = "***" # replacement: Замена (по умолчанию **)
    is_active: bool = True # is_active: Активно ли правило


# Топик для входящих сообщений
messages_topic = app.topic(
    'messages',
    value_type=Message,
)

# Топик для сообщений после фильтрации
filtered_messages_topic = app.topic(
    'filtered_messages',
    value_type=FilteredMessage,
)

# Топик для управления блокировками пользователей
blocked_users_topic = app.topic(
    'blocked_users',
    value_type=BlockedUser,
)

# Топик для управления запрещёнными словами
censored_words_topic = app.topic(
    'censored_words',
    value_type=CensoredWord,
)


# Таблица заблокированных пользователей для каждого пользователя
# Ключ: user_id (кто блокирует), Значение: set blocked_user_ids
blocked_users_table = app.Table(
    'blocked_users_store',
    default=set,
)

# Таблица запрещённых слов
# Ключ: word, Значение: CensoredWord
censored_words_table = app.Table(
    'censored_words_store',
    default=lambda: CensoredWord(word="", replacement="***", is_active=True),
)


@app.agent(blocked_users_topic)
async def process_blocked_users(blocked_stream):
    async for blocked_user in blocked_stream:
        user_id = blocked_user.user_id
        blocked_id = blocked_user.blocked_user_id
        
        # Получаем текущий список заблокированных для пользователя
        user_blocked_set = blocked_users_table[user_id]
        
        # Добавляем блокируемого пользователя в set
        user_blocked_set.add(blocked_id)
        
        # Сохраняем обновлённый set
        blocked_users_table[user_id] = user_blocked_set
        
        print(f"[BLOCK] Пользователь {user_id} заблокировал {blocked_id}")
        print(f"[BLOCK] Текущий список заблокированных {user_id}: {user_blocked_set}")


@app.agent(censored_words_topic)
async def process_censored_words(censored_stream):
    async for censored_word in censored_stream:
        word = censored_word.word
        
        if censored_word.is_active:
            # Добавляем или обновляем запрещённое слово
            censored_words_table[word] = censored_word
            print(f"[CENSOR] Добавлено запрещённое слово: '{word}' -> '{censored_word.replacement}'")
        else:
            # Удаляем запрещённое слово (если существует)
            if word in censored_words_table:
                del censored_words_table[word]
                print(f"[CENSOR] Удалено запрещённое слово: '{word}'")


@app.agent(messages_topic)
async def filter_messages(message_stream):

    async for message in message_stream:
        message_id = message.message_id
        sender_id = message.sender_id
        receiver_id = message.receiver_id
        content = message.content
        timestamp = message.timestamp
        
        # Инициализируем результат фильтрации
        is_blocked = False
        blocked_reason = ""
        censored_words_list = []
        filtered_content = content
        
        # Получаем список заблокированных пользователей для получателя
        receiver_blocked_set = blocked_users_table.get(receiver_id, set())
        
        # Проверяем, заблокирован ли отправитель
        if sender_id in receiver_blocked_set:
            is_blocked = True
            blocked_reason = f"Пользователь {sender_id} заблокирован получателем {receiver_id}"
            print(f"[FILTER] Сообщение {message_id} заблокировано: {blocked_reason}")
        else:
            
            # Получаем все активные запрещённые слова
            active_censored_words = []
            for word, censored_word in censored_words_table.items():
                if censored_word.is_active:
                    active_censored_words.append(censored_word)
            
            # Применяем цензуру к каждому запрещённому слову
            for censored_word in active_censored_words:
                word = censored_word.word
                replacement = censored_word.replacement
                
                # Ищем слово в тексте (регистронезависимо)
                pattern = re.compile(re.escape(word), re.IGNORECASE)
                matches = pattern.findall(content)
                
                if matches:
                    # Заменяем все вхождения слова
                    filtered_content = pattern.sub(replacement, filtered_content)
                    censored_words_list.extend(matches)
                    print(f"[CENSOR] В сообщении {message_id} заменено слово '{word}' ({len(matches)} раз)")
        
        
        filtered_message = FilteredMessage(
            message_id=message_id,
            sender_id=sender_id,
            receiver_id=receiver_id,
            content=filtered_content,
            timestamp=timestamp,
            is_blocked=is_blocked,
            blocked_reason=blocked_reason,
            censored_words=list(set(censored_words_list)),  # Убираем дубликаты
        )
        
        # Отправляем в топик отфильтрованных сообщений
        await filtered_messages_topic.send(value=filtered_message)
        
        status = "BLOCKED" if is_blocked else "ALLOWED"
        print(f"[FILTER] Сообщение {message_id} [{status}]: {content[:50]}... -> {filtered_content[:50]}...")


def get_blocked_users_for_user(user_id: str) -> Set[str]:
    return blocked_users_table.get(user_id, set())


def add_blocked_user(user_id: str, blocked_user_id: str, timestamp: float):
    blocked_user = BlockedUser(
        user_id=user_id,
        blocked_user_id=blocked_user_id,
        timestamp=timestamp,
    )
    blocked_users_topic.send_sync(value=blocked_user)


def add_censored_word(word: str, replacement: str = "***", is_active: bool = True):
    censored_word = CensoredWord(
        word=word,
        replacement=replacement,
        is_active=is_active,
    )
    censored_words_topic.send_sync(value=censored_word)

if __name__ == '__main__':
    app.main()