api_id = '22077758'
api_hash = 'bdd95b684ca60e44ef79a453a712f5cc'

from telethon.sync import TelegramClient
from telethon.sessions import StringSession

# Ваши данные API

# Создание временной сессии для генерации строки
with TelegramClient(StringSession(), api_id, api_hash) as client:
    # Аутентификация (если нужно)
    client.start()

    # Генерация строковой сессии
    string_session = client.session.save()

    print("Ваша строковая сессия:")
    print(string_session)
    print("\nСохраните эту строку в безопасном месте!")
