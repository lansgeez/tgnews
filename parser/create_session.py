api_id = '34833012'
api_hash = 'cbdd7731d08f01fb8b64a063d851e117'

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
