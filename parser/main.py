# parser/main.py
import os
import json
import time
import logging
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaPhoto,
    MessageMediaDocument,
    MessageMediaWebPage,
    MessageMediaGeo,
    MessageMediaContact,
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    DocumentAttributeSticker,
    DocumentAttributeAnimated,
    DocumentAttributeFilename,
)

from prometheus_client import Counter, Histogram, Gauge, start_http_server

import config

# ---------------------------------------------------------------------------
# ENV / CONFIG
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "news_raw")  # <-- ВАЖНО

SERVICE_NAME = os.getenv("SERVICE_NAME", "parser")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9101"))

DOWNLOADS_DIR = os.getenv("DOWNLOADS_DIR", "downloads")  # <-- согласовано с sender

API_ID = config.API_ID
API_HASH = config.API_HASH
SESSION_STRING = config.SESSION_STRING

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
current_time = datetime.now().strftime("%Y%m%d_%H:%M:%S")
logging.basicConfig(
    level=logging.INFO,
    filename=f"tgpars_{current_time}.log",
    filemode="a",
    format="%(asctime)s %(levelname)s %(message)s",
)

# ---------------------------------------------------------------------------
# PROMETHEUS METRICS
# ---------------------------------------------------------------------------
P_EVENTS = Counter("tgnews_parser_events_total", "Total Telegram events parsed", ["has_media", "kind"])
P_KAFKA_SEND = Counter("tgnews_parser_kafka_send_total", "Kafka send attempts", ["status"])
P_DL_SECONDS = Histogram("tgnews_parser_download_seconds", "Media download duration seconds", ["kind"])
P_DL_FAIL = Counter("tgnews_parser_download_fail_total", "Media download failures", ["reason", "kind"])
P_LAST_TS = Gauge("tgnews_parser_last_event_timestamp", "Unix timestamp of last processed event")

# ---------------------------------------------------------------------------
# TELETHON CLIENT
# ---------------------------------------------------------------------------
client = TelegramClient(
    StringSession(SESSION_STRING),
    API_ID,
    API_HASH,
    device_model="Linux",
    system_version="Docker",
)

# ---------------------------------------------------------------------------
# KAFKA PRODUCER (RETRY)
# ---------------------------------------------------------------------------
def create_kafka_producer(bootstrap: str) -> KafkaProducer:
    while True:
        try:
            print(f"🔄 [{SERVICE_NAME}] Connecting to Kafka: {bootstrap} ...")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"✅ [{SERVICE_NAME}] Kafka connected.")
            return producer
        except NoBrokersAvailable:
            print(f"❌ [{SERVICE_NAME}] Kafka not ready. Retry in 3s...")
            time.sleep(3)

producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS)

def send_to_kafka(message_data: dict):
    try:
        producer.send(KAFKA_TOPIC, value=message_data)
        producer.flush()
        P_KAFKA_SEND.labels(status="ok").inc()
        logging.info(f"Sent to Kafka: {message_data}")
    except Exception as e:
        P_KAFKA_SEND.labels(status="error").inc()
        logging.error(f"Kafka send error: {e}")
        print(f"❌ Kafka send error: {e}")

# ---------------------------------------------------------------------------
# MEDIA EXTENSION DETECTION
# ---------------------------------------------------------------------------
def detect_media_extension(media) -> str:
    if isinstance(media, MessageMediaPhoto):
        return ".jpg"

    if isinstance(media, MessageMediaDocument):
        doc = media.document
        ext = None

        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeFilename):
                file_name = getattr(attr, "file_name", "")
                if "." in file_name:
                    ext = "." + file_name.rsplit(".", 1)[-1].lower()

            if isinstance(attr, DocumentAttributeVideo):
                ext = ".mp4"
            elif isinstance(attr, DocumentAttributeAudio):
                ext = ".mp3"
            elif isinstance(attr, DocumentAttributeSticker):
                ext = ".webp"
            elif isinstance(attr, DocumentAttributeAnimated):
                ext = ".gif"

        return ext or ".bin"

    if isinstance(media, MessageMediaWebPage):
        return ".html"

    if isinstance(media, (MessageMediaGeo, MessageMediaContact)):
        return ".bin"

    return ".bin"

def kind_by_postfix(postfix: str) -> str:
    if postfix in [".jpg", ".jpeg", ".png", ".webp"]:
        return "photo"
    if postfix in [".mp4", ".mov", ".gif"]:
        return "video"
    if postfix:
        return "other"
    return "text"

def entities_to_dicts(entities):
    out = []
    for e in entities or []:
        d = {"offset": e.offset, "length": e.length, "type": type(e).__name__}
        if hasattr(e, "url") and e.url:
            d["url"] = e.url
        out.append(d)
    return out

# ---------------------------------------------------------------------------
# TELETHON HANDLER
# ---------------------------------------------------------------------------
@client.on(events.NewMessage(chats=config.channels))
async def normal_handler(event: events.NewMessage.Event):
    try:
        message = event.message
        raw_text = message.message or ""
        entities = message.entities or []

        channel_id = message.peer_id.channel_id
        grouped_id = message.grouped_id
        msg_id = message.id

        # подпись источника (@username)
        try:
            entity = await client.get_entity(message.peer_id)
            src_username = getattr(entity, "username", None)
            if src_username:
                text = f"{raw_text}\n**\n@{src_username}"
            else:
                text = f"{raw_text}\n**\n@channel_{channel_id}"
        except Exception:
            text = raw_text

        has_media = bool(message.media)
        postfix = None

        # media save
        if has_media:
            postfix = detect_media_extension(message.media)
            kind = kind_by_postfix(postfix)

            folder_suffix = grouped_id if grouped_id is not None else "single"
            folder = os.path.join(DOWNLOADS_DIR, f"{channel_id}_{folder_suffix}")
            os.makedirs(folder, exist_ok=True)

            file_path = os.path.join(folder, f"{msg_id}{postfix}")

            try:
                with P_DL_SECONDS.labels(kind=kind).time():
                    await client.download_media(message.media, file=file_path)

                print(f"💾 [{SERVICE_NAME}] saved: {file_path}")
                logging.info(f"Saved media: {file_path}")
            except Exception as e:
                P_DL_FAIL.labels(reason="exception", kind=kind).inc()
                logging.error(f"Download error: {e}")
                print(f"❌ Download error: {e}")
        else:
            kind = "text"

        P_EVENTS.labels(has_media=str(has_media).lower(), kind=kind).inc()
        P_LAST_TS.set(time.time())

        message_data = {
            "text": text,
            "entities": entities_to_dicts(entities),
            "channel_id": channel_id,
            "grouped_id": grouped_id,
            "msg_id": msg_id,
            "has_media": has_media,
            "postfix": postfix,
        }

        logging.info(f"Event: {message_data}")
        send_to_kafka(message_data)

    except Exception as e:
        logging.error(f"Handler error: {e}")
        print(f"❌ Handler error: {e}")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    start_http_server(METRICS_PORT)
    print(f"📈 [{SERVICE_NAME}] metrics on :{METRICS_PORT}/metrics")

    logging.info("Parser started")
    print("Parser started")

    client.start()
    client.run_until_disconnected()
