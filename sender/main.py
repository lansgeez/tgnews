# sender/main.py
import os
import json
import time
import logging
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from aiogram import Bot, Dispatcher
from aiogram.types import FSInputFile, MessageEntity, InputMediaPhoto, InputMediaVideo
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from prometheus_client import Counter, Gauge, Histogram, start_http_server

import config

SERVICE_NAME = os.getenv("SERVICE_NAME", "sender")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9102"))

DOWNLOADS_DIR = os.getenv("DOWNLOADS_DIR", "downloads")

RATE_LIMIT = int(os.getenv("RATE_LIMIT_SECONDS", "30"))  # sec
ALBUM_TIMEOUT = float(os.getenv("ALBUM_TIMEOUT_SECONDS", "2.0"))  # sec
FILE_WAIT_TIMEOUT = int(os.getenv("FILE_WAIT_TIMEOUT_SECONDS", "15"))  # sec

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
    format="%(asctime)s %(levelname)s %(message)s",
)

S_CONSUME = Counter("tgnews_sender_consume_total", "Consumed kafka messages", ["kind"])
S_SEND = Counter("tgnews_sender_send_total", "Telegram sends", ["type", "status"])
S_ALBUM_FLUSH = Counter("tgnews_sender_album_flush_total", "Album flushes", ["status"])
S_RATE_WAIT = Counter("tgnews_sender_rate_limit_wait_seconds_total", "Seconds waited by rate limit")
S_FILE_WAIT_FAIL = Counter("tgnews_sender_file_wait_fail_total", "File wait failures")
S_FILE_WAIT_SECONDS = Histogram("tgnews_sender_file_wait_seconds", "Seconds spent waiting for files")
S_LAST_TS = Gauge("tgnews_sender_last_event_timestamp", "Unix timestamp of last processed kafka message")

if not config.TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN is not set (check .env / docker-compose environment)")

bot = Bot(token=config.TELEGRAM_TOKEN)
dp = Dispatcher()

_LAST_SEND_TIME = 0.0
_send_lock = asyncio.Lock()

async def send_limited(fn, *args, **kwargs):
    global _LAST_SEND_TIME
    async with _send_lock:
        now = time.time()
        if now - _LAST_SEND_TIME < RATE_LIMIT:
            wait = RATE_LIMIT - (now - _LAST_SEND_TIME)
            logging.info(f"⏳ Rate limit: ждём {wait:.1f} сек...")
            S_RATE_WAIT.inc(wait)
            await asyncio.sleep(wait)

        _LAST_SEND_TIME = time.time()
        return await fn(*args, **kwargs)

async def wait_for_file(path: str, timeout_seconds: int = FILE_WAIT_TIMEOUT) -> bool:
    t0 = time.time()
    try:
        last_size = -1
        stable_ticks = 0

        steps = int(timeout_seconds / 0.1)
        for _ in range(max(1, steps)):
            if os.path.exists(path):
                try:
                    size = os.path.getsize(path)
                except OSError:
                    size = 0

                if size > 0:
                    if size == last_size:
                        stable_ticks += 1
                    else:
                        stable_ticks = 0
                        last_size = size

                    if stable_ticks >= 3:
                        return True

            await asyncio.sleep(0.1)

        S_FILE_WAIT_FAIL.inc()
        return False
    finally:
        S_FILE_WAIT_SECONDS.observe(time.time() - t0)

def convert_telethon_entities(telethon_entities):
    aiogram_entities: List[MessageEntity] = []
    for ent in telethon_entities or []:
        if not isinstance(ent, dict):
            continue

        aiogram_type = ent.get("type", "").lower().replace("messageentity", "")
        if aiogram_type == "texturl":
            aiogram_type = "text_link"
        if aiogram_type == "strike":
            aiogram_type = "strikethrough"

        supported = {
            "bold", "italic", "code", "pre", "underline", "strikethrough",
            "spoiler", "text_link", "text_mention", "blockquote", "url",
        }
        if aiogram_type not in supported:
            continue

        params = {
            "type": aiogram_type,
            "offset": int(ent.get("offset", 0) or 0),
            "length": int(ent.get("length", 0) or 0),
        }
        if ent.get("url"):
            params["url"] = ent["url"]

        try:
            aiogram_entities.append(MessageEntity(**params))
        except Exception:
            logging.exception("Entity parse error")

    return aiogram_entities

def postfix_kind(postfix: str) -> str:
    postfix = (postfix or "").lower()
    if postfix in [".jpg", ".jpeg", ".png", ".webp"]:
        return "photo"
    if postfix in [".mp4", ".mov"]:
        return "video"
    if postfix in [".gif"]:
        return "video"
    return "other"

@dataclass
class AlbumItem:
    msg_id: int
    path: str
    postfix: str
    text: str
    entities: List[MessageEntity]

album_buffer: Dict[str, List[AlbumItem]] = defaultdict(list)
album_timers: Dict[str, asyncio.Task] = {}
album_last_seen: Dict[str, float] = {}

async def extendable_album_timer(group_key: str, target_channel: str):
    try:
        while True:
            last = album_last_seen.get(group_key, time.time())
            remaining = (last + ALBUM_TIMEOUT) - time.time()
            if remaining <= 0:
                break
            await asyncio.sleep(min(remaining, 0.5))

        await send_album(group_key, target_channel)
    except asyncio.CancelledError:
        return
    except Exception:
        logging.exception(f"Album timer crashed for {group_key}")
        safe_cleanup_album_state(group_key)

async def send_album(group_key: str, target_channel: str):
    items = album_buffer.get(group_key, [])
    if not items:
        return

    try:
        items.sort(key=lambda x: x.msg_id)
    except Exception:
        pass

    caption_idx: Optional[int] = None
    for i, it in enumerate(items):
        if (it.text or "").strip():
            caption_idx = i
            break
    if caption_idx is None:
        caption_idx = 0

    caption_text = (items[caption_idx].text or "").strip()
    caption_entities = items[caption_idx].entities or []

    media_group = []
    for i, it in enumerate(items):
        kind = postfix_kind(it.postfix)

        ok = await wait_for_file(it.path)
        if not ok:
            logging.error(f"❌ File wait timeout: {it.path}")
            continue

        is_caption_item = (i == caption_idx and bool(caption_text))
        cap = caption_text if is_caption_item else None
        cap_entities = caption_entities if is_caption_item else None

        if kind == "photo":
            media_group.append(
                InputMediaPhoto(
                    media=FSInputFile(it.path),
                    caption=cap,
                    caption_entities=cap_entities,
                )
            )
        elif kind == "video":
            media_group.append(
                InputMediaVideo(
                    media=FSInputFile(it.path),
                    caption=cap,
                    caption_entities=cap_entities,
                )
            )
        else:
            logging.warning(f"⚠ Unsupported album item: postfix={it.postfix} path={it.path}")

    if not media_group:
        logging.error(f"⚠ Album {group_key} has no sendable items.")
        cleanup_album_files(items)
        safe_cleanup_album_state(group_key)
        S_ALBUM_FLUSH.labels(status="error").inc()
        return

    try:
        logging.info(f"📤 Sending album {group_key}: {len(media_group)} items (caption_idx={caption_idx})")
        await send_limited(bot.send_media_group, target_channel, media_group)
        S_ALBUM_FLUSH.labels(status="ok").inc()
        S_SEND.labels(type="album", status="ok").inc()
    except Exception:
        logging.exception(f"❌ Album send error for {group_key}")
        S_ALBUM_FLUSH.labels(status="error").inc()
        S_SEND.labels(type="album", status="error").inc()
        safe_cleanup_album_state(group_key)
        return

    cleanup_album_files(items)
    safe_cleanup_album_state(group_key)

def cleanup_album_files(items: List[AlbumItem]):
    for it in items:
        try:
            os.remove(it.path)
        except Exception:
            pass
    try:
        folder = os.path.dirname(items[0].path)
        if os.path.isdir(folder) and not os.listdir(folder):
            os.rmdir(folder)
    except Exception:
        pass

def safe_cleanup_album_state(group_key: str):
    album_buffer.pop(group_key, None)
    album_last_seen.pop(group_key, None)
    t = album_timers.pop(group_key, None)
    if t:
        try:
            t.cancel()
        except Exception:
            pass

async def create_consumer():
    while True:
        try:
            logging.info(f"🔄 [{SERVICE_NAME}] Connecting to Kafka: {config.KAFKA_BOOTSTRAP_SERVERS} topic={config.KAFKA_TOPIC}")
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                group_id="aiogram-sender-v3",
                enable_auto_commit=True,
            )
            logging.info(f"✅ [{SERVICE_NAME}] Kafka consumer connected.")
            return consumer
        except NoBrokersAvailable:
            logging.warning(f"❌ [{SERVICE_NAME}] Kafka not ready. Retry in 3s...")
            await asyncio.sleep(3)

async def kafka_consume():
    consumer = await create_consumer()
    target_channel = config.TARGET_CHANNEL

    try:
        for msg in consumer:
            data: Dict[str, Any] = msg.value
            S_LAST_TS.set(time.time())
            logging.info(f"Kafka message: {data}")

            text = data.get("text", "") or ""
            entities = convert_telethon_entities(data.get("entities", []))

            channel_id = data.get("channel_id")
            grouped_id = data.get("grouped_id")
            msg_id = int(data.get("msg_id") or 0)
            has_media = bool(data.get("has_media", False))
            postfix = data.get("postfix")

            if (not has_media) or (not postfix):
                S_CONSUME.labels(kind="text").inc()
                try:
                    await send_limited(bot.send_message, target_channel, text=text, entities=entities)
                    S_SEND.labels(type="message", status="ok").inc()
                except Exception:
                    logging.exception("❌ send_message error")
                    S_SEND.labels(type="message", status="error").inc()
                continue

            folder_suffix = grouped_id if grouped_id is not None else "single"
            folder = os.path.join(DOWNLOADS_DIR, f"{channel_id}_{folder_suffix}")
            path = os.path.join(folder, f"{msg_id}{postfix}")

            if grouped_id is not None:
                S_CONSUME.labels(kind="album_item").inc()
                group_key = f"{channel_id}_{grouped_id}"

                album_buffer[group_key].append(
                    AlbumItem(
                        msg_id=msg_id,
                        path=path,
                        postfix=postfix,
                        text=text,
                        entities=entities,
                    )
                )
                album_last_seen[group_key] = time.time()

                if group_key not in album_timers:
                    album_timers[group_key] = asyncio.create_task(
                        extendable_album_timer(group_key, target_channel)
                    )
                continue

            S_CONSUME.labels(kind="single_media").inc()
            kind = postfix_kind(postfix)

            ok = await wait_for_file(path)
            if not ok:
                logging.error(f"❌ file wait timeout: {path}")
                S_SEND.labels(type=kind, status="error").inc()
                continue

            try:
                input_file = FSInputFile(path)
                if kind == "photo":
                    await send_limited(
                        bot.send_photo, target_channel,
                        photo=input_file,
                        caption=text or None,
                        caption_entities=entities if text else None,
                    )
                    S_SEND.labels(type="photo", status="ok").inc()
                elif kind == "video":
                    await send_limited(
                        bot.send_video, target_channel,
                        video=input_file,
                        caption=text or None,
                        caption_entities=entities if text else None,
                    )
                    S_SEND.labels(type="video", status="ok").inc()
                else:
                    await send_limited(bot.send_message, target_channel, text=text, entities=entities)
                    S_SEND.labels(type="message", status="ok").inc()

                try:
                    os.remove(path)
                except Exception:
                    pass
                try:
                    if os.path.isdir(folder) and not os.listdir(folder):
                        os.rmdir(folder)
                except Exception:
                    pass

            except Exception:
                logging.exception("❌ media send error")
                S_SEND.labels(type=kind, status="error").inc()

    finally:
        try:
            consumer.close()
        except Exception:
            pass
        logging.info("Sender stopped")

if __name__ == "__main__":
    start_http_server(METRICS_PORT)
    logging.info(f"📈 [{SERVICE_NAME}] metrics on :{METRICS_PORT}/metrics")
    try:
        asyncio.run(kafka_consume())
    except KeyboardInterrupt:
        print("Stopping…")
