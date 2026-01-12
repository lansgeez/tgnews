import os, json, time, logging, numpy as np, torch
from collections import deque

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from detoxify import Detoxify
from sentence_transformers import SentenceTransformer, util

SERVICE_NAME = os.getenv("SERVICE_NAME", "ai_filter")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9103"))

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
IN_TOPIC = os.getenv("KAFKA_IN_TOPIC", "news_raw")
OUT_TOPIC = os.getenv("KAFKA_OUT_TOPIC", "news_filtered")
REJECT_TOPIC = os.getenv("KAFKA_REJECT_TOPIC", "news_rejected")

MODERATION_THRESHOLD = float(os.getenv("MODERATION_THRESHOLD", "0.70"))
DUP_SIM_THRESHOLD = float(os.getenv("DUP_SIM_THRESHOLD", "0.88"))
DUP_CACHE_SIZE = int(os.getenv("DUP_CACHE_SIZE", "2000"))

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()],
                    format="%(asctime)s %(levelname)s %(message)s")

# -------------------- METRICS --------------------
F_IN = Counter("tgnews_aifilter_in_total", "Input events", ["kind"])
F_OUT = Counter("tgnews_aifilter_out_total", "Output decisions", ["decision", "reason"])
F_LAST_TS = Gauge("tgnews_aifilter_last_event_timestamp", "Unix ts last event")
F_MOD_TIME = Histogram("tgnews_aifilter_moderation_seconds", "Moderation inference seconds")
F_DUP_TIME = Histogram("tgnews_aifilter_duplicate_seconds", "Duplicate check seconds")

def norm_text(s: str) -> str:
    s = (s or "").strip()
    return " ".join(s.split())

def create_consumer():
    while True:
        try:
            return KafkaConsumer(
                IN_TOPIC,
                bootstrap_servers=BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                group_id="ai-filter-v1",
                enable_auto_commit=True,
            )
        except NoBrokersAvailable:
            logging.warning("Kafka not ready. Retry in 3s...")
            time.sleep(3)

def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            logging.warning("Kafka not ready. Retry in 3s...")
            time.sleep(3)

# -------------------- MODELS --------------------
# 1) Модерация: Detoxify (multilingual, реальный фильтр токсичности)
moderation_model = Detoxify("multilingual", device="cpu")

# 2) Антидубль: all-MiniLM-L6-v2
embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

def moderation_score(text: str) -> float:
    if not text:
        return 0.0
    try:
        with F_MOD_TIME.time():
            scores = moderation_model.predict(text[:512])
            # scores: {toxic, severe_toxic, obscene, threat, insult, identity_hate}
            max_score = max(scores.values()) if scores else 0.0
        return float(max_score)
    except Exception as e:
        logging.error(f"Moderation error: {e}")
        return 0.0

def is_duplicate(text: str, recent_embeddings: deque) -> float:
    if not text or len(recent_embeddings) < 2:
        return 0.0
    
    try:
        with F_DUP_TIME.time():
            emb = embedder.encode(text)  # numpy array [384]
            
            recent_list = [torch.from_numpy(r) if isinstance(r, np.ndarray) else r.cpu() 
                          for r in list(recent_embeddings)[-50:]]
            
            if len(recent_list) < 2:
                return 0.0
            
            recent_2d = torch.stack(recent_list).float()
            emb_2d = torch.from_numpy(emb).float().unsqueeze(0)  # [1, 384]
            
            sims = util.cos_sim(emb_2d, recent_2d)[0]
            return float(sims.max().item())
    except Exception as e:
        logging.error(f"Dup check error: {e}")
        return 0.0

def main():
    start_http_server(METRICS_PORT)
    logging.info(f"[{SERVICE_NAME}] metrics :{METRICS_PORT}/metrics")

    consumer = create_consumer()
    producer = create_producer()

    recent_embeddings = deque(maxlen=DUP_CACHE_SIZE)

    for msg in consumer:
        data = msg.value or {}
        F_LAST_TS.set(time.time())

        text = norm_text(data.get("text", ""))
        has_media = bool(data.get("has_media", False))
        F_IN.labels(kind="media" if has_media else "text").inc()

        # -------- 1) МОДЕРАЦИЯ (Detoxify) --------
        mod_score = moderation_score(text)

        # -------- 2) АНТИДУБЛЬ --------
        dup_score = is_duplicate(text, recent_embeddings)

        # обновляем кеш эмбеддингов
        if text:
            emb = embedder.encode(text, convert_to_tensor=True, normalize_embeddings=True)
            recent_embeddings.append(emb)

        # -------- DECISION --------
        decision = "publish"
        reason = "ok"

        if mod_score >= MODERATION_THRESHOLD:
            decision, reason = "reject", "moderation"

        if decision == "publish" and dup_score >= DUP_SIM_THRESHOLD:
            decision, reason = "reject", "duplicate"

        out_event = dict(data)
        out_event["filter"] = {
            "decision": decision,
            "reason": reason,
            "moderation_score": mod_score,
            "duplicate_score": dup_score,
            "ts": time.time(),
        }

        if decision == "publish":
            producer.send(OUT_TOPIC, value=out_event)
            F_OUT.labels(decision="publish", reason=reason).inc()
            logging.info(f"PUBLISH: {text[:50]}... (mod={mod_score:.2f}, dup={dup_score:.2f})")
        else:
            producer.send(REJECT_TOPIC, value=out_event)
            F_OUT.labels(decision="reject", reason=reason).inc()
            logging.info(f"REJECT [{reason}]: {text[:50]}... (mod={mod_score:.2f}, dup={dup_score:.2f})")

        producer.flush()

if __name__ == "__main__":
    main()
