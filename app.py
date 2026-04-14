from flask import Flask, request, jsonify, Response
import json
import os
import time
import datetime
import csv
import io
import sqlite3
from pyproj import Transformer

app = Flask(__name__)

DB_PATH = "db.json"          # 初回移行用
SQLITE_PATH = "allcot.db"    # 本番DB

DEVICE_TOKEN_GLOBAL = os.environ.get("DEVICE_TOKEN_GLOBAL", "")

MANUAL_VALVE_ON = 1
MANUAL_VALVE_OFF = 0


# =========================
# Common helpers
# =========================
def now_ts():
    return int(time.time())


def norm_start_local(s: str):
    if not s:
        return ""

    s = str(s).strip()
    if s.endswith("+09:00"):
        s = s[:-6]
    if s.endswith("Z"):
        s = s[:-1]
    if len(s) == 16:
        s = s + ":00"
    return s


def parse_start_local_to_ts(start_local: str) -> int:
    try:
        dt = datetime.datetime.fromisoformat(start_local)  # naive JST
        utc_dt = dt - datetime.timedelta(hours=9)
        return int(utc_dt.replace(tzinfo=datetime.timezone.utc).timestamp())
    except Exception:
        return 0


def convert_to_latlon(body: dict):
    lat = body.get("lat")
    lon = body.get("lon")
    if lat not in [None, ""] and lon not in [None, ""]:
        return float(lat), float(lon)

    x = body.get("jgd2011_x")
    y = body.get("jgd2011_y")
    zone = body.get("zone")
    if x in [None, ""] or y in [None, ""] or zone in [None, ""]:
        return None, None

    try:
        zone_i = int(zone)
        if not (1 <= zone_i <= 19):
            return None, None

        epsg_src = 6668 + zone_i
        tf = Transformer.from_crs(f"EPSG:{epsg_src}", "EPSG:4326", always_xy=True)
        lon2, lat2 = tf.transform(float(x), float(y))
        return float(lat2), float(lon2)
    except Exception:
        return None, None


def to_num(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


# =========================
# SQLite helpers
# =========================
def get_conn():
    conn = sqlite3.connect(SQLITE_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pins (
            pin_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            template TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            note TEXT DEFAULT ''
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS latest (
            pin_id TEXT PRIMARY KEY,
            ts INTEGER DEFAULT 0,
            data_json TEXT DEFAULT '{}',
            ack_json TEXT DEFAULT '{}'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pin_id TEXT NOT NULL,
            ts INTEGER DEFAULT 0,
            soil REAL,
            rssi_up REAL,
            rssi_down REAL,
            noise REAL,
            valve_state TEXT,
            valve_open TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS command (
            pin_id TEXT PRIMARY KEY,
            cmd_id INTEGER DEFAULT 0,
            type TEXT DEFAULT 'manual',
            valve INTEGER DEFAULT 0,
            duration_sec INTEGER DEFAULT 0,
            threshold TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS schedule_meta (
            pin_id TEXT PRIMARY KEY,
            next_sid INTEGER DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS schedule_items (
            pin_id TEXT NOT NULL,
            sid TEXT NOT NULL,
            start_local TEXT,
            minutes INTEGER DEFAULT 3,
            status TEXT DEFAULT 'scheduled',
            created_at INTEGER,
            opened_at INTEGER,
            closed_at INTEGER,
            executed_at INTEGER,
            PRIMARY KEY (pin_id, sid)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS device_tokens (
            pin_id TEXT PRIMARY KEY,
            token TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS acks (
            pin_id TEXT PRIMARY KEY,
            cmd_id INTEGER DEFAULT 0,
            ts INTEGER DEFAULT 0,
            ok INTEGER DEFAULT 1,
            detail TEXT DEFAULT ''
        )
    """)

    conn.commit()
    conn.close()


def meta_get(cur, key, default=None):
    row = cur.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(row["value"])
    except Exception:
        return default


def meta_set(cur, key, value):
    cur.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)",
        (key, json.dumps(value, ensure_ascii=False))
    )


def migrate_json_to_sqlite_if_needed():
    conn = get_conn()
    cur = conn.cursor()
    row = cur.execute("SELECT COUNT(*) AS c FROM pins").fetchone()
    pin_count = int(row["c"] or 0)
    conn.close()

    if pin_count > 0:
        return

    if not os.path.exists(DB_PATH):
        return

    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            old_db = json.load(f)
    except Exception:
        return

    if not isinstance(old_db, dict):
        return

    old_db.setdefault("next_id", 1)
    old_db.setdefault("free_ids", [])
    old_db.setdefault("pins", {})
    old_db.setdefault("latest", {})
    old_db.setdefault("logs", {})
    old_db.setdefault("command", {})
    old_db.setdefault("schedule", {})
    old_db.setdefault("device_tokens", {})
    old_db.setdefault("acks", {})

    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN")

        meta_set(cur, "next_id", int(old_db.get("next_id", 1)))
        meta_set(cur, "free_ids", old_db.get("free_ids", []))

        for pid, p in old_db["pins"].items():
            cur.execute("""
                INSERT OR REPLACE INTO pins(pin_id, name, template, lat, lon, note)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                str(pid),
                p.get("name", ""),
                p.get("template", "A"),
                float(p.get("lat", 0)),
                float(p.get("lon", 0)),
                p.get("note", "")
            ))

        for pid, latest in old_db["latest"].items():
            cur.execute("""
                INSERT OR REPLACE INTO latest(pin_id, ts, data_json, ack_json)
                VALUES (?, ?, ?, ?)
            """, (
                str(pid),
                int(latest.get("ts", 0) or 0),
                json.dumps(latest.get("data", {}) or {}, ensure_ascii=False),
                json.dumps(latest.get("ack", {}) or {}, ensure_ascii=False)
            ))

        for pid, logs in old_db["logs"].items():
            for it in logs:
                cur.execute("""
                    INSERT INTO logs(pin_id, ts, soil, rssi_up, rssi_down, noise, valve_state, valve_open)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(pid),
                    int(it.get("ts", 0) or 0),
                    it.get("soil"),
                    it.get("rssi_up"),
                    it.get("rssi_down"),
                    it.get("noise"),
                    it.get("valve_state"),
                    None if "valve_open" not in it else str(it.get("valve_open"))
                ))

        for pid, cmd in old_db["command"].items():
            cur.execute("""
                INSERT OR REPLACE INTO command(pin_id, cmd_id, type, valve, duration_sec, threshold)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                str(pid),
                int(cmd.get("cmd_id", 0) or 0),
                str(cmd.get("type", "manual")),
                int(cmd.get("valve", 0) or 0),
                int(cmd.get("duration_sec", 0) or 0),
                json.dumps(cmd.get("threshold"), ensure_ascii=False)
            ))

        for pid, sch in old_db["schedule"].items():
            cur.execute("""
                INSERT OR REPLACE INTO schedule_meta(pin_id, next_sid)
                VALUES (?, ?)
            """, (
                str(pid),
                int(sch.get("next_sid", 1) or 1)
            ))
            for it in sch.get("items", []):
                cur.execute("""
                    INSERT OR REPLACE INTO schedule_items(
                        pin_id, sid, start_local, minutes, status,
                        created_at, opened_at, closed_at, executed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(pid),
                    str(it.get("id", "")),
                    str(it.get("start_local", "")),
                    int(it.get("minutes", 0) or 0),
                    str(it.get("status", "scheduled")),
                    it.get("created_at"),
                    it.get("opened_at"),
                    it.get("closed_at"),
                    it.get("executed_at")
                ))

        for pid, token in old_db["device_tokens"].items():
            cur.execute("""
                INSERT OR REPLACE INTO device_tokens(pin_id, token)
                VALUES (?, ?)
            """, (
                str(pid),
                str(token)
            ))

        for pid, a in old_db["acks"].items():
            cur.execute("""
                INSERT OR REPLACE INTO acks(pin_id, cmd_id, ts, ok, detail)
                VALUES (?, ?, ?, ?, ?)
            """, (
                str(pid),
                int(a.get("cmd_id", 0) or 0),
                int(a.get("ts", 0) or 0),
                1 if bool(a.get("ok", True)) else 0,
                str(a.get("detail", "") or "")
            ))

        conn.commit()
        print("[MIGRATE] db.json -> allcot.db 完了")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# =========================
# Read helpers
# =========================
def get_all_pins():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM pins ORDER BY CAST(pin_id AS INTEGER), pin_id").fetchall()
    conn.close()

    out = {}
    for r in rows:
        out[r["pin_id"]] = {
            "name": r["name"],
            "template": r["template"],
            "lat": r["lat"],
            "lon": r["lon"],
            "note": r["note"] or ""
        }
    return out


def get_pin(pin_id: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM pins WHERE pin_id=?", (str(pin_id),)).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "name": row["name"],
        "template": row["template"],
        "lat": row["lat"],
        "lon": row["lon"],
        "note": row["note"] or ""
    }


def get_latest(pin_id: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM latest WHERE pin_id=?", (str(pin_id),)).fetchone()
    conn.close()
    if not row:
        return {}

    try:
        data = json.loads(row["data_json"] or "{}")
    except Exception:
        data = {}

    try:
        ack = json.loads(row["ack_json"] or "{}")
    except Exception:
        ack = {}

    out = {
        "ts": int(row["ts"] or 0),
        "data": data
    }
    if ack:
        out["ack"] = ack
    return out


def get_logs(pin_id: str, limit: int = None):
    conn = get_conn()
    if limit is None:
        rows = conn.execute("""
            SELECT ts, soil, rssi_up, rssi_down, noise, valve_state, valve_open
            FROM logs
            WHERE pin_id=?
            ORDER BY id ASC
        """, (str(pin_id),)).fetchall()
    else:
        rows = conn.execute("""
            SELECT * FROM (
                SELECT ts, soil, rssi_up, rssi_down, noise, valve_state, valve_open
                FROM logs
                WHERE pin_id=?
                ORDER BY id DESC
                LIMIT ?
            ) t
            ORDER BY ts ASC
        """, (str(pin_id), int(limit))).fetchall()
    conn.close()

    out = []
    for r in rows:
        item = {
            "ts": int(r["ts"] or 0),
            "soil": r["soil"],
            "rssi_up": r["rssi_up"],
            "rssi_down": r["rssi_down"],
            "noise": r["noise"],
        }
        if r["valve_state"] is not None:
            item["valve_state"] = r["valve_state"]
        if r["valve_open"] is not None:
            item["valve_open"] = r["valve_open"]
        out.append(item)
    return out


def get_command(pin_id: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM command WHERE pin_id=?", (str(pin_id),)).fetchone()
    conn.close()
    if not row:
        return {
            "cmd_id": 0,
            "type": "manual",
            "valve": MANUAL_VALVE_OFF,
            "duration_sec": 0,
            "threshold": None
        }

    threshold = None
    if row["threshold"] not in [None, "", "null"]:
        try:
            threshold = json.loads(row["threshold"])
        except Exception:
            threshold = row["threshold"]

    return {
        "cmd_id": int(row["cmd_id"] or 0),
        "type": row["type"] or "manual",
        "valve": int(row["valve"] or 0),
        "duration_sec": int(row["duration_sec"] or 0),
        "threshold": threshold
    }


def get_ack_info(pin_id: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM acks WHERE pin_id=?", (str(pin_id),)).fetchone()
    conn.close()
    if row:
        return {
            "cmd_id": int(row["cmd_id"] or 0),
            "ts": int(row["ts"] or 0),
            "ok": bool(row["ok"]),
            "detail": row["detail"] or ""
        }

    latest = get_latest(pin_id)
    ack = latest.get("ack") or {}
    return {
        "cmd_id": int(ack.get("cmd_id", 0) or 0),
        "ts": int(ack.get("ts", 0) or 0),
        "ok": bool(ack.get("ok", True)),
        "detail": str(ack.get("detail", "") or "")
    }


def get_schedule(pin_id: str):
    conn = get_conn()
    meta = conn.execute("SELECT * FROM schedule_meta WHERE pin_id=?", (str(pin_id),)).fetchone()
    rows = conn.execute("""
        SELECT * FROM schedule_items
        WHERE pin_id=?
        ORDER BY CAST(sid AS INTEGER), sid
    """, (str(pin_id),)).fetchall()
    conn.close()

    out = {
        "items": [],
        "next_sid": int(meta["next_sid"] or 1) if meta else 1
    }

    for r in rows:
        item = {
            "id": r["sid"],
            "start_local": r["start_local"] or "",
            "minutes": int(r["minutes"] or 0),
            "status": r["status"] or "scheduled"
        }
        if r["created_at"] is not None:
            item["created_at"] = int(r["created_at"])
        if r["opened_at"] is not None:
            item["opened_at"] = int(r["opened_at"])
        if r["closed_at"] is not None:
            item["closed_at"] = int(r["closed_at"])
        if r["executed_at"] is not None:
            item["executed_at"] = int(r["executed_at"])
        out["items"].append(item)

    return out


def get_device_tokens():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM device_tokens").fetchall()
    conn.close()
    return {r["pin_id"]: r["token"] for r in rows}


# =========================
# Write helpers
# =========================
def allocate_id():
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        free_ids = meta_get(cur, "free_ids", [])
        if free_ids:
            free_ids = sorted([int(x) for x in free_ids])
            pid = str(free_ids.pop(0))
            meta_set(cur, "free_ids", free_ids)
            conn.commit()
            return pid

        nid = int(meta_get(cur, "next_id", 1))
        meta_set(cur, "next_id", nid + 1)
        conn.commit()
        return str(nid)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def release_id(pid: str):
    try:
        pid_int = int(pid)
    except Exception:
        return

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        free_ids = meta_get(cur, "free_ids", [])
        if pid_int not in free_ids:
            free_ids.append(pid_int)
            meta_set(cur, "free_ids", free_ids)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_pin(pin_id: str, name: str, template: str, lat: float, lon: float, note: str):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO pins(pin_id, name, template, lat, lon, note)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (str(pin_id), name, template, float(lat), float(lon), note))
    conn.commit()
    conn.close()


def update_pin_fields(pin_id: str, name: str, template: str, note: str, lat=None, lon=None):
    conn = get_conn()
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM pins WHERE pin_id=?", (str(pin_id),)).fetchone()
    if not row:
        conn.close()
        return False

    new_lat = float(lat) if lat is not None else float(row["lat"])
    new_lon = float(lon) if lon is not None else float(row["lon"])

    cur.execute("""
        UPDATE pins
        SET name=?, template=?, note=?, lat=?, lon=?
        WHERE pin_id=?
    """, (name, template, note, new_lat, new_lon, str(pin_id)))
    conn.commit()
    conn.close()
    return True


def delete_pin_all(pin_id: str):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.execute("DELETE FROM pins WHERE pin_id=?", (str(pin_id),))
        cur.execute("DELETE FROM latest WHERE pin_id=?", (str(pin_id),))
        cur.execute("DELETE FROM logs WHERE pin_id=?", (str(pin_id),))
        cur.execute("DELETE FROM command WHERE pin_id=?", (str(pin_id),))
        cur.execute("DELETE FROM schedule_meta WHERE pin_id=?", (str(pin_id),))
        cur.execute("DELETE FROM schedule_items WHERE pin_id=?", (str(pin_id),))
        cur.execute("DELETE FROM device_tokens WHERE pin_id=?", (str(pin_id),))
        cur.execute("DELETE FROM acks WHERE pin_id=?", (str(pin_id),))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_default_records(pin_id: str):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT OR IGNORE INTO command(pin_id, cmd_id, type, valve, duration_sec, threshold)
            VALUES (?, 0, 'manual', ?, 0, NULL)
        """, (str(pin_id), MANUAL_VALVE_OFF))

        cur.execute("""
            INSERT OR IGNORE INTO schedule_meta(pin_id, next_sid)
            VALUES (?, 1)
        """, (str(pin_id),))

        cur.execute("""
            INSERT OR IGNORE INTO acks(pin_id, cmd_id, ts, ok, detail)
            VALUES (?, 0, 0, 1, '')
        """, (str(pin_id),))

        conn.commit()
    finally:
        conn.close()


def upsert_latest_data(pin_id: str, ts: int, data: dict):
    conn = get_conn()
    cur = conn.cursor()
    old = cur.execute("SELECT ack_json FROM latest WHERE pin_id=?", (str(pin_id),)).fetchone()
    ack_json = old["ack_json"] if old else "{}"

    cur.execute("""
        INSERT OR REPLACE INTO latest(pin_id, ts, data_json, ack_json)
        VALUES (?, ?, ?, ?)
    """, (
        str(pin_id),
        int(ts),
        json.dumps(data or {}, ensure_ascii=False),
        ack_json or "{}"
    ))
    conn.commit()
    conn.close()


def upsert_latest_ack(pin_id: str, ts: int, ack_obj: dict):
    conn = get_conn()
    cur = conn.cursor()
    old = cur.execute("SELECT data_json FROM latest WHERE pin_id=?", (str(pin_id),)).fetchone()
    data_json = old["data_json"] if old else "{}"

    cur.execute("""
        INSERT OR REPLACE INTO latest(pin_id, ts, data_json, ack_json)
        VALUES (?, ?, ?, ?)
    """, (
        str(pin_id),
        int(ts),
        data_json or "{}",
        json.dumps(ack_obj or {}, ensure_ascii=False)
    ))
    conn.commit()
    conn.close()


def add_log(pin_id: str, item: dict):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO logs(pin_id, ts, soil, rssi_up, rssi_down, noise, valve_state, valve_open)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        str(pin_id),
        int(item.get("ts", 0) or 0),
        item.get("soil"),
        item.get("rssi_up"),
        item.get("rssi_down"),
        item.get("noise"),
        item.get("valve_state"),
        None if "valve_open" not in item else str(item.get("valve_open"))
    ))

    cur.execute("""
        DELETE FROM logs
        WHERE pin_id=?
          AND id NOT IN (
              SELECT id FROM logs
              WHERE pin_id=?
              ORDER BY id DESC
              LIMIT 2000
          )
    """, (str(pin_id), str(pin_id)))

    conn.commit()
    conn.close()


def set_ack(pin_id: str, cmd_id: int, ts: int, ok: bool, detail: str):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO acks(pin_id, cmd_id, ts, ok, detail)
        VALUES (?, ?, ?, ?, ?)
    """, (
        str(pin_id),
        int(cmd_id),
        int(ts),
        1 if ok else 0,
        detail or ""
    ))
    conn.commit()
    conn.close()


def set_command(pin_id: str, cmd: dict):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO command(pin_id, cmd_id, type, valve, duration_sec, threshold)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        str(pin_id),
        int(cmd.get("cmd_id", 0) or 0),
        str(cmd.get("type", "manual")),
        int(cmd.get("valve", MANUAL_VALVE_OFF) or 0),
        int(cmd.get("duration_sec", 0) or 0),
        json.dumps(cmd.get("threshold"), ensure_ascii=False)
    ))
    conn.commit()
    conn.close()


def add_schedule_item(pin_id: str, start_local: str, minutes: int):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        row = cur.execute("SELECT next_sid FROM schedule_meta WHERE pin_id=?", (str(pin_id),)).fetchone()
        next_sid = int(row["next_sid"] or 1) if row else 1

        if not row:
            cur.execute("""
                INSERT INTO schedule_meta(pin_id, next_sid)
                VALUES (?, ?)
            """, (str(pin_id), next_sid + 1))
        else:
            cur.execute("""
                UPDATE schedule_meta SET next_sid=?
                WHERE pin_id=?
            """, (next_sid + 1, str(pin_id)))

        sid = str(next_sid)
        cur.execute("""
            INSERT INTO schedule_items(
                pin_id, sid, start_local, minutes, status, created_at
            )
            VALUES (?, ?, ?, ?, 'scheduled', ?)
        """, (
            str(pin_id),
            sid,
            start_local,
            int(minutes),
            now_ts()
        ))

        conn.commit()
        return {
            "id": sid,
            "start_local": start_local,
            "minutes": int(minutes),
            "status": "scheduled",
            "created_at": now_ts()
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def delete_schedule_item(pin_id: str, sid: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM schedule_items WHERE pin_id=? AND sid=?", (str(pin_id), str(sid)))
    affected = cur.rowcount
    conn.commit()
    conn.close()
    return affected > 0


def update_schedule_status(pin_id: str, sid: str, status: str, field_name: str):
    conn = get_conn()
    cur = conn.cursor()
    sql = f"UPDATE schedule_items SET status=?, {field_name}=? WHERE pin_id=? AND sid=?"
    cur.execute(sql, (status, now_ts(), str(pin_id), str(sid)))
    conn.commit()
    conn.close()


# =========================
# Domain helpers
# =========================
def pin_exists(pin_id: str) -> bool:
    return get_pin(pin_id) is not None


def device_token_ok(pin_id: str) -> bool:
    token = (request.args.get("token") or "").strip()
    if not token:
        return False

    if DEVICE_TOKEN_GLOBAL:
        return token == DEVICE_TOKEN_GLOBAL

    device_tokens = get_device_tokens()
    return token == (device_tokens.get(str(pin_id)) or "")


def is_command_pending(pin_id: str) -> bool:
    cmd = get_command(pin_id)
    ack = get_ack_info(pin_id)

    cmd_id = int(cmd.get("cmd_id", 0) or 0)
    ack_id = int(ack.get("cmd_id", 0) or 0)
    return cmd_id != 0 and cmd_id != ack_id


def compute_valve_status(pin_id: str):
    latest = get_latest(pin_id) or {}
    data = latest.get("data", {}) or {}
    cmd = get_command(pin_id) or {}
    ack = get_ack_info(pin_id)

    cmd_id = int(cmd.get("cmd_id", 0) or 0)
    ack_id = int(ack.get("cmd_id", 0) or 0)
    pending = (cmd_id != 0 and cmd_id != ack_id)

    if "valve_state" in data:
        raw = str(data.get("valve_state", "")).strip().lower()
        if raw in ["open", "opened", "on", "1", "true"]:
            return {"value": "open", "label": "開", "source": "telemetry", "pending": pending}
        if raw in ["close", "closed", "off", "0", "false"]:
            return {"value": "closed", "label": "閉", "source": "telemetry", "pending": pending}

    if "valve_open" in data:
        try:
            b = bool(int(data.get("valve_open")))
        except Exception:
            b = bool(data.get("valve_open"))
        return {
            "value": "open" if b else "closed",
            "label": "開" if b else "閉",
            "source": "telemetry",
            "pending": pending
        }

    if pending:
        return {"value": "pending", "label": "送信待ち", "source": "command", "pending": True}

    if cmd_id != 0 and ack_id == cmd_id and bool(ack.get("ok", True)):
        valve = int(cmd.get("valve", MANUAL_VALVE_OFF) or 0)
        return {
            "value": "open" if valve == MANUAL_VALVE_ON else "closed",
            "label": "開" if valve == MANUAL_VALVE_ON else "閉",
            "source": "estimated",
            "pending": False
        }

    if cmd_id != 0 and ack_id == cmd_id and not bool(ack.get("ok", True)):
        return {"value": "error", "label": "失敗", "source": "ack", "pending": False}

    return {"value": "unknown", "label": "不明", "source": "unknown", "pending": pending}


def schedule_tick_for_pin(pin_id: str):
    sch = get_schedule(pin_id)
    items = sch.get("items", [])
    if not items:
        return False

    if is_command_pending(pin_id):
        return False

    now_utc = int(time.time())
    prev = get_command(pin_id)

    indexed_items = []
    for it in items:
        start_ts = parse_start_local_to_ts(it.get("start_local") or "")
        indexed_items.append((start_ts, it))

    indexed_items.sort(key=lambda x: x[0] if x[0] > 0 else 9999999999)

    changed = False

    for start_ts, it in indexed_items:
        status = str(it.get("status", "scheduled"))
        minutes = it.get("minutes", 3)

        try:
            minutes = int(minutes)
        except Exception:
            minutes = 3

        minutes = max(1, min(180, minutes))
        if start_ts <= 0:
            continue

        end_ts = start_ts + minutes * 60

        if status == "scheduled" and start_ts <= now_utc < end_ts:
            new_cmd = {
                "cmd_id": int(prev.get("cmd_id", 0)) + 1,
                "type": "schedule_open",
                "valve": MANUAL_VALVE_ON,
                "duration_sec": minutes * 60,
                "threshold": None
            }
            set_command(pin_id, new_cmd)
            update_schedule_status(pin_id, it["id"], "opened", "opened_at")
            changed = True
            break

        if status == "opened" and now_utc >= end_ts:
            new_cmd = {
                "cmd_id": int(prev.get("cmd_id", 0)) + 1,
                "type": "schedule_close",
                "valve": MANUAL_VALVE_OFF,
                "duration_sec": 0,
                "threshold": None
            }
            set_command(pin_id, new_cmd)
            update_schedule_status(pin_id, it["id"], "done", "closed_at")
            changed = True
            break

    return changed


# =========================
# API: pins
# =========================
@app.route("/api/pins", methods=["GET", "POST"])
def api_pins():
    if request.method == "GET":
        return jsonify(get_all_pins())

    body = request.get_json(force=True) or {}

    name = (body.get("name") or "").strip()
    tpl = (body.get("template") or "A").strip().upper()
    note = (body.get("note") or "").strip()

    if tpl not in ["A", "B", "C"]:
        return jsonify({"error": "template must be A/B/C"}), 400

    lat, lon = convert_to_latlon(body)
    if not name or lat is None or lon is None:
        return jsonify({
            "error": "座標が入力されていません",
            "hint": "lat/lon または jgd2011_x/jgd2011_y/zone を送ってください（zone=1..19）"
        }), 400

    pid = allocate_id()

    upsert_pin(pid, name, tpl, lat, lon, note)
    ensure_default_records(pid)

    return jsonify({"pin_id": pid, "lat": float(lat), "lon": float(lon)})


@app.put("/api/pins/<pin_id>")
def api_update_pin(pin_id):
    pin = get_pin(pin_id)
    if not pin:
        return jsonify({"error": "not found"}), 404

    body = request.get_json(force=True) or {}
    name = (body.get("name") or pin.get("name", "")).strip()
    tpl = (body.get("template") or pin.get("template", "A")).strip().upper()
    note = (body.get("note") if body.get("note") is not None else pin.get("note", "")).strip()

    if not name:
        return jsonify({"error": "name required"}), 400
    if tpl not in ["A", "B", "C"]:
        return jsonify({"error": "template must be A/B/C"}), 400

    lat = pin["lat"]
    lon = pin["lon"]

    has_direct_latlon = (body.get("lat") not in [None, ""] and body.get("lon") not in [None, ""])
    has_jgd = (body.get("jgd2011_x") not in [None, ""] and body.get("jgd2011_y") not in [None, ""] and body.get("zone") not in [None, ""])

    if has_direct_latlon or has_jgd:
        new_lat, new_lon = convert_to_latlon(body)
        if new_lat is None or new_lon is None:
            return jsonify({"error": "invalid coordinates"}), 400
        lat, lon = new_lat, new_lon

    update_pin_fields(pin_id, name, tpl, note, lat=lat, lon=lon)
    return jsonify({"ok": True, "mode": tpl, "lat": lat, "lon": lon})


@app.post("/api/pins/<pin_id>/move")
def api_move_pin(pin_id):
    pin = get_pin(pin_id)
    if not pin:
        return jsonify({"error": "not found"}), 404

    body = request.get_json(force=True) or {}
    lat, lon = convert_to_latlon(body)

    if lat is None or lon is None:
        return jsonify({
            "error": "座標が不正です",
            "hint": "lat/lon または jgd2011_x/jgd2011_y/zone を送ってください"
        }), 400

    update_pin_fields(
        pin_id,
        pin["name"],
        pin["template"],
        pin.get("note", ""),
        lat=lat,
        lon=lon
    )
    return jsonify({"ok": True, "pin_id": pin_id, "lat": lat, "lon": lon})


@app.delete("/api/pins/<pin_id>")
def api_del_pin(pin_id):
    if not pin_exists(pin_id):
        return jsonify({"error": "not found"}), 404

    delete_pin_all(pin_id)
    release_id(pin_id)
    return jsonify({"ok": True})


# =========================
# telemetry & latest & logs
# =========================
@app.post("/api/telemetry/<pin_id>")
def api_telemetry(pin_id):
    if not pin_exists(pin_id):
        return jsonify({"error": "unknown pin_id"}), 404

    data = request.get_json(force=True) or {}
    ts = now_ts()

    upsert_latest_data(pin_id, ts, data)

    item = {
        "ts": ts,
        "soil": to_num(data.get("soil")),
        "rssi_up": to_num(data.get("rssi_up")),
        "rssi_down": to_num(data.get("rssi_down")),
        "noise": to_num(data.get("noise")),
    }
    if "valve_state" in data:
        item["valve_state"] = data.get("valve_state")
    if "valve_open" in data:
        item["valve_open"] = data.get("valve_open")

    add_log(pin_id, item)
    return jsonify({"ok": True})


@app.get("/api/latest/<pin_id>")
def api_latest(pin_id):
    return jsonify(get_latest(pin_id))


@app.get("/api/logs/<pin_id>")
def api_logs(pin_id):
    limit = request.args.get("limit", "800")
    try:
        limit = int(limit)
    except Exception:
        limit = 800

    limit = max(1, min(2000, limit))
    items = get_logs(pin_id, limit=limit)
    return jsonify({"items": items})


# =========================
# device command
# =========================
@app.get("/api/device/command/<pin_id>")
def api_device_get_command(pin_id):
    if not pin_exists(pin_id):
        return jsonify({"error": "unknown pin_id"}), 404

    if not device_token_ok(pin_id):
        return jsonify({"error": "unauthorized"}), 401

    schedule_tick_for_pin(pin_id)

    cmd = get_command(pin_id)
    if "type" not in cmd:
        cmd["type"] = "manual"

    ack = get_ack_info(pin_id)
    cmd_id = int(cmd.get("cmd_id", 0))
    ack_id = int(ack.get("cmd_id", 0))

    if cmd_id != 0 and cmd_id == ack_id:
        return jsonify({"pending": False, "cmd_id": ack_id, "ack": ack})

    if cmd_id == 0:
        return jsonify({"pending": False, "cmd_id": 0, "ack": ack})

    return jsonify({"pending": True, "cmd": cmd, "ack": ack})


@app.post("/api/device/ack/<pin_id>")
def api_device_ack(pin_id):
    if not pin_exists(pin_id):
        return jsonify({"error": "unknown pin_id"}), 404

    if not device_token_ok(pin_id):
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(force=True) or {}
    ts = now_ts()

    ack_obj = {
        "ts": ts,
        "cmd_id": int(body.get("cmd_id", 0)),
        "ok": bool(body.get("ok", True)),
        "detail": (body.get("detail") or "")
    }

    upsert_latest_ack(pin_id, ts, ack_obj)
    set_ack(
        pin_id,
        ack_obj["cmd_id"],
        ack_obj["ts"],
        ack_obj["ok"],
        ack_obj["detail"]
    )
    return jsonify({"ok": True})


@app.get("/api/device/schedule/<pin_id>")
def api_device_get_schedule(pin_id):
    if not pin_exists(pin_id):
        return jsonify({"error": "unknown pin_id"}), 404

    if not device_token_ok(pin_id):
        return jsonify({"error": "unauthorized"}), 401

    sch = get_schedule(pin_id)

    out_items = []
    for it in sch.get("items", []):
        out_items.append({
            "id": str(it.get("id", "")),
            "start_local": str(it.get("start_local", "")),
            "minutes": int(it.get("minutes", 0) or 0),
            "status": str(it.get("status", "scheduled"))
        })

    return jsonify({
        "pin_id": pin_id,
        "version": 1,
        "items": out_items
    })


# =========================
# UI command / schedule
# =========================
@app.get("/api/command/<pin_id>")
def api_get_command(pin_id):
    schedule_tick_for_pin(pin_id)

    cmd = get_command(pin_id)
    ack = get_ack_info(pin_id)

    out = dict(cmd)
    out["ack"] = ack
    out["pending"] = (
        int(out.get("cmd_id", 0)) != 0 and
        int(out.get("cmd_id", 0)) != int(ack.get("cmd_id", 0))
    )
    return jsonify(out)


@app.post("/api/command/<pin_id>")
def api_set_command(pin_id):
    if not pin_exists(pin_id):
        return jsonify({"error": "unknown pin_id"}), 404

    prev = get_command(pin_id)
    body = request.get_json(force=True) or {}
    new_cmd_id = int(prev.get("cmd_id", 0)) + 1
    cmd_type = (body.get("type") or prev.get("type") or "manual")

    cmd = {
        "cmd_id": new_cmd_id,
        "type": str(cmd_type),
        "valve": int(body.get("valve", prev.get("valve", MANUAL_VALVE_OFF))),
        "duration_sec": int(body.get("duration_sec", prev.get("duration_sec", 0))),
        "threshold": body.get("threshold", prev.get("threshold"))
    }
    set_command(pin_id, cmd)
    return jsonify({"ok": True, "cmd": cmd})


@app.get("/api/schedule/<pin_id>")
def api_get_schedule(pin_id):
    return jsonify({"items": get_schedule(pin_id).get("items", [])})


@app.post("/api/schedule/<pin_id>")
def api_add_schedule(pin_id):
    if not pin_exists(pin_id):
        return jsonify({"error": "unknown pin_id"}), 404

    body = request.get_json(force=True) or {}
    start_local = norm_start_local(body.get("start_local", ""))
    minutes = body.get("minutes", 3)

    try:
        minutes = int(minutes)
    except Exception:
        minutes = 3

    minutes = max(1, min(180, minutes))

    if not start_local:
        return jsonify({"error": "start_local required"}), 400

    item = add_schedule_item(pin_id, start_local, minutes)
    return jsonify({"ok": True, "item": item})


@app.delete("/api/schedule/<pin_id>/<sid>")
def api_del_schedule(pin_id, sid):
    ok = delete_schedule_item(pin_id, sid)
    if not ok:
        return jsonify({"error": "not found"}), 404

    return jsonify({"ok": True})


# =========================
# status / export / debug
# =========================
@app.get("/api/pin_status/<pin_id>")
def api_pin_status(pin_id):
    if not pin_exists(pin_id):
        return jsonify({"error": "unknown pin_id"}), 404

    schedule_tick_for_pin(pin_id)

    latest = get_latest(pin_id)
    cmd = get_command(pin_id)
    ack = get_ack_info(pin_id)
    valve_status = compute_valve_status(pin_id)

    return jsonify({
        "pin_id": pin_id,
        "pin": get_pin(pin_id),
        "latest": latest,
        "command": cmd,
        "ack": ack,
        "valve_status": valve_status
    })


@app.get("/api/export/<pin_id>.csv")
def api_export_csv(pin_id):
    if not pin_exists(pin_id):
        return jsonify({"error": "unknown pin_id"}), 404

    logs = get_logs(pin_id, limit=None)

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "pin_id",
        "timestamp_unix",
        "datetime_jst",
        "soil",
        "rssi_up",
        "rssi_down",
        "noise",
        "valve_state",
        "valve_open"
    ])

    for it in logs:
        ts = int(it.get("ts", 0) or 0)
        dt_jst = datetime.datetime.fromtimestamp(
            ts,
            tz=datetime.timezone(datetime.timedelta(hours=9))
        ).strftime("%Y-%m-%d %H:%M:%S") if ts else ""

        writer.writerow([
            pin_id,
            ts,
            dt_jst,
            it.get("soil", ""),
            it.get("rssi_up", ""),
            it.get("rssi_down", ""),
            it.get("noise", ""),
            it.get("valve_state", ""),
            it.get("valve_open", "")
        ])

    csv_data = output.getvalue()
    output.close()

    filename = f"pin_{pin_id}_logs.csv"
    return Response(
        "\ufeff" + csv_data,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@app.get("/api/gateway/<gateway_id>/pull")
def api_gateway_pull(gateway_id):
    token = (request.args.get("token") or "").strip()
    if not token:
        return jsonify({"error": "unauthorized"}), 401

    if DEVICE_TOKEN_GLOBAL:
        if token != DEVICE_TOKEN_GLOBAL:
            return jsonify({"error": "unauthorized"}), 401
    else:
        return jsonify({"error": "DEVICE_TOKEN_GLOBAL not set"}), 400

    pins_raw = (request.args.get("pins") or "").strip()
    if not pins_raw:
        return jsonify({"error": "pins required, e.g. pins=1,2,3"}), 400

    pin_ids = []
    for x in pins_raw.split(","):
        x = x.strip()
        if x:
            pin_ids.append(x)

    items = []

    for pin_id in pin_ids:
        if not pin_exists(pin_id):
            items.append({"pin_id": pin_id, "error": "unknown pin_id"})
            continue

        schedule_tick_for_pin(pin_id)

        cmd = get_command(pin_id)
        if "type" not in cmd:
            cmd["type"] = "manual"

        ack = get_ack_info(pin_id)
        cmd_id = int(cmd.get("cmd_id", 0))
        ack_id = int(ack.get("cmd_id", 0))

        if cmd_id != 0 and cmd_id == ack_id:
            items.append({"pin_id": pin_id, "pending": False, "cmd_id": ack_id, "ack": ack})
            continue

        if cmd_id == 0:
            items.append({"pin_id": pin_id, "pending": False, "cmd_id": 0, "ack": ack})
            continue

        items.append({"pin_id": pin_id, "pending": True, "cmd": cmd, "ack": ack})

    return jsonify({"gateway_id": gateway_id, "ts": now_ts(), "items": items})


@app.get("/api/debug/pin/<pin_id>")
def api_debug_pin(pin_id):
    if not pin_exists(pin_id):
        return jsonify({"error": "unknown pin_id"}), 404

    return jsonify({
        "pin_id": pin_id,
        "command": get_command(pin_id),
        "ack": get_ack_info(pin_id),
        "schedule": get_schedule(pin_id),
        "latest": get_latest(pin_id),
        "valve_status": compute_valve_status(pin_id)
    })


@app.get("/status")
def status():
    return "Flask server is running."


if __name__ == "__main__":
    init_db()
    migrate_json_to_sqlite_if_needed()
    app.run(host="0.0.0.0", port=5000, debug=False)