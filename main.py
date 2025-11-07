#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Datagate → NGP forwarder (lean, satellites default)
- Canonical internal: m/s
- NGP expects: km/h (int, floor)
- Endpoint default: http://ngp-tracker.eu.navixy.com:80  (override via NGP_URL)
- Optional auth header via NGP_AUTH_HEADER (e.g. "Authorization: Bearer xxx" atau "X-Api-Key: token")
"""

from fastapi import FastAPI, Request, Response, HTTPException, Depends, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import xml.etree.ElementTree as ET
import requests, json, os, threading, logging, math
from typing import Dict, Optional, Tuple, Any
from datetime import datetime, timezone as tz
from pathlib import Path

# ================== CONFIG ==================
NGP_URL = os.getenv("NGP_URL", "http://ngp-tracker.eu.navixy.com:80")
NGP_AUTH_HEADER = os.getenv("NGP_AUTH_HEADER", "")      # contoh: "Authorization: Bearer <token>" atau "X-Api-Key: <token>"
NGP_TIMEOUT_S = int(os.getenv("NGP_TIMEOUT_S", "10"))

# Default satellites jika tak tersedia pada XML (bisa override via env)
SATELLITES_DEFAULT = int(os.getenv("SATELLITES_DEFAULT", "8"))

# Fallback jika units speed tak ada di XML
SPEED_UNIT_FALLBACK = "kmh"

DATA_DIR = Path("data")
EVENTS_DIR = DATA_DIR / "events"           # JSONL harian
RAW_BY_ASSET_DIR = DATA_DIR / "raw_by_asset"
for p in (DATA_DIR, EVENTS_DIR, RAW_BY_ASSET_DIR):
    p.mkdir(parents=True, exist_ok=True)

MAPPINGS_FILE = DATA_DIR / "name_to_imei.json"
LAST_POS_FILE = DATA_DIR / "last_pos.json"

DEFAULT_MAPPINGS: Dict[str, str] = {
    # "Sinar Mataram": "010006429582",
}

# ================== ADMIN TOKEN ==================
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "secret123")

# ================== LOGGING ==================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("datagate-ngp")

app = FastAPI(title="Datagate→NGP forwarder (km/h int)")

_lock = threading.Lock()
NAME_TO_IMEI: Dict[str, str] = {}
LAST_POS: Dict[str, Dict[str, Any]] = {}

# ---------- Utils ----------
def now_utc_iso() -> str:
    return datetime.utcnow().replace(tzinfo=tz.utc).isoformat().replace("+00:00", "Z")

def norm_name(s: Optional[str]) -> Optional[str]:
    return s.strip().casefold() if s else None

def to_float(txt: Optional[str]) -> Optional[float]:
    if txt is None:
        return None
    try:
        return float(txt)
    except Exception:
        return None

def normalize_unit(unit: Optional[str]) -> str:
    if not unit:
        return "kmh"
    u = unit.strip().lower()
    if u in {"kmh", "kph", "km/h"}: return "kmh"
    if u in {"mph", "mi/h"}:        return "mph"
    if u in {"knots", "knot", "kt", "kts"}: return "knots"
    if u in {"m/s", "mps", "ms"}:   return "mps"
    return "kmh"

# ---- conversions with canonical m/s ----
def any_to_mps(v: Optional[float], unit: str) -> Optional[float]:
    if v is None:
        return None
    u = normalize_unit(unit)
    if u == "kmh":
        return v / 3.6
    if u == "mph":
        return v * 0.44704
    if u == "knots":
        return v * 0.514444
    if u == "mps":
        return v
    return v / 3.6  # default km/h

def mps_to_kmh_int_floor(v_mps: Optional[float]) -> Optional[int]:
    if v_mps is None:
        return None
    return int(math.floor(v_mps * 3.6))

def strip_ns(root: ET.Element) -> ET.Element:
    for e in root.iter():
        if "}" in e.tag:
            e.tag = e.tag.split("}", 1)[1]
    return root

def element_to_dict(el: ET.Element) -> Any:
    node: Dict[str, Any] = {}
    if el.attrib:
        node["@attr"] = {k: v for k, v in el.attrib.items()}
    children = list(el)
    if children:
        d: Dict[str, Any] = {}
        for c in children:
            key = c.tag
            val = element_to_dict(c)
            if key in d:
                if not isinstance(d[key], list):
                    d[key] = [d[key]]
                d[key].append(val)
            else:
                d[key] = val
        if el.text and el.text.strip():
            node["#text"] = el.text.strip()
        node.update(d)
    else:
        text = (el.text or "").strip()
        node = text
        if el.attrib:
            node = {"@attr": {k: v for k, v in el.attrib.items()}, "#text": text}
    return node

def round6(v: Optional[float]) -> Optional[float]:
    return float(f"{v:.6f}") if (v is not None and not math.isnan(v)) else None

# ---------- Persistence ----------
def save_mappings(m: Dict[str, str]) -> None:
    with _lock:
        with open(MAPPINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(m, f, indent=2, ensure_ascii=False)

def load_mappings() -> Dict[str, str]:
    if MAPPINGS_FILE.exists():
        try:
            with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception as e:
            log.warning("failed to load mappings file: %s", e)
    save_mappings(dict(DEFAULT_MAPPINGS))
    return dict(DEFAULT_MAPPINGS)

def _save_last_pos() -> None:
    with _lock:
        try:
            with open(LAST_POS_FILE, "w", encoding="utf-8") as f:
                json.dump(LAST_POS, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("failed to save last_pos: %s", e)

def _load_last_pos() -> None:
    global LAST_POS
    if LAST_POS_FILE.exists():
        try:
            with open(LAST_POS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    LAST_POS = data
        except Exception as e:
            log.warning("failed to load last_pos: %s", e)

NAME_TO_IMEI = load_mappings()

# ---------- Auth ----------
def require_admin(x_admin_token: Optional[str] = Header(default=None, alias="X-Admin-Token")):
    if x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid X-Admin-Token")

# ---------- NGP push ----------
def _split_header(line: str):
    if not line or ":" not in line:
        return None, None
    k, v = line.split(":", 1)
    return k.strip(), v.strip()

def post_ngp(payload: dict) -> Tuple[bool, str]:
    headers = {"Content-Type": "application/json"}
    k, v = _split_header(NGP_AUTH_HEADER)
    if k and v:
        headers[k] = v
    try:
        r = requests.post(NGP_URL, json=payload, headers=headers, timeout=NGP_TIMEOUT_S)
        body = (r.text or "")[:1000].replace("\n", " ")
        log.info("[NGP] url=%s code=%s body=%s", NGP_URL, r.status_code, body)
        r.raise_for_status()
        return True, f"{r.status_code}"
    except requests.exceptions.HTTPError as he:
        status = getattr(he.response, "status_code", None)
        text = (getattr(he.response, "text", "") or "")[:200]
        return False, f"HTTP {status} {text}"
    except Exception as e:
        return False, f"EXC {type(e).__name__}: {e}"

def build_ngp_payload(
    imei: str,
    rx_time: Optional[str],
    gmt_time: Optional[str],
    gps_valid: bool,
    lat: float,
    lon: float,
    alt: Optional[float],
    speed_kmh_int: Optional[int],
    heading_deg: Optional[int],
    sats: Optional[int],
    battery_level: Optional[int],
) -> dict:
    msg_time = rx_time or now_utc_iso()
    gnss_time = gmt_time or rx_time or now_utc_iso()
    return {
        "message_time": msg_time,
        "device_id": imei,
        "location": {
            "gnss_time": gnss_time,
            "fix_type": "HAS_FIX" if gps_valid else "NO_FIX",
            "satellites": sats if sats is not None else SATELLITES_DEFAULT,  # default 8
            "latitude": round6(lat),
            "longitude": round6(lon),
            "altitude": alt if alt is not None else 0,
            "speed": speed_kmh_int if speed_kmh_int is not None else 0,
            "heading": heading_deg if heading_deg is not None else 0,
        },
        "battery_level": battery_level if battery_level is not None else 0,
    }

# ---------- App lifecycle ----------
@app.on_event("startup")
async def on_start():
    _load_last_pos()
    log.info("Mappings loaded: %s", NAME_TO_IMEI)
    log.info("NGP_URL=%s NGP_TIMEOUT_S=%s AuthHeader=%s",
             NGP_URL, NGP_TIMEOUT_S, (NGP_AUTH_HEADER.split(':',1)[0] if NGP_AUTH_HEADER else "(none)"))

# ---------- Ingest from Datagate ----------
@app.post("/")
async def receive_datagate(request: Request):
    raw = await request.body()
    body = raw.decode("utf-8", errors="ignore")
    log.info("POST / len=%d", len(body))

    sent = 0
    try:
        root = strip_ns(ET.fromstring(body))
    except ET.ParseError as e:
        log.error("XML parse error: %s", e)
        return Response(content="BAD XML", media_type="text/plain", status_code=200)

    events = root.findall(".//AssetEvent")
    for ev in events:
        def get(p: str) -> Optional[str]:
            x = ev.find(p)
            return x.text.strip() if (x is not None and x.text) else None

        name = get("AssetDescription")
        rx_time = get("RXTime")
        gmt_time = get("GMTTime")
        gps_valid = (get("GPS/Valid") or "").lower() == "true"
        lat = to_float(get("GPS/Latitude"))
        lon = to_float(get("GPS/Longitude"))

        sp_el = ev.find("Telemetry/Speed")
        sp_raw = to_float(sp_el.text if (sp_el is not None and sp_el.text) else None)
        sp_unit_in = normalize_unit(sp_el.get("units") if sp_el is not None else SPEED_UNIT_FALLBACK)
        v_mps = any_to_mps(sp_raw, sp_unit_in) if sp_raw is not None else None
        kmh_int = mps_to_kmh_int_floor(v_mps)

        heading = to_float(get("Telemetry/Heading"))
        heading_deg = int(round(heading)) % 360 if heading is not None else None

        sats_txt = get("GPS/Satellites") or get("Telemetry/Satellites")
        try:
            sats = int(float(sats_txt)) if sats_txt is not None else None
        except Exception:
            sats = None

        alt = to_float(get("GPS/Altitude") or get("Telemetry/Altitude"))
        battery_txt = get("Telemetry/BatteryLevel") or get("Telemetry/Battery") or get("Telemetry/ExtBattery")
        try:
            battery_level = int(float(battery_txt)) if battery_txt is not None else None
        except Exception:
            battery_level = None

        if not name:
            log.warning("[SKIP] missing AssetDescription")
            continue

        key_norm = norm_name(name) or name.strip().casefold()

        # record RAW JSON
        raw_dict = element_to_dict(ev)
        day_file = EVENTS_DIR / (datetime.utcnow().strftime("%Y-%m-%d") + ".jsonl")
        record = {
            "ts_ingest": now_utc_iso(),
            "asset_name": name,
            "imei": None,
            "message_time": rx_time or gmt_time,
            "gps_valid": bool(gps_valid),
            "speed": {
                "input": {"value": sp_raw, "units": sp_unit_in},
                "kmh_floor": kmh_int,
            },
            "location": {"lat": lat, "lon": lon, "alt": alt, "sats": sats, "heading": heading_deg},
            "raw": raw_dict,
        }
        try:
            with open(day_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as e:
            log.warning("failed to write daily jsonl: %s", e)

        # snapshot per-asset
        snap_path = None
        try:
            snap_path = RAW_BY_ASSET_DIR / (key_norm.replace(" ", "_") + ".json")
            with open(snap_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.warning("failed to write per-asset snapshot: %s", e)

        # update cache last position
        if lat is not None and lon is not None:
            LAST_POS[key_norm] = {
                "asset_name": name,
                "lat": lat,
                "lon": lon,
                "rx_time": rx_time or gmt_time or now_utc_iso(),
                "gps_valid": bool(gps_valid),
                "sats": sats if sats is not None else SATELLITES_DEFAULT,
                "speed_kmh": float(kmh_int) if kmh_int is not None else None,
                "heading": heading_deg,
                "raw_snapshot_path": str(snap_path) if snap_path else None,
            }
            _save_last_pos()

        # forward ke NGP
        imei = None
        for display, val in NAME_TO_IMEI.items():
            if norm_name(display) == key_norm:
                imei = val.strip()
                break

        if not imei:
            log.warning("[SKIP FORWARD] no mapping for '%s' (norm='%s')", name, key_norm)
            continue
        if lat is None or lon is None:
            log.warning("[SKIP FORWARD] missing lat/lon for '%s' (raw_gps=%s)", name, raw_dict.get("GPS"))
            continue

        # tulis update IMEI ke JSONL
        try:
            with open(day_file, "a", encoding="utf-8") as f:
                f.write(json.dumps({"_update": "imei", "asset_name": name, "imei": imei, "ts": now_utc_iso()}, ensure_ascii=False) + "\n")
        except Exception:
            pass

        payload = build_ngp_payload(
            imei=imei,
            rx_time=rx_time,
            gmt_time=gmt_time,
            gps_valid=gps_valid,
            lat=lat,
            lon=lon,
            alt=alt,
            speed_kmh_int=kmh_int,
            heading_deg=heading_deg,
            sats=sats,
            battery_level=battery_level
        )

        try:
            log.info("[NGP-PAYLOAD] %s", json.dumps(payload)[:900])
        except Exception:
            pass

        ok, info = post_ngp(payload)
        log.info("[FORWARD] %s | name=%s | imei=%s | src_unit=%s | kmh=%s | sats=%s | info=%s",
                 "OK" if ok else "FAIL", name, imei, sp_unit_in,
                 str(kmh_int) if kmh_int is not None else "None",
                 (str(sats) if sats is not None else f"default:{SATELLITES_DEFAULT}"),
                 info)
        if ok:
            sent += 1

    return Response(content=f"OK ({sent})", media_type="text/plain")

# ---------- Test push (static payload) ----------
@app.post("/_test_push")
def test_push():
    sample = {
        "message_time": "2025-10-17T08:39:11Z",
        "device_id": "010006429582",
        "location": {
            "gnss_time":"2025-10-17T08:39:10Z",
            "fix_type": "HAS_FIX",
            "satellites": SATELLITES_DEFAULT,
            "latitude": -6.26676,
            "longitude": 106.85065,
            "altitude": 271,
            "speed": 43,
            "heading": 77
        },
        "battery_level": 71
    }
    ok, info = post_ngp(sample)
    return {"ok": ok, "info": info, "url": NGP_URL, "sat_default": SATELLITES_DEFAULT}

# ---------- Admin / misc ----------
@app.get("/lastpos")
def list_last_positions():
    return LAST_POS

@app.get("/mappings")
def list_mappings():
    return NAME_TO_IMEI

class MappingIn(BaseModel):
    name: str
    imei: str

@app.post("/mappings", status_code=201)
def add_mapping(m: MappingIn, admin: None = Depends(require_admin)):
    NAME_TO_IMEI[m.name] = m.imei
    with _lock:
        with open(MAPPINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(NAME_TO_IMEI, f, indent=2, ensure_ascii=False)
    return {"ok": True, "mappings": NAME_TO_IMEI}

@app.get("/health")
def health():
    return {"ok": True, "ngp_url": NGP_URL, "sat_default": SATELLITES_DEFAULT}
