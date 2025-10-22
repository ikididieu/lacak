#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Datagate → NGP forwarder + OpenWeather enrichment
- Match unit by AssetDescription → IMEI (mappings JSON)
- Cache last position per asset (JSON)
- /asset/{asset_name}/weather: balikin blok OpenWeather + meta terakhir
- Speed dikonversi ke km/h sesuai SPEED_UNIT: "kmh" | "mph" | "knots"
- Heading dipastikan 0–359°

CATATAN:
- ADMIN_TOKEN & OPENWEATHER_API_KEY diset di bawah untuk keperluan internal.
"""

from fastapi import FastAPI, Request, Response, HTTPException, Depends, Header
from pydantic import BaseModel
import xml.etree.ElementTree as ET
import requests, json, os, threading, logging, math
from typing import Dict, Optional, Tuple, Any
from datetime import datetime, timezone as tz
import math
# ================== CONFIG ==================
NGP_URL = "http://ngp-tracker.eu.navixy.com:80"   # endpoint NGP
TIMEOUT_S = 10

# Sumber data speed dari payload (Telemetry/Speed) pakai unit apa?
# Pilih: "kmh" | "mph" | "knots"
SPEED_UNIT = "kmh"

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

MAPPINGS_FILE = os.path.join(DATA_DIR, "name_to_imei.json")
LAST_POS_FILE = os.path.join(DATA_DIR, "last_pos.json")

DEFAULT_MAPPINGS = {
    # "Sinar Mataram": "8140018210000",
}

# ================== FIXED CREDENTIALS ==================
OPENWEATHER_API_KEY = "f1c81dd3fc3e2d98c23a4b81b515604f"  # fixed key
ADMIN_TOKEN = "secret123"                                  # fixed admin token
# ========================================================

OPENWEATHER_BASE = "https://api.openweathermap.org/data/2.5/weather"
OPENWEATHER_TIMEOUT_S = 10

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("datagate-ngp")

app = FastAPI(title="Datagate→NGP + Weather (match by AssetDescription)")
_lock = threading.Lock()
NAME_TO_IMEI: Dict[str, str] = {}
LAST_POS: Dict[str, Dict[str, Any]] = {}

# ---------- Utils ----------
def norm_name(s: Optional[str]) -> Optional[str]:
    return s.strip().casefold() if s else None

def to_float(txt: Optional[str]) -> Optional[float]:
    if txt is None:
        return None
    try:
        return float(txt)
    except Exception:
        return None

def convert_speed(v: Optional[float]) -> Optional[int]:
    """Konversi ke km/h dan hasilnya dibulatkan ke bawah (floor)."""
    if v is None:
        return None
    if SPEED_UNIT == "kmh":
        return int(math.floor(v))
    if SPEED_UNIT == "mph":
        return int(math.floor(v * 1.609344))
    if SPEED_UNIT == "knots":
        return int(math.floor(v * 1.852))
    return int(math.floor(v))

def strip_ns(root: ET.Element) -> ET.Element:
    for e in root.iter():
        if "}" in e.tag:
            e.tag = e.tag.split("}", 1)[1]
    return root

def round6(v: Optional[float]) -> Optional[float]:
    return float(f"{v:.6f}") if (v is not None and not math.isnan(v)) else None

# ---------- Persistence ----------
def save_mappings(m: Dict[str, str]) -> None:
    with _lock:
        with open(MAPPINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(m, f, indent=2, ensure_ascii=False)

def load_mappings() -> Dict[str, str]:
    if os.path.exists(MAPPINGS_FILE):
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
    if os.path.exists(LAST_POS_FILE):
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
def post_ngp(payload: dict) -> Tuple[bool, str]:
    try:
        r = requests.post(NGP_URL, json=payload, headers={"Content-Type": "application/json"}, timeout=TIMEOUT_S)
        log.info("[NGP] %s %s", r.status_code, r.text[:160].replace("\n", " "))
        r.raise_for_status()
        return True, f"{r.status_code}"
    except requests.exceptions.HTTPError as he:
        return False, f"HTTPError {he}"
    except Exception as e:
        return False, str(e)

# ---------- OpenWeather ----------
def fetch_openweather(lat: float, lon: float) -> dict:
    try:
        r = requests.get(
            OPENWEATHER_BASE,
            params={"lat": lat, "lon": lon, "appid": OPENWEATHER_API_KEY, "units": "metric"},
            timeout=OPENWEATHER_TIMEOUT_S,
        )
        log.info("[OPENWEATHER] %s %s", r.status_code, r.text[:160].replace("\n", " "))
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenWeather failed: {e}")

def build_weather_output(asset_meta: Dict[str, Any], lat: float, lon: float, weather_raw: dict) -> dict:
    out: Dict[str, Any] = {}
    for k in ("asset_name", "asset_model", "asset_mfg_year", "asset_registration",
              "site_id", "group_id", "image", "battery", "charging_status",
              "heading_calculation", "speed_calculation", "temperature", "humidity",
              "battery_voltage", "timestamp"):
        if k in asset_meta and asset_meta[k] is not None:
            out[k] = asset_meta[k]
    out.setdefault("timestamp", datetime.utcnow().replace(tzinfo=tz.utc).isoformat().replace("+00:00", "Z"))
    out["latitude"] = round6(lat)
    out["longitude"] = round6(lon)
    out["coord"] = {"lon": float(f"{lon:.4f}"), "lat": float(f"{lat:.4f}")}
    for key in ("weather", "base", "main", "visibility", "wind", "clouds", "dt", "sys", "timezone", "id", "name", "cod"):
        if key in weather_raw:
            out[key] = weather_raw[key]
    return out

class MappingIn(BaseModel):
    name: str
    imei: str

# ---------- App lifecycle ----------
@app.on_event("startup")
async def on_start():
    _load_last_pos()
    log.info("Mappings loaded: %s", NAME_TO_IMEI)

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
        sp_raw = to_float(get("Telemetry/Speed"))
        heading = to_float(get("Telemetry/Heading"))

        # extras
        sats_txt = get("GPS/Satellites") or get("Telemetry/Satellites")
        alt_txt = get("GPS/Altitude") or get("Telemetry/Altitude")
        battery_txt = get("Telemetry/BatteryLevel") or get("Telemetry/Battery") or get("Telemetry/ExtBattery")

        sats = None
        try:
            sats = int(sats_txt) if sats_txt is not None else None
        except Exception:
            sats = None
        alt = to_float(alt_txt)
        battery_level = None
        try:
            battery_level = int(float(battery_txt)) if battery_txt is not None else None
        except Exception:
            battery_level = None

        if not name:
            log.warning("[SKIP] missing AssetDescription")
            continue

        # update cache last position
        if lat is not None and lon is not None:
            key = norm_name(name) or name.strip().casefold()
            kph = convert_speed(sp_raw) if sp_raw is not None else None
            heading_deg: Optional[int] = None
            if heading is not None:
                try:
                    heading_deg = int(round(heading)) % 360
                except Exception:
                    heading_deg = None

            LAST_POS[key] = {
                "asset_name": name,
                "lat": lat,
                "lon": lon,
                "rx_time": rx_time or gmt_time or datetime.utcnow().replace(tzinfo=tz.utc).isoformat().replace("+00:00", "Z"),
                "gps_valid": bool(gps_valid),
                "speed_calculation": float(kph) if kph is not None else None,
                "heading_calculation": heading_deg,
            }
            _save_last_pos()

        # forward ke NGP jika ada mapping
        imei = None
        key_norm = norm_name(name)
        for display, val in NAME_TO_IMEI.items():
            if norm_name(display) == key_norm:
                imei = val.strip()
                break
        if not imei:
            log.warning("[SKIP FORWARD] no mapping for AssetDescription='%s'", name)
            continue
        if lat is None or lon is None:
            log.warning("[SKIP FORWARD] missing lat/lon for '%s'", name)
            continue

        kph_val = convert_speed(sp_raw)
        heading_deg2 = None
        if heading is not None:
            try:
                heading_deg2 = int(round(heading)) % 360
            except Exception:
                heading_deg2 = None

        location: Dict[str, Any] = {
            "gnss_time": gmt_time or rx_time,
            "fix_type": "HAS_FIX" if gps_valid else "NO_FIX",
            "latitude": lat,
            "longitude": lon,
        }
        if sats is not None:
            location["satellites"] = sats
        if alt is not None:
            location["altitude"] = alt
        if kph_val is not None:
            location["speed"] = kph_val
        if heading_deg2 is not None:
            location["heading"] = heading_deg2

        payload: Dict[str, Any] = {
            "device_id": imei,
            "message_time": rx_time or datetime.utcnow().replace(tzinfo=tz.utc).isoformat().replace("+00:00", "Z"),
            "location": location,
        }
        if battery_level is not None:
            payload["battery_level"] = battery_level

        try:
            log.info("[NGP-PAYLOAD] %s", json.dumps(payload)[:800])
        except Exception:
            pass

        ok, info = post_ngp(payload)
        log.info("[FORWARD] %s | name=%s | imei=%s | info=%s",
                 "OK" if ok else "FAIL", name, imei, info)
        if ok:
            sent += 1

    return Response(content=f"OK ({sent})", media_type="text/plain")

# ---------- Weather endpoint ----------
@app.get("/asset/{asset_name}/weather")
def asset_last_weather(asset_name: str):
    key = norm_name(asset_name)
    rec = LAST_POS.get(key)
    if not rec:
        raise HTTPException(status_code=404, detail=f"no last position for asset '{asset_name}'")
    lat, lon = float(rec["lat"]), float(rec["lon"])
    asset_meta = {
        "asset_name": rec.get("asset_name"),
        "timestamp": rec.get("rx_time"),
        "speed_calculation": rec.get("speed_calculation"),
        "heading_calculation": rec.get("heading_calculation"),
    }
    wx = fetch_openweather(lat, lon)
    return build_weather_output(asset_meta, lat, lon, wx)

# ---------- Admin / misc ----------
@app.get("/lastpos")
def list_last_positions():
    return LAST_POS

@app.get("/mappings")
def list_mappings():
    return NAME_TO_IMEI

@app.post("/mappings", status_code=201)
def add_mapping(m: MappingIn, admin: None = Depends(require_admin)):
    NAME_TO_IMEI[m.name] = m.imei
    save_mappings(NAME_TO_IMEI)
    return {"ok": True, "mappings": NAME_TO_IMEI}

@app.get("/health")
def health():
    return {"ok": True}
