#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Datagate → NGP forwarder + OpenWeather + RAW XML→JSON recorder
Units policy:
- Canonical internal: m/s
- Tracsat: kph (hanya referensi)
- NGP payload: km/h (int, floor)
- /asset/{name}/weather: speed_calculation = knots (float, 3 desimal, dari LAST_POS.speed_kmh)

Fitur:
- Baca <Telemetry><Speed units="...">, normalisasi ke m/s, turunkan ke km/h & knots
- Rekam event: JSONL harian + snapshot per-asset
- Cache last position berisi kmh (int floor) + knots (floor 0.1) untuk kompatibilitas
- /weather mengambil km/h terakhir dari LAST_POS, lalu konversi → knots (3 desimal) sebagai speed_calculation
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
NGP_URL = "http://ngp-tracker.eu.navixy.com:80"   # endpoint NGP
TIMEOUT_S = 10

# Fallback jika atribut units tidak ada di XML
# Pilih: "kmh" | "mph" | "knots" | "mps"
SPEED_UNIT_FALLBACK = "kmh"

DATA_DIR = Path("data")
EVENTS_DIR = DATA_DIR / "events"           # JSONL harian
RAW_BY_ASSET_DIR = DATA_DIR / "raw_by_asset"
for p in (DATA_DIR, EVENTS_DIR, RAW_BY_ASSET_DIR):
    p.mkdir(parents=True, exist_ok=True)

MAPPINGS_FILE = DATA_DIR / "name_to_imei.json"
LAST_POS_FILE = DATA_DIR / "last_pos.json"

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

app = FastAPI(title="Datagate→NGP + Weather + RAW recorder (kmh for NGP, knots for weather)")
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
    """Convert incoming speed to meters per second."""
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
    return v / 3.6  # default assume km/h

def mps_to_kmh(v_mps: Optional[float]) -> Optional[float]:
    return None if v_mps is None else (v_mps * 3.6)

def mps_to_knots(v_mps: Optional[float]) -> Optional[float]:
    return None if v_mps is None else (v_mps * 1.943844)

def strip_ns(root: ET.Element) -> ET.Element:
    for e in root.iter():
        if "}" in e.tag:
            e.tag = e.tag.split("}", 1)[1]
    return root

def element_to_dict(el: ET.Element) -> Any:
    """Konversi element XML (AssetEvent) → dict (rekursif)."""
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
    """
    Output /weather:
    - speed_calculation = knots (float, 3 desimal)
    - tidak expose speed_kmh/speed_knots
    """
    out: Dict[str, Any] = {}
    for k in ("asset_name", "asset_model", "asset_mfg_year", "asset_registration","site_id", "group_id", "image", "battery", "charging_status","heading_calculation", "speed_calculation", "temperature", "humidity", "battery_voltage", "timestamp"):
        if k in asset_meta and asset_meta[k] is not None:
            out[k] = asset_meta[k]
    out.setdefault("timestamp", now_utc_iso())
    out["latitude"] = round6(lat)
    out["longitude"] = round6(lon)
    out["coord"] = {"lon": float(f"{lon:.4f}"), "lat": float(f"{lat:.4f}")}

    for key in ("weather", "base", "main", "visibility", "wind", "clouds", "dt", "sys", "timezone", "id", "name", "cod"):
        if key in weather_raw:
            out[key] = weather_raw[key]

    # Optional: expose kecepatan angin dari OpenWeather (m/s)
    out["wind_speed_ms"] = weather_raw.get("wind", {}).get("speed", None)
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

        # --- Speed & heading ---
        sp_el = ev.find("Telemetry/Speed")
        sp_raw = to_float(sp_el.text if (sp_el is not None and sp_el.text) else None)
        sp_unit_attr = sp_el.get("units") if sp_el is not None else None
        sp_unit_in = normalize_unit(sp_unit_attr or SPEED_UNIT_FALLBACK)

        heading = to_float(get("Telemetry/Heading"))

        # --- extras ---
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

        # --- derive via canonical m/s ---
        v_mps = any_to_mps(sp_raw, sp_unit_in) if sp_raw is not None else None
        kmh_val = mps_to_kmh(v_mps)    # float
        knots_val = mps_to_knots(v_mps)  # float

        # policies:
        kmh_int_floor = int(math.floor(kmh_val)) if kmh_val is not None else None   # for NGP & cache
        knots_1dp_floor = (math.floor(knots_val * 10) / 10.0) if knots_val is not None else None  # legacy cache

        heading_deg = int(round(heading)) % 360 if heading is not None else None
        key_norm = norm_name(name) or name.strip().casefold()

        # --- record RAW JSON ---
        raw_dict = element_to_dict(ev)
        day_file = EVENTS_DIR / (datetime.utcnow().strftime("%Y-%m-%d") + ".jsonl")
        record = {
            "ts_ingest": now_utc_iso(),
            "asset_name": name,
            "imei": None,  # diisi setelah mapping ditemukan
            "message_time": rx_time or gmt_time,
            "gps_valid": bool(gps_valid),
            "speed": {
                "input": {"value": sp_raw, "units": sp_unit_attr or SPEED_UNIT_FALLBACK},
                "mps": v_mps,
                "kmh_floor": kmh_int_floor,
                "knots_floor_1dp": knots_1dp_floor
            },
            "location": {
                "lat": lat, "lon": lon, "alt": alt, "sats": sats,
                "heading": heading_deg
            },
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
            snap_path = None

        # --- update cache last position (store both) ---
        if lat is not None and lon is not None:
            LAST_POS[key_norm] = {
                "asset_name": name,
                "lat": lat,
                "lon": lon,
                "rx_time": rx_time or gmt_time or now_utc_iso(),
                "gps_valid": bool(gps_valid),
                # Sumber kebenaran untuk /weather (kph int)
                "speed_kmh": float(kmh_int_floor) if kmh_int_floor is not None else None,
                # Legacy simpan knots 0.1dp (tidak dipakai weather, hanya fallback)
                "speed_knots": float(knots_1dp_floor) if knots_1dp_floor is not None else None,
                "heading_calculation": heading_deg,
                "raw_snapshot_path": str(snap_path) if snap_path else None,
            }
            _save_last_pos()

        # --- forward ke NGP (km/h int floor) jika ada mapping ---
        imei = None
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

        # tulis update IMEI kecil ke JSONL
        record["imei"] = imei
        try:
            with open(day_file, "a", encoding="utf-8") as f:
                f.write(json.dumps({"_update": "imei", "asset_name": name, "imei": imei, "ts": now_utc_iso()}, ensure_ascii=False) + "\n")
        except Exception:
            pass

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
        if kmh_int_floor is not None:
            location["speed"] = kmh_int_floor   # <-- NGP expects km/h
        if heading_deg is not None:
            location["heading"] = heading_deg

        payload: Dict[str, Any] = {
            "device_id": imei,
            "message_time": rx_time or now_utc_iso(),
            "location": location,
        }
        if battery_level is not None:
            payload["battery_level"] = battery_level

        try:
            log.info("[NGP-PAYLOAD] %s", json.dumps(payload)[:800])
        except Exception:
            pass

        ok, info = post_ngp(payload)
        log.info("[FORWARD] %s | name=%s | imei=%s | src_unit=%s | kmh=%s | knots=%.1f | info=%s",
                 "OK" if ok else "FAIL", name, imei, sp_unit_in,
                 str(kmh_int_floor) if kmh_int_floor is not None else "None",
                 knots_1dp_floor if knots_1dp_floor is not None else -1.0, info)
        if ok:
            sent += 1

    return Response(content=f"OK ({sent})", media_type="text/plain")

# ---------- Weather endpoint (return speed_calculation = LAST_POS.speed_knots, no conversion) ----------
@app.get("/asset/{asset_name}/weather")
def asset_last_weather(asset_name: str):
    key = norm_name(asset_name)
    rec = LAST_POS.get(key)
    if not rec:
        raise HTTPException(status_code=404, detail=f"no last position for asset '{asset_name}'")

    lat, lon = float(rec["lat"]), float(rec["lon"])

    # Ambil langsung dari cache (tanpa konversi dari km/h)
    # Jika tidak ada (record lama), default 0.0
    speed_knots = rec.get("speed_knots")
    try:
        # pastikan float; biarkan presisi apa adanya (mis. 11.2)
        speed_calc_knots = float(speed_knots) if speed_knots is not None else 0.0
    except Exception:
        speed_calc_knots = 0.0

    asset_meta = {
        "asset_name": rec.get("asset_name"),
        "timestamp": rec.get("rx_time"),
        "heading_calculation": rec.get("heading_calculation"),
        # Hanya expose speed_calculation (knots), taken as-is from LAST_POS
        "speed_calculation": speed_calc_knots,
    }

    wx = fetch_openweather(lat, lon)
    return build_weather_output(asset_meta, lat, lon, wx)


# ---------- Debug endpoint ----------
@app.get("/asset/{asset_name}/debug")
def asset_debug(asset_name: str):
    key = (asset_name or "").strip().casefold()
    rec = LAST_POS.get(key)
    if not rec:
        raise HTTPException(status_code=404, detail="no last position")
    raw_path = rec.get("raw_snapshot_path")
    raw = None
    if raw_path and os.path.exists(raw_path):
        try:
            with open(raw_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except Exception:
            raw = {"error": "failed to read snapshot"}
    return JSONResponse({
        "asset_name": rec.get("asset_name"),
        "rx_time": rec.get("rx_time"),
        "speed_kmh_cached": rec.get("speed_kmh"),
        "speed_knots_cached": rec.get("speed_knots"),
        "heading_deg": rec.get("heading_calculation"),
        "gps_valid": rec.get("gps_valid"),
        "raw_snapshot_path": raw_path,
        "input_speed_from_snapshot": (raw or {}).get("speed", {}).get("input"),
        "calc_from_snapshot": {
            "mps": (raw or {}).get("speed", {}).get("mps"),
            "kmh_floor": (raw or {}).get("speed", {}).get("kmh_floor"),
            "knots_floor_1dp": (raw or {}).get("speed", {}).get("knots_floor_1dp"),
        },
        "location_from_snapshot": (raw or {}).get("location"),
    })

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
    return {"ok": True}
