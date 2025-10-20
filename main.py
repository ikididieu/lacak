from fastapi import FastAPI, Request, Response, HTTPException, Depends, Header, Path
from pydantic import BaseModel
import xml.etree.ElementTree as ET
import requests, json, os, threading, logging, math
from typing import Dict, Optional, Tuple, Any
from datetime import datetime, timezone as tz

# ================== CONFIG ==================
NGP_URL = "http://ngp-tracker.eu.navixy.com:80"   # endpoint NGP
TIMEOUT_S = 10

# Folder penyimpanan data (otomatis dibuat kalau belum ada)
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# File JSON untuk mapping & last position
MAPPINGS_FILE = os.path.join(DATA_DIR, "name_to_imei.json")
LAST_POS_FILE = os.path.join(DATA_DIR, "last_pos.json")

DEFAULT_MAPPINGS = {
    # "Sinar Mataram": "8140018210000",
}

# OpenWeather
OPENWEATHER_BASE = "https://api.openweathermap.org/data/2.5/weather"
OPENWEATHER_TIMEOUT_S = 10  # detik
# >>> HARD-CODED API KEY (override dengan ENV kalau tersedia)
OPENWEATHER_API_KEY = "f1c81dd3fc3e2d98c23a4b81b515604f"
# ============================================

# (Opsional) set env ADMIN_TOKEN=secret123 untuk proteksi endpoint /mappings
ADMIN_TOKEN_ENV = "secret123"

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("datagate-ngp")

app = FastAPI(title="Datagate→NGP + Weather (match by AssetDescription)")

_lock = threading.Lock()
NAME_TO_IMEI: Dict[str, str] = {}
LAST_POS: Dict[str, Dict[str, Any]] = {}

# ---------- Small utils ----------
def norm_name(s: Optional[str]) -> Optional[str]:
    return s.strip().casefold() if s else None

def to_float(txt: Optional[str]) -> Optional[float]:
    if txt is None: return None
    try: return float(txt)
    except: return None

def mph_to_kph(v: Optional[float]) -> Optional[int]:
    if v is None: return None
    return int(round(v * 1.609344))

def strip_ns(root: ET.Element) -> ET.Element:
    for e in root.iter():
        if "}" in e.tag:
            e.tag = e.tag.split("}", 1)[1]
    return root

def round6(v: Optional[float]) -> Optional[float]:
    return float(f"{v:.6f}") if (v is not None and not math.isnan(v)) else None

# ---------- Persistence (Mappings) ----------
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
                    changed = False
                    for k, v in DEFAULT_MAPPINGS.items():
                        if k not in data:
                            data[k] = v
                            changed = True
                    if changed:
                        save_mappings(data)
                    return data
        except Exception as e:
            log.warning("failed to load mappings file: %s", e)
    save_mappings(dict(DEFAULT_MAPPINGS))
    return dict(DEFAULT_MAPPINGS)

# ---------- Persistence (Last Position) ----------
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

# ---------- Auth (opsional) ----------
def require_admin(x_admin_token: Optional[str] = Header(default=None, alias="X-Admin-Token")):
    env_token = os.getenv(ADMIN_TOKEN_ENV, "").strip()
    if not env_token:
        return  # tidak aktif, lewati
    if x_admin_token is None:
        raise HTTPException(status_code=401, detail="Missing X-Admin-Token")
    if x_admin_token != env_token:
        raise HTTPException(status_code=403, detail="Invalid X-Admin-Token")

# ---------- NGP push ----------
def post_ngp(payload: dict) -> Tuple[bool, str]:
    try:
        r = requests.post(
            NGP_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=TIMEOUT_S
        )
        log.info("[NGP] %s %s", r.status_code, r.text[:160].replace("\n"," "))
        r.raise_for_status()
        return True, f"{r.status_code}"
    except requests.exceptions.HTTPError as he:
        try:
            code = he.response.status_code  # type: ignore
        except Exception:
            code = "HTTPError"
        return False, f"{code} {he}"
    except Exception as e:
        return False, str(e)

# ---------- OpenWeather ----------
def fetch_openweather(lat: float, lon: float) -> dict:
    # Prioritas: ENV lalu konstanta
    api_key = os.getenv("OPENWEATHER_API_KEY", "").strip() or OPENWEATHER_API_KEY
    if not api_key:
        raise HTTPException(status_code=500, detail="OPENWEATHER_API_KEY is not set (env or constant)")
    try:
        r = requests.get(
            OPENWEATHER_BASE,
            params={"lat": lat, "lon": lon, "appid": api_key, "units": "metric"},
            timeout=OPENWEATHER_TIMEOUT_S,
        )
        log.info("[OPENWEATHER] %s %s", r.status_code, r.text[:160].replace("\n"," "))
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as he:
        code = getattr(he.response, "status_code", 500)
        raise HTTPException(status_code=code, detail=f"OpenWeather error: {he}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenWeather request failed: {e}")

def build_weather_output(
    asset_meta: Dict[str, Any],
    lat: float,
    lon: float,
    weather_raw: dict
) -> dict:
    """
    Gabungkan metadata asset + posisi + blok cuaca (mirip contoh user).
    """
    out: Dict[str, Any] = {}

    # ---- metadata asset (kalau ada) ----
    for k in (
        "asset_name","asset_model","asset_mfg_year","asset_registration",
        "site_id","group_id","image","battery","charging_status","heading_calculation",
        "speed_calculation","temperature","humidity","battery_voltage","timestamp"
    ):
        if k in asset_meta and asset_meta[k] is not None:
            out[k] = asset_meta[k]

    # jika belum ada timestamp, set UTC now
    out.setdefault("timestamp", datetime.utcnow().replace(tzinfo=tz.utc).isoformat().replace("+00:00", "Z"))

    # ---- posisi ----
    out["latitude"] = round6(lat)
    out["longitude"] = round6(lon)
    out["coord"] = {"lon": float(f"{lon:.4f}"), "lat": float(f"{lat:.4f}")}

    # ---- salin blok OpenWeather (nama field tetap) ----
    for key in ("weather","base","main","visibility","wind","clouds","dt","sys","timezone","id","name","cod"):
        if key in weather_raw:
            out[key] = weather_raw[key]

    return out

# ---------- Models ----------
class MappingIn(BaseModel):
    name: str  # AssetDescription
    imei: str  # device_id NGP

# ---------- Startup banner ----------
@app.on_event("startup")
async def show_routes():
    _load_last_pos()
    log.info("Loading mappings from %s", MAPPINGS_FILE)
    log.info("Mappings loaded: %s", NAME_TO_IMEI)
    log.info("LastPos loaded: %d assets", len(LAST_POS))
    routes = [f"{','.join(sorted(r.methods or []))} {r.path}" for r in app.router.routes]
    log.info("Routes:\n%s", "\n".join(routes))

# ---------- Webhook (gabungan: cache + forward NGP) ----------
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
        sp_mph = to_float(get("Telemetry/Speed"))
        heading = to_float(get("Telemetry/Heading"))

        if not name:
            log.warning("[SKIP] missing AssetDescription")
            continue

        # update cache last position
        if lat is not None and lon is not None:
            key = norm_name(name) or name.strip().casefold()
            LAST_POS[key] = {
                "asset_name": name,
                "lat": lat,
                "lon": lon,
                "rx_time": rx_time or gmt_time or datetime.utcnow().replace(tzinfo=tz.utc).isoformat().replace("+00:00","Z"),
                "gps_valid": bool(gps_valid),
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

        payload = {"device_id": imei}
        if rx_time:
            payload["message_time"] = rx_time

        loc = {
            "fix_type": "HAS_FIX" if gps_valid else "NO_FIX",
            "latitude": lat,
            "longitude": lon
        }
        if gmt_time:
            loc["gnss_time"] = gmt_time
        payload["location"] = loc

        kph = mph_to_kph(sp_mph)
        if kph is not None:
            payload["speed"] = kph
        if heading is not None:
            try:
                payload["heading"] = int(round(heading))
            except Exception:
                pass

        ok, info = post_ngp(payload)
        log.info("[FORWARD] %s | name=%s | imei=%s | info=%s",
                "OK" if ok else "FAIL", name, imei, info)
        if ok:
            sent += 1

    return Response(content=f"OK ({sent})", media_type="text/plain", status_code=200)

# ---------- ENDPOINT: last lat/long + OpenWeather ----------
@app.get("/asset/{asset_name}/weather")
def asset_last_weather(asset_name: str = Path(..., description="AssetDescription (case-insensitive)")):
    key = norm_name(asset_name) or asset_name.strip().casefold()
    rec = LAST_POS.get(key)
    if not rec:
        raise HTTPException(status_code=404, detail=f"no last position for asset '{asset_name}'")

    lat = float(rec["lat"])
    lon = float(rec["lon"])

    asset_meta = {
        "asset_name": rec.get("asset_name"),
        "timestamp": rec.get("rx_time"),
    }

    wx = fetch_openweather(lat, lon)
    out = build_weather_output(asset_meta, lat, lon, wx)
    return out

# ---------- NEW ENDPOINT: lihat cache posisi terakhir ----------
@app.get("/lastpos")
def list_last_positions():
    return LAST_POS

# ---------- CRUD Mapping ----------
@app.get("/mappings")
def list_mappings():
    return NAME_TO_IMEI

@app.post("/mappings", status_code=201)
def add_mapping(m: MappingIn, admin: None = Depends(require_admin)):
    if not m.name.strip() or not m.imei.strip():
        raise HTTPException(status_code=400, detail="name & imei wajib diisi")
    nm = norm_name(m.name)
    for display in NAME_TO_IMEI.keys():
        if norm_name(display) == nm:
            raise HTTPException(status_code=409, detail="mapping sudah ada; gunakan PUT untuk edit")
    NAME_TO_IMEI[m.name] = m.imei
    save_mappings(NAME_TO_IMEI)
    return {"ok": True, "mappings": NAME_TO_IMEI}

@app.put("/mappings/{name}")
def edit_mapping(name: str, m: MappingIn, admin: None = Depends(require_admin)):
    nm_target = norm_name(name)
    found_key = None
    for display in NAME_TO_IMEI.keys():
        if norm_name(display) == nm_target:
            found_key = display
            break
    if not found_key:
        raise HTTPException(status_code=404, detail="mapping tidak ditemukan")
    if not m.imei.strip():
        raise HTTPException(status_code=400, detail="imei wajib diisi")
    del NAME_TO_IMEI[found_key]
    NAME_TO_IMEI[m.name] = m.imei
    save_mappings(NAME_TO_IMEI)
    return {"ok": True, "mappings": NAME_TO_IMEI}

@app.delete("/mappings/{name}", status_code=204)
def delete_mapping(name: str, admin: None = Depends(require_admin)):
    nm_target = norm_name(name)
    found_key = None
    for display in list(NAME_TO_IMEI.keys()):
        if norm_name(display) == nm_target:
            found_key = display
            break
    if found_key:
        del NAME_TO_IMEI[found_key]
        save_mappings(NAME_TO_IMEI)
    return Response(status_code=204)

@app.get("/health")
def health():
    return {"ok": True}
