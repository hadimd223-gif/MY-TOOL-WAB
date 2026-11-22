# M H X TopUp Backend (Python Flask) — ZRPay auto-detect version
# This server TRIES every common ZRPay request format automatically and remembers the one that works.
import os, math, json, time, uuid, secrets, re, threading, hashlib, functools, requests, copy
from concurrent.futures import ThreadPoolExecutor
from collections import Counter, deque
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
try:
    from dotenv import load_dotenv; load_dotenv()
except Exception:
    pass
from google.oauth2 import service_account, id_token
from google.auth.transport.requests import Request as GReq, AuthorizedSession

PROJECT = os.getenv("PROJECT_ID", "m-h-x-wab-7abdf")
KEYFILE = os.getenv("FIREBASE_KEY_FILE", "serviceAccount.json")

# ===== ZRPay keys: put them in the .env file (best) OR paste between the quotes below (this file stays private on your host) =====
API_KEY    = os.getenv("ZRPAYBD_API_KEY", "")    or ""
SECRET_KEY = os.getenv("ZRPAYBD_SECRET_KEY", "") or API_KEY
BRAND_KEY  = os.getenv("ZRPAYBD_BRAND_KEY", "")  or API_KEY
# =========================================================================================================================
CREATE_URL = os.getenv("ZRPAYBD_CREATE_URL", "https://pay.zrpaybd.xyz/api/payment/create")
VERIFY_URL = os.getenv("ZRPAYBD_VERIFY_URL", "https://pay.zrpaybd.xyz/api/payment/verify")
DEFAULT_ORIGIN = os.getenv("SITE_ORIGIN", "https://mhxtoolsff.vercel.app")

APP_VERSION = "2026-11-17-final"
_FS_LAST = {"t": 0.0, "code": 0, "msg": "", "n": 0}      # last Firestore overload answer (shown at /health)
_FS_COOL = {"until": 0.0}                               # while set, background loops sleep (see fs_cooling)
def _fs_hook(resp, *a, **k):
    try:
        if resp.status_code == 429 and "firestore.googleapis.com" in (resp.url or ""):
            _FS_LAST.update(t=time.time(), code=429, msg=(resp.text or "")[:400].replace("\n", " "), n=_FS_LAST["n"] + 1)
            _FS_COOL["until"] = time.time() + 120
    except Exception: pass
    return resp
SCOPES = ["https://www.googleapis.com/auth/datastore", "https://www.googleapis.com/auth/firebase.messaging"]
_sess = None
def S():
    global _sess
    if _sess is None:
        try:
            if os.getenv("FIREBASE_KEY_JSON"):
                c = service_account.Credentials.from_service_account_info(json.loads(os.environ["FIREBASE_KEY_JSON"].strip()), scopes=SCOPES)
            else:
                c = service_account.Credentials.from_service_account_file(KEYFILE, scopes=SCOPES)
            _sess = AuthorizedSession(c); _sess.hooks["response"].append(_fs_hook)
        except Exception as e:
            raise Err("Setup error: Firebase key file/JSON missing or invalid (" + type(e).__name__ + ")", 500)
    return _sess

DB = f"projects/{PROJECT}/databases/(default)/documents"
ROOT = "https://firestore.googleapis.com/v1/"
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

class Err(Exception):
    def __init__(s, m, c=400): s.m, s.c = m, c
@app.errorhandler(Err)
def _e(e): return jsonify(error=e.m), e.c
@app.errorhandler(Exception)
def _ex(e):
    from werkzeug.exceptions import HTTPException
    if isinstance(e, HTTPException): return jsonify(error=e.description), e.code
    if isinstance(e, requests.exceptions.HTTPError):
        sc = getattr(getattr(e, "response", None), "status_code", 0)
        print("FIRESTORE HTTP", sc, str(e)[:160])
        if sc == 429: return jsonify(error="সার্ভারে এখন অনেক চাপ, ১-২ মিনিট পর আবার চেষ্টা করুন।"), 503
        if sc >= 500: return jsonify(error="ডাটাবেস সাময়িকভাবে ব্যস্ত, একটু পরে আবার চেষ্টা করুন।"), 503
    print("UNHANDLED", type(e).__name__, e)
    return jsonify(error="Server error: " + type(e).__name__ + ": " + str(e)[:200]), 500

# ---------- Firestore REST helpers ----------
def dec(v):
    k, x = next(iter(v.items()))
    if k == "integerValue": return int(x)
    if k == "doubleValue": return float(x)
    if k == "nullValue": return None
    if k == "mapValue": return {a: dec(b) for a, b in x.get("fields", {}).items()}
    if k == "arrayValue": return [dec(i) for i in x.get("values", [])]
    return x
def enc(v):
    if isinstance(v, bool): return {"booleanValue": v}
    if isinstance(v, int): return {"integerValue": str(v)}
    if isinstance(v, float): return {"doubleValue": v}
    if v is None: return {"nullValue": None}
    return {"stringValue": str(v)}
# Config documents (settings/*, adsConfig/*) are read on almost every request but change rarely.
# They are cached in memory for FCACHE_TTL seconds, so a crowd of visitors costs ONE Firestore read, not thousands.
# Any write that touches such a document (commit) clears its cache at once, so admin changes still apply immediately.
# If Firestore is overloaded (429 / 5xx) the last known copy is served instead of an error.
_FCACHE = {}
_FCACHE_LOCK = threading.Lock()
FCACHE_TTL = 60
def fs_cooling(): return time.time() < _FS_COOL["until"]
_FCACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config_cache.json")
def _disk_save():
    try:
        with _FCACHE_LOCK: snap = {p: [v[1], v[2]] for p, v in _FCACHE.items()}
        with open(_FCACHE_FILE, "w", encoding="utf-8") as f: json.dump(snap, f)
    except Exception as ex: print("config_cache save failed", type(ex).__name__)
def _disk_load(path):
    try:
        with open(_FCACHE_FILE, encoding="utf-8") as f: d = json.load(f)
        v = d.get(path)
        return (v[0], v[1]) if v else None
    except Exception: return None
def _cacheable(path): return path.startswith("settings/") or path.startswith("adsConfig/")
def _fget_raw(path):
    last = None
    for attempt in range(3):
        r = S().get(ROOT + DB + "/" + path)
        if r.status_code == 404: return None, None
        if r.status_code in (429, 500, 502, 503, 504):
            if r.status_code == 429 and attempt == 0: print("FIRESTORE 429 on", path, "->", r.text[:300].replace("\n", " "))
            last = r; time.sleep(0.4 * (attempt + 1) ** 2); continue
        r.raise_for_status(); d = r.json()
        return {k: dec(v) for k, v in d.get("fields", {}).items()}, d["updateTime"]
    last.raise_for_status()
def fget(path):
    if not _cacheable(path): return _fget_raw(path)
    now = time.time()
    with _FCACHE_LOCK: c = _FCACHE.get(path)
    if c and now - c[0] < FCACHE_TTL: return copy.deepcopy(c[1]), c[2]
    try:
        v, t = _fget_raw(path)
    except Exception as ex:
        if c:
            print("FGET stale used", path, type(ex).__name__); return copy.deepcopy(c[1]), c[2]
        dv = _disk_load(path)                                # after a restart: use the copy saved on disk
        if dv:
            print("FGET disk copy used", path, type(ex).__name__)
            with _FCACHE_LOCK: _FCACHE[path] = (now - FCACHE_TTL + 15, copy.deepcopy(dv[0]), dv[1])
            return dv
        raise
    with _FCACHE_LOCK: _FCACHE[path] = (now, copy.deepcopy(v), t)
    if v is not None: _disk_save()
    return v, t
def _fcache_drop(writes):
    with _FCACHE_LOCK:
        for w in writes:
            n = (w.get("update") or {}).get("name") or w.get("delete") or ""
            p = n.split("/documents/", 1)[-1]
            if _cacheable(p): _FCACHE.pop(p, None)
def commit(writes):
    r = S().post(ROOT + DB + ":commit", json={"writes": writes})
    _fcache_drop(writes)
    if not r.ok: raise Err("Database error: " + r.text[:200], 500)
def w_set(path, data, mask=None, pre=None, inc=None, now=None):
    w = {"update": {"name": f"{DB}/{path}", "fields": {k: enc(v) for k, v in data.items()}},
         "updateMask": {"fieldPaths": mask if mask is not None else list(data.keys())}}
    tf = [{"fieldPath": k, "increment": {"integerValue": str(v)}} for k, v in (inc or {}).items()]
    tf += [{"fieldPath": k, "setToServerValue": "REQUEST_TIME"} for k in (now or [])]
    if tf: w["updateTransforms"] = tf
    if pre: w["currentDocument"] = pre
    return w
def w_del(path, pre=None):
    w = {"delete": f"{DB}/{path}"}
    if pre: w["currentDocument"] = pre
    return w
def fquery(collection, field=None, value=None, order=None, desc=True, limit=50, parent=None):
    """Firestore runQuery helper: one equality filter OR one orderBy (so no composite index is ever needed)."""
    q = {"from": [{"collectionId": collection}], "limit": limit}
    if field: q["where"] = {"fieldFilter": {"field": {"fieldPath": field}, "op": "EQUAL", "value": enc(value)}}
    if order: q["orderBy"] = [{"field": {"fieldPath": order}, "direction": "DESCENDING" if desc else "ASCENDING"}]
    r = S().post(ROOT + DB + (("/" + parent) if parent else "") + ":runQuery", json={"structuredQuery": q})
    r.raise_for_status(); out = []
    for it in r.json():
        d = it.get("document")
        if d: out.append((d["name"].rsplit("/", 1)[1], {k: dec(v) for k, v in d.get("fields", {}).items()}))
    return out
def flist(collection, mask=None, limit=2000):
    out, page = [], None
    while len(out) < limit:
        params = {"pageSize": 300}
        if page: params["pageToken"] = page
        if mask: params["mask.fieldPaths"] = mask
        r = S().get(ROOT + DB + "/" + collection, params=params)
        if r.status_code == 404: break
        r.raise_for_status(); j = r.json()
        for d in j.get("documents", []): out.append((d["name"].rsplit("/", 1)[1], {k: dec(v) for k, v in d.get("fields", {}).items()}))
        page = j.get("nextPageToken")
        if not page: break
    return out
def auth_user():
    h = request.headers.get("Authorization", "")
    if not h.startswith("Bearer "): raise Err("Login required", 401)
    try: c = id_token.verify_firebase_token(h[7:], GReq(), audience=PROJECT)
    except Exception: raise Err("Invalid login, please login again", 401)
    return c.get("user_id") or c.get("sub"), c

def pick(o, keys):
    if isinstance(o, dict):
        for k in keys:
            if o.get(k) not in (None, "", {}): return o[k]
        for v in o.values():
            x = pick(v, keys)
            if x: return x
    return None

# ---------- ZRPay: auto-detect the request format ----------
HDRS = {
    "keys3":   {"API-KEY": API_KEY, "SECRET-KEY": SECRET_KEY, "BRAND-KEY": BRAND_KEY},
    "uddokta": {"RT-UDDOKTAPAY-API-KEY": API_KEY},
    "xapi":    {"X-API-KEY": API_KEY},
    "bearer":  {"Authorization": "Bearer " + API_KEY},
    "none":    {},
    "xapi3":   {"X-API-KEY": API_KEY, "X-SECRET-KEY": SECRET_KEY, "X-BRAND-KEY": BRAND_KEY},
    "xapibr":  {"X-API-KEY": API_KEY, "X-BRAND-KEY": BRAND_KEY},
}
BODIES = ["std", "cus", "all"]
ENCS = ["json_s", "json_i", "form"]
COMBOS = [(b, h, e) for b in BODIES for h in HDRS for e in ENCS]
WFILE = "zr_working.json"
WORKING = None
try:
    if os.path.exists(WFILE):
        w = json.load(open(WFILE)); WORKING = (w["b"], w["h"], w["e"]) if (w["b"], w["h"], w["e"]) in COMBOS else None
except Exception: WORKING = None
_pos = 0

def make_body(kind, c):
    base = {"api_key": API_KEY, "secret_key": SECRET_KEY, "brand_key": BRAND_KEY, "amount": c["amt"], "metadata": {"ref": c["ref"], "uid": c["uid"]}}
    if kind == "std":
        base.update(full_name=c["nm"], email=c["em"], redirect_url=c["ok"], cancel_url=c["bad"], webhook_url=c["hook"], return_type="GET")
    elif kind == "cus":
        base.update(cus_name=c["nm"], cus_email=c["em"], cus_phone=c["ph"], success_url=c["ok"], cancel_url=c["bad"], fail_url=c["bad"], ipn_url=c["hook"], order_id=c["ref"], currency="BDT")
    else:
        base.update(full_name=c["nm"], cus_name=c["nm"], customer_name=c["nm"], name=c["nm"], email=c["em"], cus_email=c["em"], customer_email=c["em"],
                    phone=c["ph"], cus_phone=c["ph"], customer_phone=c["ph"], mobile=c["ph"], currency="BDT",
                    order_id=c["ref"], trx_id=c["ref"], transaction_id=c["ref"], reference=c["ref"],
                    redirect_url=c["ok"], success_url=c["ok"], return_url=c["ok"], callback_url=c["ok"],
                    cancel_url=c["bad"], fail_url=c["bad"], webhook_url=c["hook"], ipn_url=c["hook"])
    return base

def send(url, hdrs, body, e, timeout=8):
    h = {"Accept": "application/json"}; h.update(hdrs); body = dict(body)
    if e == "form":
        kw = {"data": {k: (json.dumps(v) if isinstance(v, (dict, list)) else str(v)) for k, v in body.items()}}
    else:
        body["amount"] = str(body["amount"]) if e == "json_s" else int(body["amount"]); kw = {"json": body}
    r = requests.post(url, headers=h, timeout=timeout, **kw)
    try: j = r.json()
    except Exception: j = {"raw": r.text[:200]}
    return r.status_code, j

def bad(j):
    if not isinstance(j, dict): return True
    msg = str(j.get("message", "")).lower()
    return j.get("status") in (0, "0", False, "false", "error", "failed") or "invalid" in msg or "raw" in j or "error" in j

def gateway_create(c):
    global WORKING, _pos
    def attempt(cb):
        b, h, e = cb
        code, j = send(CREATE_URL, HDRS[h], make_body(b, c), e)
        print(f"TRY body={b} hdr={h} enc={e} -> {code} {str(j)[:160]}")
        u = pick(j, ["payment_url", "paymentUrl", "checkout_url", "redirect_url", "payment_link", "link", "url"])
        return (u if isinstance(u, str) and u.startswith("http") else None), j
    if WORKING:
        u, j = attempt(WORKING)
        if u: return u, j
        WORKING = None
    seen = Counter(); tried = 0; end = time.time() + 22
    while tried < len(COMBOS) and time.time() < end:
        cb = COMBOS[_pos % len(COMBOS)]; _pos += 1; tried += 1
        try: u, j = attempt(cb)
        except Exception as ex: seen["connection: " + type(ex).__name__] += 1; continue
        if u:
            WORKING = cb
            try: json.dump({"b": cb[0], "h": cb[1], "e": cb[2]}, open(WFILE, "w"))
            except Exception: pass
            print("*** WORKING FORMAT FOUND:", cb); return u, j
        seen[(str(j.get("message") or j)[:70])] += 1
    raise Err(f"ZRPay rejected {tried} request formats. Replies: " + "; ".join(f"{m} x{n}" for m, n in seen.most_common(4)) + ". Check API key / ZRPay docs.", 502)

PAID = {"completed", "complete", "paid", "success", "successful", "succeeded", "approved"}
def gateway_verify(gid, ref):
    b, h, e = WORKING or ("all", "keys3", "json_s")
    tries = [{"invoice_id": gid, "transaction_id": gid, "payment_id": gid, "trx_id": gid, "id": gid, "order_id": ref, "ref": ref}] + \
            [{k: gid} for k in ("invoice_id", "transaction_id", "payment_id", "trx_id", "id")] + [{"order_id": ref}]
    last = {}
    for t in tries:
        body = {"api_key": API_KEY, "secret_key": SECRET_KEY, "brand_key": BRAND_KEY, "amount": 0, **t}
        try: code, j = send(VERIFY_URL, HDRS[h], body, e)
        except Exception as ex: print("VERIFY conn err", ex); continue
        print("VERIFY", t if len(t) < 3 else "all-ids", "->", code, str(j)[:300]); last = j
        if not bad(j): return j
    return last

def is_paid(res):
    for k in ("payment_status", "transaction_status", "trx_status", "paymentStatus"):
        v = pick(res, [k])
        if v is not None: return str(v).lower() in PAID
    return str(pick(res, ["status"]) or "").lower() in PAID

# ================= TELEGRAM CHANNEL NOTIFICATIONS (website -> your channel) =================
# Every important website event (purchase, deposit, ads bonus, referral, ban, admin balance change) is posted to the Telegram channel,
# headed "WEBSITE NOTIFICATION" so you can tell it apart from the bot's own posts. Sending runs in the background and can NEVER break an order/payment.
# Set TG_BOT_TOKEN / TG_CHANNEL_ID in .env (best). The fallback values below are the same bot + channel that bot.py already uses.
TG_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip() or "8802123213:AAHTzcYMwyHWYGD_gBB0zdhRWoDJDEVJe-k"
TG_CHANNEL = os.getenv("TG_CHANNEL_ID", "").strip() or "-1003924166076"
TG_ENABLED = os.getenv("TG_NOTIFY", "1") != "0"
_tg_pool = ThreadPoolExecutor(max_workers=2)

def _tg_send(emoji, title, uid, u, rows):
    try:
        if u is None and uid:
            try: u = fget("users/" + str(uid))[0]
            except Exception: u = None
        L = ["🌐 WEBSITE NOTIFICATION", "━━━━━━━━━━━━━━━━━━━", f"{emoji} {title}", ""]
        if uid:
            L += ["👤 User: " + (str((u or {}).get("name") or "N/A")[:40]), "📧 Email: " + str((u or {}).get("email") or "N/A")[:80], "🆔 User ID: " + str(uid)]
        for ic, lb, val in rows: L.append(f"{ic} {lb}: {val}")
        L += ["🕐 Time: " + bd_now() + " (BDT)", "━━━━━━━━━━━━━━━━━━━"]
        r = requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", json={"chat_id": TG_CHANNEL, "text": "\n".join(L)[:3900], "disable_web_page_preview": True}, timeout=12)
        if not r.ok: print("TG notify failed", r.status_code, r.text[:160])
    except Exception as ex: print("TG notify error", type(ex).__name__, ex)

def tg_notify(emoji, title, uid=None, u=None, rows=()):
    """rows = [(icon, label, value), ...]. Fire-and-forget."""
    if not TG_ENABLED or not TG_TOKEN or not TG_CHANNEL: return
    try: _tg_pool.submit(_tg_send, emoji, title, uid, u, list(rows))
    except Exception as ex: print("TG submit error", ex)

# ---------- routes ----------
@app.get("/")
@app.get("/health")
@app.get("/api/health")
def health():
    ago = int(time.time() - _FS_LAST["t"]) if _FS_LAST["t"] else None
    return jsonify(ok=True, version=APP_VERSION, firestore_429_count=_FS_LAST["n"], firestore_last_429_seconds_ago=ago,
                   firestore_last_429_reason=_FS_LAST["msg"] or None, firestore_cooling=fs_cooling(), project=PROJECT, api_key=bool(API_KEY), firebase=bool(os.getenv("FIREBASE_KEY_JSON") or os.path.exists(KEYFILE)),
                   zr_format=list(WORKING) if WORKING else "not detected yet")

# ---------- promo codes ----------
def now_ms(): return int(time.time() * 1000)
CTX_TYPES = {"order": ("discount_percent", "discount_flat"), "redeem": ("cash",), "deposit": ("bonus_percent",)}
CTX_HELP = {"order": "অর্ডারের সময় ব্যবহার করা যায়", "redeem": "প্রোফাইল থেকে রিডিম করা যায়", "deposit": "টাকা ডিপোজিটের সময় (Add Money পেজে) ব্যবহার করা যায়"}
def promo_load(code, uid, ctx):
    code = str(code or "").strip().upper()
    if not code: raise Err("প্রোমো কোড লিখুন")
    p, _ = fget("promos/" + code)
    if not p or p.get("active") is False:
        brute_check(uid, "promo"); raise Err("প্রোমো কোডটি সঠিক নয়")
    exp = int(p.get("expiresAtMs") or 0)
    if exp and now_ms() > exp: raise Err("এই প্রোমো কোডের মেয়াদ শেষ হয়ে গেছে")
    mx = int(p.get("maxUses") or 0)
    if mx and int(p.get("used") or 0) >= mx: raise Err("এই প্রোমো কোডের ব্যবহারের সীমা শেষ")
    if fget(f"promoUses/{code}_{uid}")[0]: raise Err("আপনি এই কোড আগেই ব্যবহার করেছেন")
    if p.get("type") not in CTX_TYPES[ctx]:
        t = p.get("type"); where = next((k for k, v in CTX_TYPES.items() if t in v), None)
        raise Err("এই কোডটি শুধু " + CTX_HELP.get(where, "অন্য জায়গায় ব্যবহার করা যায়"))
    p["code"] = code; return p
def promo_writes(p, uid, kind):
    return [w_set(f"promoUses/{p['code']}_{uid}", {"code": p["code"], "userId": uid, "kind": kind}, pre={"exists": False}, now=["at"]),
            w_set("promos/" + p["code"], {}, mask=[], inc={"used": 1})]
def calc_discount(p, price):
    v = float(p.get("value") or 0)
    return min(price, int(price * v / 100)) if p.get("type") == "discount_percent" else min(price, int(v))

@app.post("/checkPromo")
@app.post("/api/checkPromo")
def check_promo():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}; ctx = b.get("context", "order")
    if ctx not in CTX_TYPES: raise Err("Bad context")
    p = promo_load(b.get("code"), uid, ctx); out = {"ok": True, "type": p["type"], "value": p.get("value")}
    amt = int(float(b.get("amount") or 0))
    if ctx == "order" and amt: out["discount"] = calc_discount(p, amt)
    return jsonify(out)

@app.post("/redeemPromo")
@app.post("/api/redeemPromo")
def redeem_promo():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    u, ut = fget("users/" + uid)
    if not u or u.get("banned"): raise Err("Account banned", 403)
    p = promo_load(b.get("code"), uid, "redeem"); val = int(float(p.get("value") or 0))
    if val <= 0: raise Err("প্রোমো কোডে কোনো টাকা সেট করা নেই")
    commit([w_set("users/" + uid, {}, mask=[], inc={"balance": val, "lbScore": val}, pre={"updateTime": ut})] + promo_writes(p, uid, "cash") +
           [w_set("deposits/" + uuid.uuid4().hex, {"userId": uid, "userEmail": u.get("email", ""), "amount": 0, "reward": val, "method": "Promo " + p["code"], "status": "approved", "trxId": p["code"]},
                  pre={"exists": False}, now=["createdAt", "date"])])
    return jsonify(ok=True, amount=val)

# ---------- orders (server decides the price, user can never edit balance) ----------
def _place_like_order(uid, u, ut, svc, prod, gid, player, b, price, original, promo, disc, pf, name, oid):
    """Service type "like": the like API is called right away (no admin request). Money is reserved first and refunded if no like was delivered."""
    digits = "".join(ch for ch in str(player) if ch.isdigit())
    if not 5 <= len(digits) <= 15: raise Err("সঠিক UID দিন (শুধু সংখ্যা)")
    region = str(b.get("server") or "").strip().upper(); region = {"ME": "MEB", "US": "NA"}.get(region, region)
    if region not in LIKE_REGIONS: region = "BD"
    key = digits + region
    with _like_lock:
        if key in _like_busy: raise Err("এই UID-তে একটি রিকোয়েস্ট এখন প্রসেস হচ্ছে, একটু অপেক্ষা করুন", 409)
        _like_busy.add(key)
    try:
        base = {"userId": uid, "userName": name, "userEmail": u.get("email", ""), "gameName": svc.get("name", ""), "gameId": gid, "playerId": digits, "server": region,
                "productName": prod.get("name", ""), "price": price, "originalPrice": original, "promoCode": promo["code"] if promo else "", "type": "like", "payWith": pf}
        try: commit([w_set("users/" + uid, {}, mask=[], inc={pf: -price}, pre={"updateTime": ut}), w_set("orders/" + oid, dict(base, status="processing"), pre={"exists": False}, now=["date"])])
        except Err: raise Err("অর্ডার শুরু করা যায়নি, আবার চেষ্টা করুন", 409)
        settled = False
        try:
            res = call_like_api(digits, region)
            try: st = int(res.get("status"))
            except Exception: st = 0
            pname = str(res.get("PlayerNickname") or "N/A")
            if st == 1:
                given = to_int(res.get("LikesGivenByAPI")) or 0
                before, after = to_int(res.get("LikesBeforeCommand")), to_int(res.get("LikesAfterCommand"))
                order_no = str(res.get("OrderNo") or "N/A")
                try:
                    commit([w_set("orders/" + oid, {"status": "completed", "orderNo": order_no, "likesGiven": given, "playerName": pname}, now=["doneAt"]),
                            w_set("recentOrders/" + oid, {"name": name[:3] + "***", "productName": prod.get("name", ""), "price": price}, pre={"exists": False}, now=["date"])])
                except Exception as ex: print("LIKE ORDER record error", ex)
                if promo:
                    try: commit(promo_writes(promo, uid, "order"))
                    except Exception as ex: print("LIKE ORDER promo error", ex)
                tg_notify("🔥", "LIKE ORDER (AUTO)", uid, u, [("🎮", "Service", svc.get("name", "")), ("🆔", "UID", digits), ("👤", "Player", pname), ("🌍", "Server", region), ("📈", "Likes given", given), ("💰", "Price", f"৳{price}")])
                settled = True
                return jsonify(ok=True, like=True, status="success", price=price, discount=disc, orderId=oid, payWith=pf, given=given, before=before, after=after, player=pname, uid=digits, region=region, orderNo=order_no)
            refund_balance(uid, price, pf); settled = True
            msg = str(res.get("message") or ("এই UID আজকের লাইক ইতিমধ্যে পেয়ে গেছে।" if st == 2 else "লাইক পাঠানো যায়নি, আবার চেষ্টা করুন।"))
            try: commit([w_set("orders/" + oid, {"status": "failed", "apiError": msg[:200], "refunded": price}, now=["doneAt"])])
            except Exception as ex: print("LIKE ORDER record error", ex)
            raise Err(msg + " (টাকা ফেরত দেওয়া হয়েছে)", 400)
        except Err: raise
        except Exception as ex:
            print("LIKE ORDER unexpected", type(ex).__name__, ex)
            if not settled:
                refund_balance(uid, price, pf)
                try: commit([w_set("orders/" + oid, {"status": "failed", "apiError": "unexpected", "refunded": price}, now=["doneAt"])])
                except Exception as e2: print("LIKE ORDER unstick error", e2)
            raise Err("অপ্রত্যাশিত সমস্যা হয়েছে। টাকা ফেরত দেওয়া হয়েছে, আবার চেষ্টা করুন।", 500)
    finally:
        with _like_lock: _like_busy.discard(key)


@app.post("/placeOrder")
@app.post("/api/placeOrder")
def place_order():
    uid, claims = auth_user(); b = request.get_json(silent=True) or {}
    gid, pid = str(b.get("gameId", "")), str(b.get("productId", "")); player = str(b.get("playerId", "")).strip()[:80]
    u, ut = fget("users/" + uid)
    if not u: raise Err("User not found", 404)
    if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
    svc, _ = fget("services/" + gid); prod, _ = fget(f"services/{gid}/products/{pid}")
    if not svc or not prod: raise Err("প্যাকেজটি পাওয়া যায়নি", 404)
    stype = svc.get("type") or "uid"
    if stype != "code" and not player: raise Err("প্লেয়ার আইডি (UID) দিন")
    g, _ = fget("settings/general"); g = g or {}
    price = int(float(prod.get("price") or 0))
    if (u.get("role") or "user") in ("reseller", "admin"):
        dpct = max(0.0, min(100.0, float(g.get("resellerDiscount") or 0)))
        if g.get("resellerDiscountAll") and dpct: price = round(price * (1 - dpct / 100))   # admin: same % on every product
        elif prod.get("resellerPrice"): price = int(float(prod["resellerPrice"]))
        elif dpct: price = round(price * (1 - dpct / 100))
    original, promo, disc = price, None, 0
    if str(b.get("promoCode") or "").strip():
        promo = promo_load(b["promoCode"], uid, "order"); disc = calc_discount(promo, price); price -= disc
    pf = "balance"; price = check_pay(u, price)
    name = u.get("name") or (u.get("email") or "User").split("@")[0]; oid = uuid.uuid4().hex
    if stype == "like":
        return _place_like_order(uid, u, ut, svc, prod, gid, player, b, price, original, promo, disc, pf, name, oid)
    writes = [
        w_set("users/" + uid, {}, mask=[], inc={pf: -price}, pre={"updateTime": ut}),
        w_set("orders/" + oid, {"userId": uid, "userName": name, "userEmail": u.get("email", ""), "gameName": svc.get("name", ""), "gameId": gid, "playerId": player, "server": str(b.get("server", ""))[:8],
              "productName": prod.get("name", ""), "price": price, "originalPrice": original, "promoCode": promo["code"] if promo else "", "type": stype, "status": "pending", "payWith": pf},
              pre={"exists": False}, now=["date"]),
        w_set("recentOrders/" + oid, {"name": name[:3] + "***", "productName": prod.get("name", ""), "price": price}, pre={"exists": False}, now=["date"]),
    ]
    if promo: writes += promo_writes(promo, uid, "order")
    try: commit(writes)
    except Err: raise Err("অর্ডার সম্পন্ন করা যায়নি, আবার চেষ্টা করুন", 409)
    tg_notify("🛒", "NEW ORDER", uid, u, [("🎮", "Game", svc.get("name", "")), ("📦", "Package", prod.get("name", "")), ("🆔", "Player ID", player or "-"), ("💰", "Price", f"৳{price}" + (f" (promo {promo['code']} -৳{disc})" if promo else ""))])
    return jsonify(ok=True, price=price, discount=disc, orderId=oid, payWith=pf)

# ---------- payments ----------
@app.post("/createPayment")
@app.post("/api/createPayment")
def create_payment():
    if not API_KEY: raise Err("Server setup: ZRPay API key is empty (set ZRPAYBD_API_KEY).", 500)
    uid, claims = auth_user(); b = request.get_json(silent=True) or {}
    g, _ = fget("settings/general"); g = g or {}
    try: amount = int(float(b.get("amount", 0)))
    except Exception: raise Err("Invalid amount")
    if g.get("gatewayOff"): raise Err("পেমেন্ট সাময়িকভাবে বন্ধ আছে", 403)
    if amount < int(g.get("minDeposit", 10)) or amount > 100000: raise Err("Invalid amount")
    u, _ = fget("users/" + uid); u = u or {}
    if u.get("banned"): raise Err("Account banned", 403)
    origin = str(b.get("origin", "")).rstrip("/")
    if not origin.startswith("http"): origin = DEFAULT_ORIGIN
    dep_promo = None
    if str(b.get("promoCode") or "").strip(): dep_promo = promo_load(b["promoCode"], uid, "deposit")
    ref = uuid.uuid4().hex
    ctx = {"amt": amount, "ref": ref, "uid": uid, "nm": u.get("name") or "User",
           "em": u.get("email") or claims.get("email") or "user@example.com", "ph": str(u.get("phone") or u.get("mobile") or "01700000000"),
           "ok": f"{origin}/index.html?payok={ref}", "bad": f"{origin}/index.html", "hook": f"{origin}/api/health"}
    url, res = gateway_create(ctx)
    gid = str(pick(res, ["invoice_id", "payment_id", "transaction_id", "trx_id", "id"]) or ref)
    commit([w_set(f"payments/{ref}", {"userId": uid, "email": u.get("email", ""), "amount": amount, "status": "pending", "gatewayId": gid,
                          "promoCode": dep_promo["code"] if dep_promo else "", "bonusPercent": float(dep_promo.get("value") or 0) if dep_promo else 0},
                  pre={"exists": False}, now=["createdAt"])])
    return jsonify(paymentUrl=url)

@app.post("/verifyPayment")
@app.post("/api/verifyPayment")
def verify_payment():
    uid, _ = auth_user(); ref = str((request.get_json(silent=True) or {}).get("ref", ""))
    p, ptime = fget("payments/" + ref)
    if not p or p.get("userId") != uid: raise Err("Payment not found", 404)
    if p.get("status") == "paid": return jsonify(ok=True, amount=p["amount"], already=True)
    res = gateway_verify(p.get("gatewayId") or ref, ref)
    if not is_paid(res): return jsonify(ok=False, status=str(pick(res, ["status"]) or ""))
    try: paid = int(float(pick(res, ["amount"]) or 0))
    except Exception: paid = 0
    if paid and paid < p["amount"]: raise Err("Amount mismatch", 400)
    g, _ = fget("settings/general"); th = int((g or {}).get("resellerThreshold", 500))
    u, _ = fget("users/" + uid); u = u or {}
    amt = p["amount"]; total = int(u.get("totalDeposit", 0)) + amt
    bonus, pw = 0, []          # deposit-bonus promo: % extra on top of the deposit (goes to the main balance, NOT counted as a real deposit)
    if str(p.get("promoCode") or ""):
        try:
            pr = promo_load(p["promoCode"], uid, "deposit"); bonus = max(0, int(amt * float(pr.get("value") or 0) / 100))
            if bonus > 0: pw = promo_writes(pr, uid, "deposit")
        except Exception as ex: bonus, pw = 0, []; print("DEPOSIT PROMO skipped:", ex)
    promote = total >= th and (u.get("role") or "user") == "user"
    upd = {"role": "reseller"} if promote else {}
    cw = []
    ref_by = u.get("referredBy")
    if ref_by and ref_by != uid:
        pct = float((g or {}).get("refPercent", 10)); com = int(amt * pct / 100)
        rf_u = fget("users/" + str(ref_by))[0]
        if com > 0 and rf_u is not None and not rf_u.get("banned") and not u.get("refVoid") and not u.get("banned"):
            cw = [w_set("users/" + str(ref_by), {}, mask=[], inc={"balance": com, "lbScore": com, "refEarned": com}),
                  w_set("refEarnings/" + uuid.uuid4().hex, {"referrerId": ref_by, "fromUserId": uid, "fromName": (u.get("name") or "User")[:20], "amount": com, "base": amt, "kind": "commission"},
                        pre={"exists": False}, now=["date"])]
    commit([
        w_set("payments/" + ref, {"status": "paid"}, pre={"updateTime": ptime}, now=["paidAt"]),
        w_set("users/" + uid, upd, mask=list(upd.keys()), inc={"balance": amt + bonus, "lbScore": amt + bonus, "points": amt, "totalDeposit": amt}),
        w_set("deposits/" + uuid.uuid4().hex, {"userId": uid, "userEmail": u.get("email", ""), "amount": amt, "method": "ZRPay (Auto)" + (" + Promo " + str(p.get("promoCode")) if bonus > 0 else ""), "status": "approved", "trxId": p.get("gatewayId") or ref, **({"reward": bonus} if bonus > 0 else {})},
              pre={"exists": False}, now=["createdAt", "date"]),
    ] + cw + pw)
    tg_notify("💵", "NEW DEPOSIT", uid, u, [("💰", "Amount", f"৳{amt}")] + ([("🎁", "Promo bonus", f"+৳{bonus}")] if bonus > 0 else []) + ([("🤝", "Referral commission paid", f"৳{com}")] if cw else []) + ([("⭐", "Promoted", "Reseller")] if promote else []))
    return jsonify(ok=True, amount=amt, bonus=bonus, promoted=promote)

# ---------- Free Fire player info (proxied through the server so the browser has no CORS problem) ----------
FF_INFO_API = os.getenv("FF_INFO_API", "https://info-api-pink-seven.vercel.app/player-info")
# Optional: several info APIs separated by commas (tried one after another). Example in .env:
# FF_INFO_APIS=https://api-one.example.com/player-info,https://api-two.example.com/info
FF_INFO_APIS = [x.strip() for x in os.getenv("FF_INFO_APIS", "").split(",") if x.strip()] or [FF_INFO_API]
_NICK_KEYS = ("nickname", "playernickname", "player_name", "playername", "accountname", "name")
def has_player(o, depth=0):
    """True when the reply holds a player name under any common key (nickname / PlayerNickname / name ...)."""
    if depth > 6: return False
    if isinstance(o, dict):
        for k, v in o.items():
            if str(k).lower() in _NICK_KEYS and isinstance(v, str) and v.strip(): return True
        return any(has_player(v, depth + 1) for v in o.values())
    if isinstance(o, list): return any(has_player(v, depth + 1) for v in o[:5])
    return False

def fetch_player_info(uid, region):
    """Returns (json_or_None, last_http_status). Tries every configured API, upper/lower-case region and 1 retry on network/5xx errors."""
    j, code, net_err = None, None, None
    for base in FF_INFO_APIS:
        for rg in dict.fromkeys([region.upper(), region.lower()]):
            for attempt in range(2):
                try:
                    r = requests.get(base, params={"uid": uid, "region": rg, "server": rg}, timeout=20, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
                except Exception as ex:
                    net_err = ex; time.sleep(0.6); continue
                code = r.status_code
                try: j = r.json()
                except Exception: j = None
                print("PLAYERINFO", base[:40], uid, rg, code, str(j)[:160])
                if j is not None and has_player(j): return j, code
                if code < 500: break
                time.sleep(0.6)
    if j is None and net_err is not None: raise net_err
    return j, code

@app.get("/playerInfo")
@app.get("/api/playerInfo")
def player_info():
    uid = "".join(ch for ch in request.args.get("uid", "") if ch.isdigit())[:15]
    if len(uid) < 5: raise Err("সঠিক UID দিন")
    region = "".join(ch for ch in request.args.get("region", "") if ch.isalpha())[:6]
    if not region: raise Err("আগে রিজিয়ন সিলেক্ট করুন")
    try: j, code = fetch_player_info(uid, region)
    except Exception as ex: raise Err("ইনফো সার্ভারে পৌঁছানো যায়নি (" + type(ex).__name__ + ")", 502)
    if j is None: raise Err(f"ইনফো API থেকে সঠিক উত্তর আসেনি (HTTP {code})", 502)
    if not has_player(j) and (code is None or code in (401, 402, 403, 429) or code >= 500):
        # the third-party info API itself is down / unpaid / rate-limited: this is NOT "ID not found"
        raise Err("ID চেক সার্ভিস এখন সাময়িক বন্ধ আছে (HTTP %s)। আপনি চাইলে সরাসরি অর্ডার করতে পারেন।" % code, 503)
    return jsonify(j)

# =====================================================================================================
#  ANTI-FRAUD / AUTO-BAN SYSTEM  (referral abuse, fake emails, bots, glitch tricks)
#  * Every referral now goes into the "refQueue" collection first. NO money is added instantly.
#  * Instant auto-ban (BOTH the referrer and the referred account) when: referral burst, fake/abnormal/
#    disposable email, self-referral (same Gmail with dots/+ trick), same device making many accounts,
#    referral-code guessing.
#  * Everything that looks only "a bit strange" is held for the admin (Admin Panel -> Anti-Fraud).
#  * Clean referrals are paid automatically only after the hold time (default 24 hours).
#  * All thresholds are editable from the admin panel (settings/antifraud).
# =====================================================================================================
AF_DEFAULTS = {
    "enabled": True,          # master switch
    "holdMinutes": 1440,      # clean referral bonus is paid this many minutes after the referral (24h)
    "autoRelease": True,      # False = admin must approve every referral by hand
    "burst1Count": 3, "burst1Sec": 60,       # 3 referrals within 1 minute  -> instant ban
    "burst2Count": 5, "burst2Sec": 600,      # 5 referrals within 10 minutes -> instant ban
    "hourCount": 8,                          # 8 referrals within 1 hour -> instant ban
    "dayCount": 20,                          # 20 referrals within 24h -> held for admin review
    "ipFlag": 2, "ipBan": 5,                 # accounts referred from one IP in 24h: flag / ban
    "devBan": 2,                             # accounts referred from ONE device (2 = the 2nd one is banned)
    "banBadEmail": True,                     # fake / too-short / disposable / invalid Gmail -> ban
    "requireVerified": False,                # True = unverified e-mail is only "held for review"
    "strikeLimit": 3,                        # glitch strikes within 1 hour -> ban
    "bruteLimit": 6,                         # wrong referral/promo codes in 10 minutes -> ban
}
_af_cache = {"t": 0, "v": dict(AF_DEFAULTS)}
def af_cfg(force=False):
    if not force and time.time() - _af_cache["t"] < 15: return _af_cache["v"]
    cfg = dict(AF_DEFAULTS)
    try:
        d, _ = fget("settings/antifraud")
        for k, dv in AF_DEFAULTS.items():
            if d and k in d and d[k] not in (None, ""):
                cfg[k] = bool(d[k]) if isinstance(dv, bool) else max(0, to_i(d[k], dv))
    except Exception as ex: print("AF cfg error", type(ex).__name__, ex)
    _af_cache.update(t=time.time(), v=cfg); return cfg

def client_ip():
    h = request.headers.get("CF-Connecting-IP") or request.headers.get("X-Real-IP") or (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip() or (request.remote_addr or "")
    return h[:64]
def sha(s): return hashlib.sha1(str(s).encode()).hexdigest()[:24]
def clean_dev(v): return re.sub(r"[^A-Za-z0-9_\-]", "", str(v or ""))[:64]
def to_ms(v):
    try: return int(datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", str(v)).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception: return 0

# ---------- e-mail inspection ----------
DISPOSABLE = {"mailinator.com", "guerrillamail.com", "guerrillamail.net", "10minutemail.com", "10minutemail.net", "tempmail.com", "temp-mail.org", "temp-mail.io", "tempmail.net", "yopmail.com",
              "yopmail.net", "trashmail.com", "trashmail.net", "throwawaymail.com", "getnada.com", "nada.email", "maildrop.cc", "sharklasers.com", "dispostable.com", "fakeinbox.com", "mailnesia.com",
              "mintemail.com", "moakt.com", "emailondeck.com", "mohmal.com", "tempail.com", "burnermail.io", "spamgourmet.com", "mytemp.email", "tmpmail.org", "tmpmail.net", "discard.email",
              "mailcatch.com", "inboxkitten.com", "emailfake.com", "fakemail.net", "tempinbox.com", "mail.tm", "1secmail.com", "1secmail.net", "1secmail.org", "dropmail.me", "gmailnator.com", "tempr.email", "tmail.ws"}
GMAIL_TYPOS = {"gmial.com", "gmai.com", "gamil.com", "gmail.co", "gmail.con", "gmal.com", "gmaill.com", "gmail.cm", "gnail.com", "gmeil.com", "gmil.com", "gmail.om", "gmaol.com"}
def canon_email(e):
    e = str(e or "").strip().lower()
    if "@" not in e: return e
    loc, dom = e.rsplit("@", 1)
    if dom in ("gmail.com", "googlemail.com"): loc, dom = loc.split("+")[0].replace(".", ""), "gmail.com"
    return loc + "@" + dom
def email_verdict(email):
    """-> (level, [reasons]);  level 2 = fake/invalid (auto-ban), 1 = suspicious (admin review), 0 = fine"""
    e = str(email or "").strip().lower(); bad, sus = [], []
    if not e: return 2, ["ইমেইল নেই"]
    if not re.match(r"^[a-z0-9._%+\-]+@[a-z0-9\-]+(\.[a-z0-9\-]+)+$", e) or len(e) > 120: return 2, ["ইমেইলের ফরম্যাট সঠিক নয়: " + e[:40]]
    loc, dom = e.rsplit("@", 1)
    if dom in DISPOSABLE: bad.append("টেম্প/ডিসপোজেবল ইমেইল (" + dom + ")")
    if dom in GMAIL_TYPOS: bad.append("ভুয়া Gmail ডোমেইন (" + dom + ")")
    if dom in ("gmail.com", "googlemail.com"):
        base = loc.split("+")[0]
        if not re.match(r"^[a-z0-9.]+$", base) or base.startswith(".") or base.endswith(".") or ".." in base: bad.append("অবৈধ Gmail ইউজারনেম")
        c = base.replace(".", "")
        if len(c) < 6: bad.append(f"Gmail ইউজারনেম মাত্র {len(c)} অক্ষরের (Gmail-এ কমপক্ষে ৬)")
        elif len(c) > 30: bad.append("Gmail ইউজারনেম ৩০ অক্ষরের বেশি")
        if "+" in loc: sus.append("Gmail '+' alias")
        if c.isdigit(): sus.append("শুধু সংখ্যার Gmail")
        if len(c) >= 6 and len(set(c)) <= 2: sus.append("একই অক্ষর বারবার (Gmail)")
    else:
        if len(loc) < 3: bad.append("ইমেইলের ইউজারনেম খুব ছোট")
        if len(dom.rsplit(".", 1)[-1]) < 2: bad.append("ডোমেইন সঠিক নয়")
    if bad: return 2, bad + sus
    return (1 if sus else 0), sus

# ---------- ban + logs ----------
def fraud_log(uid, action, reason, details="", target=""):
    try:
        commit([w_set("fraudLog/" + uuid.uuid4().hex, {"userId": str(uid), "action": action, "reason": str(reason)[:300], "details": str(details)[:900], "target": str(target)[:80], "ts": now_ms()},
                      pre={"exists": False}, now=["at"])])
    except Exception as ex: print("fraudLog error", type(ex).__name__, ex)

def fraud_ban(uid, reason, details="", by="auto"):
    """Instantly bans one account (never an admin). Returns True when the account was banned."""
    uid = str(uid or "")
    if not uid: return False
    u, _ = fget("users/" + uid)
    if not u or (u.get("role") or "user") == "admin": return False
    newly = not u.get("banned")
    if newly:
        commit([w_set("users/" + uid, {"banned": True, "banReason": str(reason)[:200], "bannedBy": by, "bannedAtMs": now_ms()}, pre={"exists": True})])
    fraud_log(uid, "ban", reason, details, (u.get("email") or ""))
    if newly: tg_notify("⛔", "USER BANNED", uid, u, [("⚠️", "Reason", str(reason)[:250]), ("ℹ️", "Detail", str(details)[:250] or "-"), ("⚙️", "By", by)])
    print("AUTO-BAN", uid, reason); return True

# ---------- tiny string-log docs  ("ts:id;ts:id;...")  used for exact window counting ----------
def lg_read(path):
    d, ut = fget(path); out = []
    for part in str((d or {}).get("log") or "").split(";"):
        if ":" in part:
            a, b = part.split(":", 1)
            if a.isdigit(): out.append((int(a), b))
    return out, ut
def lg_write(path, entries, ut):
    s = ";".join(f"{a}:{b}" for a, b in entries[-60:])
    return w_set(path, {"log": s}, pre=({"updateTime": ut} if ut else {"exists": False}))
def within(entries, sec, now): return [(a, b) for a, b in entries if now - a <= sec * 1000]

# ---------- glitch strikes / brute force ----------
_bf, _bf_lock = {}, threading.Lock()
def bf_hit(key, window=600):
    """in-memory counter: how many times `key` happened in the last `window` seconds (including now)"""
    now = time.time()
    with _bf_lock:
        dq = _bf.setdefault(key, deque())
        while dq and now - dq[0] > window: dq.popleft()
        dq.append(now)
        if len(_bf) > 20000:
            for k in [k for k, v in _bf.items() if not v or now - v[-1] > window][:10000]: _bf.pop(k, None)
        return len(dq)
def fraud_strike(uid, reason, critical=False):
    cfg = af_cfg()
    if not cfg["enabled"]: return False
    if critical: return fraud_ban(uid, "Glitch/tamper: " + reason, "critical")
    now = now_ms(); ent, ut = lg_read("fraudStrikes/" + uid)
    ent = within(ent, 3600, now) + [(now, re.sub(r"[;:]", " ", reason)[:40])]
    try: commit([lg_write("fraudStrikes/" + uid, ent, ut)])
    except Err: pass
    fraud_log(uid, "strike", reason, f"{len(ent)}/{cfg['strikeLimit']}")
    if len(ent) >= max(1, cfg["strikeLimit"]): return fraud_ban(uid, "Repeated glitch attempts: " + reason, "; ".join(b for _, b in ent))
    return False
def brute_check(uid, kind):
    """wrong referral / promo codes tried again and again = code guessing -> ban"""
    cfg = af_cfg()
    if cfg["enabled"] and bf_hit(f"{kind}:{uid}") >= max(2, cfg["bruteLimit"]): fraud_ban(uid, f"Code guessing ({kind})", "wrong codes in 10 minutes")

# ---------- referral queue: approve / reject / ban ----------
def rf_approve(qid, by="auto"):
    q, qt = fget("refQueue/" + qid)
    if not q: raise Err("Referral পাওয়া যায়নি", 404)
    if q.get("status") != "pending": raise Err("এটি আগেই প্রসেস করা হয়েছে")
    rid, uid = str(q.get("referrerId")), str(q.get("referredId"))
    ru, _ = fget("users/" + rid); du, _ = fget("users/" + uid)
    if not ru or ru.get("banned") or not du or du.get("banned"):
        commit([w_set("refQueue/" + qid, {"status": "rejected", "note": "একজন ব্যান/অনুপস্থিত", "doneBy": by}, pre={"updateTime": qt}, now=["doneAt"])]); return 0
    bonus = to_i(q.get("bonus"))
    ws = [w_set("refQueue/" + qid, {"status": "approved", "doneBy": by}, pre={"updateTime": qt}, now=["doneAt"]),
          w_set("users/" + uid, {"refState": "approved"}, pre={"exists": True})]
    if bonus > 0:
        ws += [w_set("users/" + rid, {}, mask=[], inc={"balance": bonus, "lbScore": bonus, "refCount": 1, "refEarned": bonus}),
               w_set("refEarnings/" + uuid.uuid4().hex, {"referrerId": rid, "fromUserId": uid, "fromName": (du.get("name") or "User")[:20], "amount": bonus, "base": 0, "kind": "signup"}, pre={"exists": False}, now=["date"])]
    else: ws.append(w_set("users/" + rid, {}, mask=[], inc={"refCount": 1}))
    commit(ws)
    tg_notify("🎉", "REFERRAL BONUS PAID", rid, ru, [("👥", "Referred user", f"{du.get('name') or 'N/A'} ({uid})"), ("🎁", "Bonus", f"+৳{bonus}"), ("⚙️", "By", by)])
    return bonus
def rf_reject(qid, by="admin", note=""):
    q, qt = fget("refQueue/" + qid)
    if not q or q.get("status") != "pending": raise Err("এটি আগেই প্রসেস করা হয়েছে")
    commit([w_set("refQueue/" + qid, {"status": "rejected", "note": note[:200], "doneBy": by}, pre={"updateTime": qt}, now=["doneAt"]),
            w_set("users/" + str(q.get("referredId")), {"refState": "rejected", "refVoid": True}, pre={"exists": True})])
def rf_ban_both(qid, reason, by="admin"):
    q, qt = fget("refQueue/" + qid)
    if not q: raise Err("Referral পাওয়া যায়নি", 404)
    fraud_ban(q.get("referrerId"), reason, "referral " + qid, by); fraud_ban(q.get("referredId"), reason, "referral " + qid, by)
    if q.get("status") == "pending": commit([w_set("refQueue/" + qid, {"status": "banned", "note": reason[:200], "doneBy": by}, pre={"updateTime": qt}, now=["doneAt"])])

def rf_tick():
    """background worker: pays clean referrals after the hold time; risky ones wait for the admin"""
    if fs_cooling(): return 0
    cfg = af_cfg(); n = 0
    if not cfg["enabled"] or not cfg["autoRelease"]: return 0
    now = now_ms()
    for qid, q in fquery("refQueue", "status", "pending", limit=300):
        if q.get("risk") != "clean" or to_i(q.get("releaseAtMs")) > now: continue
        try: rf_approve(qid, "auto-release"); n += 1
        except Err: pass
        except Exception as ex: print("RF release error", type(ex).__name__, ex)
    return n
def rf_loop():
    time.sleep(25)
    while True:
        try: rf_tick()
        except Exception as ex: print("RF tick error", type(ex).__name__, ex)
        time.sleep(300 if fs_cooling() else 180)

# ---------- referral system ----------
REF_ALPHA = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
def ref_ads_percent():
    g, _ = fget("settings/general"); v = (g or {}).get("refAdsPercent")
    try: return max(0.0, min(100.0, float(v))) if v not in (None, "") else 5.0
    except Exception: return 5.0
def ref_settings():
    g, _ = fget("settings/general"); g = g or {}
    return int(float(g.get("refBonus", 1))), float(g.get("refPercent", 10))

@app.post("/getReferral")
@app.post("/api/getReferral")
def get_referral():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    u, ut = fget("users/" + uid)
    if not u: raise Err("User not found", 404)
    if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
    code = u.get("refCode")
    if not code:
        for _ in range(6):
            c = "".join(secrets.choice(REF_ALPHA) for _ in range(6))
            try:
                commit([w_set("refCodes/" + c, {"userId": uid}, pre={"exists": False}),
                        w_set("users/" + uid, {"refCode": c}, pre={"updateTime": ut} if ut else None)])
                code = c; break
            except Err:
                u, ut = fget("users/" + uid)
                code = (u or {}).get("refCode")
                if code: break
        if not code: raise Err("রেফার কোড তৈরি করা যায়নি, আবার চেষ্টা করুন")
        u, _ = fget("users/" + uid); u = u or {}
    # remember the referrer's own device / IP (used to catch self-referral farms) -- only written when it changes
    dev, ip = clean_dev(b.get("device")), client_ip(); chg = {}
    if dev and u.get("refDevice") != dev: chg["refDevice"] = dev
    if ip and u.get("lastIp") != ip: chg["lastIp"] = ip
    if chg:
        try: commit([w_set("users/" + uid, chg, pre={"exists": True})])
        except Err: pass
    bonus, pct = ref_settings(); cfg = af_cfg(); pend = 0
    try: pend = sum(1 for _, q in fquery("refQueue", "referrerId", uid, limit=200) if q.get("status") == "pending")
    except Exception: pass
    return jsonify(code=code, bonus=bonus, percent=pct, adsPercent=ref_ads_percent(), count=int(u.get("refCount") or 0), earned=int(u.get("refEarned") or 0),
                   pending=pend, holdMinutes=cfg["holdMinutes"] if cfg["enabled"] else 0)

def _af_apply(uid, claims, u, ut, rid, ru, code, bonus, ip, dev, ua, cfg):
    """Runs ONE referral through every anti-fraud rule. Instant-ban on cheating, otherwise it is queued (no money yet)."""
    now = now_ms(); flags, bans = [], []
    em_new = str(claims.get("email") or u.get("email") or ""); em_ref = str(ru.get("email") or "")
    # 1) e-mails ------------------------------------------------------------------------------
    for who, em in (("Referred", em_new), ("Referrer", em_ref)):
        lv, why = email_verdict(em)
        if lv == 2 and cfg["banBadEmail"]: bans.append(f"{who} e-mail: " + "; ".join(why))
        elif lv: flags += [f"{who}: {w}" for w in why]
    c_new, c_ref = canon_email(em_new), canon_email(em_ref)
    if c_new and c_new == c_ref: bans.append("Self-referral (same e-mail / Gmail dot-trick)")
    if c_new:
        cd, _ = fget("emailCanons/" + sha(c_new))
        if cd and str(cd.get("userId")) != uid: bans.append("Duplicate Gmail of another account (dot/+ trick)")
    if cfg["requireVerified"] and not claims.get("email_verified"): flags.append("Referred e-mail is not verified")
    # 2) device ---------------------------------------------------------------------------------
    dev_same = set()
    dev_ent, dev_ut, dev_path = [], None, ("refDevs/" + sha(dev) if dev else "")
    if dev:
        dev_ent, dev_ut = lg_read(dev_path)
        others = {b for _, b in dev_ent if b != uid}
        if len(others) >= max(1, cfg["devBan"] - 1): bans.append(f"Same device created {len(others) + 1} referred accounts"); dev_same = others
        if ru.get("refDevice") and ru.get("refDevice") == dev: bans.append("Referred account is on the referrer's own device")
    else: flags.append("Device ID missing (direct API call / old page?)")
    # 3) IP -------------------------------------------------------------------------------------
    ip_ent, ip_ut, ip_path = [], None, ("refIps/" + sha(ip) if ip else "")
    if ip:
        ip_ent, ip_ut = lg_read(ip_path)
        n_ip = len({b for a, b in within(ip_ent, 86400, now) if b != uid}) + 1
        if n_ip >= max(2, cfg["ipBan"]): bans.append(f"{n_ip} referred accounts from one IP in 24h")
        elif n_ip >= max(2, cfg["ipFlag"]): flags.append(f"{n_ip} referred accounts from one IP in 24h")
        if ru.get("lastIp") and ru.get("lastIp") == ip: flags.append("Referrer and referred share the same IP")
    # 4) speed of this referrer -------------------------------------------------------------------
    st_path = "refStats/" + rid
    st_ent, st_ut = lg_read(st_path); st_ent = [(a, b) for a, b in st_ent if b != uid]
    group = []
    for cnt, sec, label in ((cfg["burst1Count"], cfg["burst1Sec"], "{n} referrals in {s}s"), (cfg["burst2Count"], cfg["burst2Sec"], "{n} referrals in {s}s"), (cfg["hourCount"], 3600, "{n} referrals in 1 hour")):
        w = within(st_ent, sec, now)
        if cnt and len(w) + 1 >= cnt:
            bans.append("Referral burst: " + label.format(n=len(w) + 1, s=sec))
            if len(w) > len(group): group = w
    if cfg["dayCount"] and len(within(st_ent, 86400, now)) + 1 >= cfg["dayCount"]: flags.append("Very many referrals in 24 hours")
    rca = to_ms(ru.get("createdAt"))
    if rca and now - rca < 30 * 60 * 1000: flags.append("Referrer account is less than 30 minutes old")
    # ---------------- verdict ----------------
    qid = uuid.uuid4().hex
    base = {"referrerId": rid, "referrerName": str(ru.get("name") or "")[:30], "referrerEmail": em_ref[:80], "referredId": uid, "referredName": str(u.get("name") or "")[:30], "referredEmail": em_new[:80],
            "referredVerified": bool(claims.get("email_verified")), "bonus": bonus, "ts": now, "ip": ip, "device": dev, "ua": ua, "code": code}
    if bans:
        reason = "; ".join(bans)[:280]
        commit([w_set("refQueue/" + qid, dict(base, status="banned", risk="ban", flags=reason, note="Auto-ban", releaseAtMs=0), pre={"exists": False}, now=["createdAt"])])
        victims = {rid, uid} | {b for _, b in group} | dev_same
        for v in victims: fraud_ban(v, "Referral fraud: " + reason, "referral " + qid)
        for v in victims - {rid, uid}:                      # earlier referrals of the same burst never get paid
            try:
                for pq, pd in fquery("refQueue", "referredId", v, limit=20):
                    if pd.get("status") == "pending": commit([w_set("refQueue/" + pq, {"status": "banned", "note": "Same burst as " + qid}, mask=["status", "note"], pre={"exists": True})])
            except Exception as ex: print("AF burst clean error", type(ex).__name__, ex)
        raise Err("সন্দেহজনক কার্যকলাপের কারণে আপনার অ্যাকাউন্ট ব্যান করা হয়েছে।", 403)
    risk = "review" if flags else "clean"
    ws = [w_set("users/" + uid, {"referredBy": rid, "refState": "pending"}, pre={"updateTime": ut}),
          w_set("refQueue/" + qid, dict(base, status="pending", risk=risk, flags=" | ".join(flags)[:600], note="", releaseAtMs=now + cfg["holdMinutes"] * 60000), pre={"exists": False}, now=["createdAt"]),
          lg_write(st_path, st_ent + [(now, uid)], st_ut)]
    if ip: ws.append(lg_write(ip_path, ip_ent + [(now, uid)], ip_ut))
    if dev: ws.append(lg_write(dev_path, dev_ent + [(now, uid)], dev_ut))
    if c_new: ws.append(w_set("emailCanons/" + sha(c_new), {"userId": uid, "email": em_new[:80]}, pre={"exists": False}))
    try: commit(ws)
    except Err: raise Err("আবার চেষ্টা করুন", 409)
    return risk

@app.post("/applyReferral")
@app.post("/api/applyReferral")
def apply_referral():
    uid, claims = auth_user(); b = request.get_json(silent=True) or {}
    cfg = af_cfg(); ip = client_ip(); dev = clean_dev(b.get("device")); ua = (request.headers.get("User-Agent") or "")[:160]
    if cfg["enabled"]:
        for k, lim in ((f"refapply:{uid}", 6), (f"refapply-ip:{ip}", 20)):
            ok, _w = rate_ok(k, lim)
            if not ok:
                fraud_strike(uid, "applyReferral spam"); raise Err("একটু পরে আবার চেষ্টা করুন", 429)
    code = "".join(ch for ch in str(b.get("code", "")).upper() if ch.isalnum())[:12]
    if not code: raise Err("রেফার কোড লিখুন")
    u, ut = fget("users/" + uid)
    if not u: raise Err("User not found", 404)
    if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
    if u.get("referredBy"): raise Err("আপনি আগেই একটি রেফার কোড ব্যবহার করেছেন")
    if int(u.get("totalDeposit") or 0) > 0 or int(u.get("totalOrders") or 0) > 0: raise Err("রেফার কোড শুধু নতুন অ্যাকাউন্টে ব্যবহার করা যায়")
    ca = u.get("createdAt")
    if ca:
        try:
            t = datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", str(ca)).replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - t).total_seconds() > 2 * 86400: raise Err("রেফার কোড শুধু নতুন অ্যাকাউন্টে ব্যবহার করা যায়")
        except ValueError: pass
    r, _ = fget("refCodes/" + code)
    if not r:
        if cfg["enabled"]: brute_check(uid, "refcode")
        raise Err("রেফার কোডটি সঠিক নয়")
    rid = str(r.get("userId") or "")
    if rid == uid:
        if cfg["enabled"]: fraud_strike(uid, "own referral code")
        raise Err("নিজের রেফার কোড নিজে ব্যবহার করা যায় না")
    ru, _ = fget("users/" + rid)
    if not ru or ru.get("banned"): raise Err("রেফার কোডটি সঠিক নয়")
    bonus, _ = ref_settings()
    if not cfg["enabled"]:                                   # anti-fraud switched off in the admin panel -> old instant behaviour
        commit([w_set("users/" + uid, {"referredBy": rid}, pre={"updateTime": ut}),
                w_set("users/" + rid, {}, mask=[], inc={"balance": bonus, "lbScore": bonus, "refCount": 1, "refEarned": bonus}),
                w_set("refEarnings/" + uuid.uuid4().hex, {"referrerId": rid, "fromUserId": uid, "fromName": (u.get("name") or "User")[:20], "amount": bonus, "base": 0, "kind": "signup"}, pre={"exists": False}, now=["date"])])
        tg_notify("👥", "NEW REFERRAL (bonus paid)", uid, u, [("🤝", "Referred by", f"{ru.get('name') or 'N/A'} ({rid})"), ("🎁", "Bonus", f"৳{bonus}")])
        return jsonify(ok=True, referrer=str(ru.get("name") or "")[:20])
    risk = None
    for attempt in range(3):                                  # per-referrer lock: parallel/bot requests are handled one by one
        try:
            with user_lock(rid): risk = _af_apply(uid, claims, u, ut, rid, ru, code, bonus, ip, dev, ua, cfg)
            break
        except Err as e:
            if e.c != 409 or attempt == 2: raise
            u, ut = fget("users/" + uid); time.sleep(0.2 + 0.2 * attempt)
    tg_notify("👥", "NEW REFERRAL", uid, u, [("🤝", "Referred by", f"{ru.get('name') or 'N/A'} ({rid})"), ("🎁", "Bonus (after check)", f"৳{bonus}"), ("🔎", "Risk", risk or "-")])
    return jsonify(ok=True, pending=True, holdMinutes=cfg["holdMinutes"], referrer=str(ru.get("name") or "")[:20])

# ---------- admin: Anti-Fraud panel ----------
@app.post("/adminReferral")
@app.post("/api/adminReferral")
def admin_referral():
    require_admin(); b = request.get_json(silent=True) or {}; act = str(b.get("action") or "getConfig")
    if act == "setConfig":
        def num(k, lo, hi):
            try: v = float(b.get(k))
            except Exception: raise Err("সঠিক সংখ্যা দিন")
            if not lo <= v <= hi: raise Err(f"{lo} থেকে {hi} এর মধ্যে দিন")
            return v
        bonus, dep, ads = int(num("bonus", 0, 100000)), num("depositPercent", 0, 100), num("adsPercent", 0, 100)
        commit([w_set("settings/general", {"refBonus": bonus, "refPercent": dep, "refAdsPercent": ads})])
    bonus, dep = ref_settings()
    return jsonify(ok=True, bonus=bonus, depositPercent=dep, adsPercent=ref_ads_percent())

@app.post("/adminTgTest")
@app.post("/api/adminTgTest")
def admin_tg_test():
    """Sends one test message to the channel and returns Telegram's real answer, so a wrong token / channel / missing bot-admin right is visible."""
    require_admin()
    if not TG_TOKEN or not TG_CHANNEL: raise Err("TG_BOT_TOKEN / TG_CHANNEL_ID সেট করা নেই", 500)
    try:
        r = requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", json={"chat_id": TG_CHANNEL, "text": "🌐 WEBSITE NOTIFICATION\n━━━━━━━━━━━━━━━━━━━\n✅ TEST MESSAGE — ওয়েবসাইট থেকে নোটিফিকেশন ঠিকঠাক কাজ করছে।\n🕐 " + bd_now() + " (BDT)"}, timeout=15)
        j = r.json() if r.content else {}
    except Exception as ex: raise Err("Telegram-এ পৌঁছানো যায়নি: " + type(ex).__name__, 502)
    if not j.get("ok"): raise Err("Telegram বলছে: " + str(j.get("description") or r.status_code)[:200] + f"  (channel: {TG_CHANNEL})", 502)
    return jsonify(ok=True, channel=TG_CHANNEL)

@app.post("/adminTgNotify")
@app.post("/api/adminTgNotify")
def admin_tg_notify():
    """Admin panel changes that are written straight to Firestore by admin.html (add money, ban, manual deposit approve) report here."""
    me = require_admin(); b = request.get_json(silent=True) or {}; kind = str(b.get("kind") or "")
    mu, _ = fget("users/" + me); by = (mu or {}).get("email") or me
    uid = re.sub(r"[^A-Za-z0-9]", "", str(b.get("userId") or ""))[:40]
    u, _ = fget("users/" + uid) if uid else (None, None)
    if not u: raise Err("User not found", 404)
    amt = to_i(b.get("amount")); bal = ("৳" + str(to_i(u.get("balance"))))
    if kind == "balance" and amt: tg_notify("🛠️", "ADMIN " + ("ADDED MONEY" if amt > 0 else "DEDUCTED MONEY"), uid, u, [("💰", "Amount", f"{'+' if amt > 0 else '-'}৳{abs(amt)}"), ("💳", "Balance now", bal), ("⚙️", "By admin", by)])
    elif kind == "deposit" and amt > 0: tg_notify("💵", "MANUAL DEPOSIT APPROVED", uid, u, [("💰", "Amount", f"৳{amt}"), ("💳", "Balance now", bal), ("⚙️", "By admin", by)])
    elif kind == "ban": tg_notify("⛔", "USER BANNED (by admin)", uid, u, [("⚙️", "By admin", by)])
    elif kind == "unban": tg_notify("✅", "USER UNBANNED (by admin)", uid, u, [("⚙️", "By admin", by)])
    else: raise Err("Unknown kind")
    return jsonify(ok=True)

@app.post("/adminAntiFraud")
@app.post("/api/adminAntiFraud")
def admin_antifraud():
    me = require_admin(); b = request.get_json(silent=True) or {}; act = str(b.get("action") or "overview")
    qid = re.sub(r"[^A-Za-z0-9]", "", str(b.get("id") or ""))[:40]
    if act == "approve":
        amt = rf_approve(qid, "admin:" + me); return jsonify(ok=True, paid=amt)
    if act == "reject": rf_reject(qid, "admin:" + me, str(b.get("note") or "")); return jsonify(ok=True)
    if act == "ban": rf_ban_both(qid, "Banned by admin (referral review)", "admin:" + me); return jsonify(ok=True)
    if act == "unban":
        n = 0
        for x in (b.get("userIds") or [])[:10]:
            x = str(x)
            if fget("users/" + x)[0]: commit([w_set("users/" + x, {"banned": False, "banReason": "", "refVoid": False}, pre={"exists": True})]); fraud_log(x, "unban", "Unbanned by admin", "", me); n += 1
        return jsonify(ok=True, unbanned=n)
    if act == "save":
        s = b.get("settings") or {}; data = {}
        for k, dv in AF_DEFAULTS.items():
            if k in s: data[k] = bool(s[k]) if isinstance(dv, bool) else max(0, to_i(s[k], dv))
        if data: commit([w_set("settings/antifraud", data)])
        af_cfg(True); return jsonify(ok=True, settings=af_cfg(True))
    if act == "releaseNow":
        return jsonify(ok=True, paid=rf_tick())
    # overview
    flt = str(b.get("filter") or "pending")
    rows = fquery("refQueue", "status", flt, limit=200) if flt in ("pending", "approved", "rejected", "banned") else fquery("refQueue", order="ts", limit=200)
    if flt == "review": rows = [x for x in fquery("refQueue", "status", "pending", limit=300) if x[1].get("risk") == "review"]
    if flt == "clean": rows = [x for x in fquery("refQueue", "status", "pending", limit=300) if x[1].get("risk") == "clean"]
    rows.sort(key=lambda x: to_i(x[1].get("ts")), reverse=True)
    keep = ("referrerId", "referrerName", "referrerEmail", "referredId", "referredName", "referredEmail", "referredVerified", "status", "risk", "flags", "bonus", "releaseAtMs", "ts", "ip", "device", "ua", "note", "doneBy")
    items = [dict({k: d.get(k) for k in keep}, id=i) for i, d in rows[:150]]
    pend = fquery("refQueue", "status", "pending", limit=300)
    stats = {"pending": len(pend), "review": sum(1 for _, d in pend if d.get("risk") == "review"), "clean": sum(1 for _, d in pend if d.get("risk") == "clean")}
    logs = [{"userId": d.get("userId"), "action": d.get("action"), "reason": d.get("reason"), "details": d.get("details"), "target": d.get("target"), "ts": d.get("ts")} for _, d in fquery("fraudLog", order="ts", limit=60)]
    return jsonify(ok=True, settings=af_cfg(True), stats=stats, items=items, logs=logs, defaults=AF_DEFAULTS)

# ---------- Daily Like (same fflikebd API & same rules as the Telegram bot) ----------
# Key is read from the .env (LIKE_API_KEY). The fallback below is the key already used by the bot.
LIKE_API_KEY  = os.getenv("LIKE_API_KEY", "") or "JIBON-5EHN2TGWG249VP"
LIKE_API_BASE = (os.getenv("LIKE_API_BASE", "") or "https://fflikebd.site/ghostxlike").replace("://www.", "://")   # https://fflikebd.site/ghostxlike?region=BD&uid={uid}&userkey=KEY
LIKE_PKGORDER = os.getenv("LIKE_PKGORDER", "")   # empty = not sent (the new URL has no pkgorder)
LIKE_REGIONS  = ("BD", "IND", "SG", "ID", "TH", "PK", "MEB", "BR", "SAC", "NA", "EU", "VN", "TW", "RU")
_like_busy, _like_lock = set(), threading.Lock()

def bd_now(): return datetime.now(timezone(timedelta(hours=6))).strftime("%Y-%m-%d %I:%M:%S %p")
def to_int(x):
    try: return int(float(str(x).replace(",", "").strip()))
    except Exception: return None

def call_like_api(uid, region, max_retries=1):
    """Same behaviour as call_like_api() in bot.py: GET the fflikebd API, map HTTP errors to friendly results."""
    params = {"region": region, "uid": str(uid), "userkey": LIKE_API_KEY}
    if LIKE_PKGORDER: params["pkgorder"] = LIKE_PKGORDER
    last = None
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(LIKE_API_BASE, params=params, timeout=(10, 50), headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
            print("LIKE API", attempt + 1, uid, region, r.status_code, r.text[:300])
            try: res = r.json()
            except Exception: res = {"status": 0, "error": "invalid_response", "message": f"Server returned HTTP {r.status_code}"}
            if not isinstance(res, dict): res = {"status": 0, "error": "invalid_response", "message": "Invalid server reply"}
            if "status" not in res: res["status"] = 0
            c = r.status_code
            if c == 400:
                m = str(res.get("message") or res.get("error") or res.get("msg") or "")
                res.update(status=0, error="bad_request", message=("UID বা রিজিয়ন সঠিক নয়। UID ও সার্ভার (BD/IND/SG...) চেক করে আবার চেষ্টা করুন।" if (not m or "Client Error" in m or "Bad Request" in m) else m)); return res
            if c == 401: res.update(status=0, error="invalid_key", message="সার্ভিসে সাময়িক সমস্যা, সাপোর্টে যোগাযোগ করুন।"); return res
            if c == 402: res.update(status=0, error="insufficient_balance", message="সার্ভিসে সাময়িক সমস্যা, কিছুক্ষণ পর চেষ্টা করুন।"); return res
            if c == 403: res.update(status=0, error="no_permission", message="সার্ভিসে সাময়িক সমস্যা, সাপোর্টে যোগাযোগ করুন।"); return res
            if c in (409, 429):
                if attempt < max_retries: time.sleep(2 + attempt * 2); continue
                res.update(status=0, error="rate_limited" if c == 429 else "order_in_progress", message="১ মিনিট অপেক্ষা করে আবার চেষ্টা করুন।"); return res
            if c == 502: res.update(status=0, error="delivery_failed", message="লাইক ডেলিভারি হয়নি। কোনো টাকা কাটা হয়নি, আবার চেষ্টা করুন।"); return res
            if c >= 500:
                if attempt < max_retries: time.sleep(2); continue
                res.update(status=0, error="server_error", message="লাইক সার্ভারে সমস্যা, একটু পরে চেষ্টা করুন।"); return res
            return res
        except requests.exceptions.Timeout:
            last = {"status": 0, "error": "timeout", "message": "লাইক সার্ভার সাড়া দিচ্ছে না, আবার চেষ্টা করুন।"}; break
        except requests.exceptions.RequestException as ex:
            print("LIKE API network error", ex); last = {"status": 0, "error": "network_error", "message": "নেটওয়ার্ক সমস্যা, আবার চেষ্টা করুন।"}
        except Exception as ex:
            print("LIKE API unexpected", ex); last = {"status": 0, "error": "unknown", "message": "অপ্রত্যাশিত সমস্যা, আবার চেষ্টা করুন।"}; break
        if attempt < max_retries: time.sleep(2)
    return last or {"status": 0, "error": "unknown", "message": "আবার চেষ্টা করুন।"}

def check_pay(u, price):
    """One wallet only: the main balance. Raises a friendly error if it is too small, else returns the price."""
    if to_i(u.get("balance")) < price: raise Err("আপনার পর্যাপ্ত ব্যালেন্স নেই। ওয়ালেটে টাকা অ্যাড করুন।")
    return price
def refund_balance(uid, amount, field="balance"):
    for _ in range(3):
        try: commit([w_set("users/" + uid, {}, mask=[], inc={"balance": amount})]); return True
        except Exception as ex: print("REFUND retry", uid, amount, ex); time.sleep(1)
    print("!!! REFUND FAILED", uid, amount); return False

def like_price_for(u, g):
    price = int(float(g.get("dailyLikePrice", 6) or 6))
    if (u.get("role") or "user") in ("reseller", "admin"):
        if g.get("dailyLikeResellerPrice") not in (None, ""): price = int(float(g["dailyLikeResellerPrice"]))
        elif g.get("resellerDiscount"): price = round(price * (1 - float(g["resellerDiscount"]) / 100))
    return max(price, 0)

def like_off(g):
    en = g.get("dailyLikeEnabled", True)
    return en is False or str(en).strip().lower() in ("off", "false", "0")

@app.get("/dailyLikeInfo")
@app.get("/api/dailyLikeInfo")
def daily_like_info():
    g, _ = fget("settings/general"); g = g or {}
    return jsonify(price=int(float(g.get("dailyLikePrice", 6) or 6)), resellerPrice=(int(float(g["dailyLikeResellerPrice"])) if g.get("dailyLikeResellerPrice") not in (None, "") else None),
                   likes=int(float(g.get("dailyLikeAmount", 220) or 220)), enabled=not like_off(g), offMsg=str(g.get("dailyLikeOffMsg") or ""), regions=list(LIKE_REGIONS))

@app.post("/dailyLikeStatus")
@app.post("/api/dailyLikeStatus")
def daily_like_status():
    """After a dropped connection the page asks here what happened to its request (same reqId) instead of guessing."""
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    rid = re.sub(r"[^A-Za-z0-9]", "", str(b.get("reqId") or ""))[:40]
    if not rid: raise Err("reqId required")
    d, _ = fget(f"dailyLikes/{uid[:28]}_{rid}")
    if not d: return jsonify(found=False)
    if d.get("status") == "pending": return jsonify(found=True, pending=True)
    try: return jsonify(found=True, pending=False, result=json.loads(d.get("resp") or "{}"))
    except Exception: return jsonify(found=True, pending=False, result={"ok": False, "status": "failed", "message": "রিকোয়েস্টটি প্রসেস হয়েছে, অর্ডার হিস্টোরি চেক করুন।"})

@app.post("/dailyLike")
@app.post("/api/dailyLike")
def daily_like():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    player = "".join(ch for ch in str(b.get("uid", "")) if ch.isdigit())
    if not 5 <= len(player) <= 15: raise Err("সঠিক UID দিন (শুধু সংখ্যা)")
    region = str(b.get("region", "")).strip().upper()
    if region not in LIKE_REGIONS: raise Err("আগে সঠিক রিজিয়ন (সার্ভার) সিলেক্ট করুন")
    rid = re.sub(r"[^A-Za-z0-9]", "", str(b.get("reqId") or ""))[:40] or uuid.uuid4().hex
    docid = f"{uid[:28]}_{rid}"
    # same request sent twice (network retry / double tap) -> never charge twice
    prev, _ = fget("dailyLikes/" + docid)
    if prev:
        if prev.get("status") == "pending": raise Err("আপনার রিকোয়েস্ট প্রসেস হচ্ছে, একটু অপেক্ষা করুন", 409)
        try: return jsonify(json.loads(prev.get("resp") or "{}"))
        except Exception: raise Err("রিকোয়েস্টটি আগেই প্রসেস হয়েছে", 409)
    g, _ = fget("settings/general"); g = g or {}
    if like_off(g): raise Err(str(g.get("dailyLikeOffMsg") or "🔥 ডেইলি লাইক সার্ভিস সাময়িকভাবে বন্ধ আছে। পরে আবার চেষ্টা করুন।"), 403)
    u, ut = fget("users/" + uid)
    if not u: raise Err("User not found", 404)
    if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
    price = like_price_for(u, g); amount_label = int(float(g.get("dailyLikeAmount", 220) or 220))
    pf = "balance"; price = check_pay(u, price)
    key = player + region
    with _like_lock:
        if key in _like_busy: raise Err("এই UID-তে একটি রিকোয়েস্ট এখন প্রসেস হচ্ছে, একটু অপেক্ষা করুন", 409)
        _like_busy.add(key)
    try:
        # 1) reserve the price first (atomic, so the balance can never go negative / be spent twice)
        try:
            commit([w_set("users/" + uid, {}, mask=[], inc={pf: -price}, pre={"updateTime": ut}),
                    w_set("dailyLikes/" + docid, {"userId": uid, "playerId": player, "region": region, "price": price, "status": "pending", "payWith": pf}, pre={"exists": False}, now=["createdAt"])])
        except Err: raise Err("রিকোয়েস্ট শুরু করা যায়নি, আবার চেষ্টা করুন", 409)
        settled = False
        try:
            # 2) call the like API (exactly like the bot does)
            res = call_like_api(player, region)
            try: st = int(res.get("status"))
            except Exception: st = 0
            name = str(res.get("PlayerNickname") or "N/A"); shown_uid = str(res.get("UID") or player); shown_region = str(res.get("Region") or region)
            now = bd_now()
            if st == 1:
                given = to_int(res.get("LikesGivenByAPI")) or 0
                before, after = to_int(res.get("LikesBeforeCommand")), to_int(res.get("LikesAfterCommand"))
                out = {"ok": True, "status": "success", "uid": shown_uid, "player": name, "region": shown_region, "before": before, "given": given, "after": after,
                       "package": str(res.get("Package") or f"{amount_label} LIKE INSTANT"), "orderNo": str(res.get("OrderNo") or "N/A"), "price": price, "time": now, "payWith": pf,
                       "message": str(res.get("message") or "")}
                pname = f"{given} Likes" + (f" • {before} → {after}" if before is not None and after is not None else "")
                try:
                    commit([w_set("dailyLikes/" + docid, {"status": "success", "resp": json.dumps(out, ensure_ascii=False), "player": name, "given": given, "before": before, "after": after, "orderNo": out["orderNo"]}, now=["doneAt"]),
                            w_set("orders/" + uuid.uuid4().hex, {"userId": uid, "userName": u.get("name", ""), "userEmail": u.get("email", ""), "gameName": "🔥 Daily Like", "gameId": "dailylike", "playerId": shown_uid, "server": shown_region,
                                  "productName": pname, "price": price, "originalPrice": price, "promoCode": "", "type": "dailylike", "status": "completed", "orderNo": out["orderNo"], "payWith": pf}, pre={"exists": False}, now=["date"]),
                            w_set("recentOrders/" + uuid.uuid4().hex, {"name": str(u.get("name") or "User")[:3] + "***", "productName": f"Daily Like {given}+", "price": price}, pre={"exists": False}, now=["date"])])
                except Exception as ex: print("DAILYLIKE record error", ex)
                tg_notify("🔥", "DAILY LIKE BOUGHT", uid, u, [("🎮", "UID", shown_uid), ("👤", "Player", name), ("🌍", "Server", shown_region), ("📈", "Likes given", given), ("💰", "Price", f"৳{price}")])
                settled = True
                return jsonify(out)
            # not delivered -> give the reserved money back
            refund_balance(uid, price, pf); settled = True
            if st == 2:
                out = {"ok": False, "status": "already", "uid": shown_uid, "player": name, "region": shown_region, "price": price, "refunded": price, "time": now,
                       "message": str(res.get("message") or "এই UID আজকের লাইক ইতিমধ্যে পেয়ে গেছে।")}
            else:
                out = {"ok": False, "status": "failed", "error": str(res.get("error") or "unknown"), "uid": player, "region": region, "price": price, "time": now,
                       "message": str(res.get("message") or "লাইক পাঠানো যায়নি, আবার চেষ্টা করুন।")}
            try: commit([w_set("dailyLikes/" + docid, {"status": out["status"], "resp": json.dumps(out, ensure_ascii=False), "player": name}, now=["doneAt"])])
            except Exception as ex: print("DAILYLIKE record error", ex)
            return jsonify(out)
        except Err: raise
        except Exception as ex:
            print("DAILYLIKE unexpected", type(ex).__name__, ex)
            if not settled:
                refund_balance(uid, price, pf)
                try: commit([w_set("dailyLikes/" + docid, {"status": "failed", "resp": json.dumps({"ok": False, "status": "failed", "message": "লাইক পাঠানো যায়নি। টাকা ফেরত দেওয়া হয়েছে, আবার চেষ্টা করুন।", "refunded": price}, ensure_ascii=False)}, now=["doneAt"])])
                except Exception as e2: print("DAILYLIKE unstick error", e2)
            raise Err("অপ্রত্যাশিত সমস্যা হয়েছে। টাকা ফেরত দেওয়া হয়েছে, আবার চেষ্টা করুন।", 500)
    finally:
        with _like_lock: _like_busy.discard(key)

# ======================================================================
# ======================================================================
# Guild Glory  (bots join the user's guild and farm glory)
# The bots run on the separate "Guild Glory" server (Ajay_Guild_Glory / start.py). This backend only talks to it
# server-to-server, so the secret never reaches the browser.  Put in the .env of this backend:
#     GLORY_API_BASE=https://YOUR-GLORY-SERVER-URL        GLORY_API_SECRET=<api_secret printed by the glory server>
# Admin controls (admin panel -> Guild Glory) live in settings/general: gloryEnabled, gloryMode (free|paid), gloryFreeBots,
# gloryOptions (e.g. [4,8,12,16,20]), gloryPricePerBot, gloryResellerPricePerBot, gloryFreeDaily, gloryOffMsg.
# ======================================================================
GLORY_API_BASE   = (os.getenv("GLORY_API_BASE", "") or "").strip().rstrip("/")
GLORY_API_SECRET = os.getenv("GLORY_API_SECRET", "") or ""
GLORY_REGIONS    = ("BD", "IND", "PK", "SG", "ID", "TH", "VN", "TW", "BR", "SAC", "NA", "ME", "RU", "CIS")
_glory_busy, _glory_lock = set(), threading.Lock()

def glory_call(method, path, payload=None, params=None, timeout=25):
    if not GLORY_API_BASE or not GLORY_API_SECRET: raise Err("Guild Glory সার্ভার এখনো কানেক্ট করা হয়নি (অ্যাডমিনকে জানান)", 503)
    try:
        r = requests.request(method, GLORY_API_BASE + path, json=payload, params=params, timeout=timeout,
                             headers={"X-Api-Secret": GLORY_API_SECRET, "Accept": "application/json", "User-Agent": "MHX-Backend"})
    except Exception as ex:
        print("GLORY network error", type(ex).__name__, ex); raise Err("Guild Glory সার্ভারের সাথে কানেক্ট করা যাচ্ছে না, একটু পরে আবার চেষ্টা করুন", 503)
    try: j = r.json()
    except Exception: j = {}
    return r.status_code, (j if isinstance(j, dict) else {})

def glory_cfg(g):
    raw = g.get("gloryOptions")
    if isinstance(raw, str):
        try: raw = json.loads(raw)
        except Exception: raw = [x for x in re.split(r"[,\s]+", raw) if x]
    opts = set()
    for x in (raw if isinstance(raw, list) else [4, 8, 12, 16, 20]):
        n = to_i(x, 0)
        if 1 <= n <= 200: opts.add(n)
    opts = sorted(opts) or [4]
    en = g.get("gloryEnabled", True)
    return {"enabled": not (en is False or str(en).strip().lower() in ("off", "false", "0")),
            "mode": "paid" if str(g.get("gloryMode", "free")).strip().lower() == "paid" else "free",
            "freeBots": max(1, to_i(g.get("gloryFreeBots"), 4)), "options": opts,
            "pricePerBot": max(0, to_i(g.get("gloryPricePerBot"), 2)),
            "resellerPricePerBot": (max(0, to_i(g.get("gloryResellerPricePerBot"), 0)) if g.get("gloryResellerPricePerBot") not in (None, "") else None),
            "freeDaily": max(0, to_i(g.get("gloryFreeDaily"), 0)), "offMsg": str(g.get("gloryOffMsg") or "")}

def glory_unit_price(u, g, c):
    if c["mode"] != "paid": return 0
    p = c["pricePerBot"]
    if (u.get("role") or "user") in ("reseller", "admin"):
        if c["resellerPricePerBot"] is not None: p = c["resellerPricePerBot"]
        elif g.get("resellerDiscount"): p = round(p * (1 - float(g["resellerDiscount"]) / 100))
    return max(int(p), 0)

@app.get("/guildGloryInfo")
@app.get("/api/guildGloryInfo")
def guild_glory_info():
    g, _ = fget("settings/general"); g = g or {}; c = glory_cfg(g)
    opts = [{"bots": n, "locked": c["mode"] == "free" and n > c["freeBots"]} for n in c["options"]]
    if c["mode"] == "free" and not any(not o["locked"] for o in opts): opts.insert(0, {"bots": c["freeBots"], "locked": False})
    return jsonify(enabled=c["enabled"], mode=c["mode"], freeBots=c["freeBots"], options=opts, pricePerBot=c["pricePerBot"],
                   resellerPricePerBot=c["resellerPricePerBot"], freeDaily=c["freeDaily"], offMsg=c["offMsg"], regions=list(GLORY_REGIONS),
                   serverConfigured=bool(GLORY_API_BASE and GLORY_API_SECRET))

@app.post("/guildGloryCheck")
@app.post("/api/guildGloryCheck")
def guild_glory_check():
    auth_user(); b = request.get_json(silent=True) or {}
    gid = "".join(ch for ch in str(b.get("guildId", "")) if ch.isdigit()); region = str(b.get("region", "")).strip().upper()
    if not 4 <= len(gid) <= 15: raise Err("সঠিক Guild ID দিন (শুধু সংখ্যা)")
    if region not in GLORY_REGIONS: raise Err("আগে সঠিক রিজিয়ন (সার্ভার) সিলেক্ট করুন")
    st, j = glory_call("GET", "/api/public/guild", params={"guild_id": gid, "region": region})
    if st == 200 and j.get("ok"): return jsonify(ok=True, guild=j.get("guild") or {})
    raise Err(str(j.get("error") or "গিল্ড পাওয়া যায়নি, Guild ID ও রিজিয়ন ঠিক আছে কিনা দেখুন"), 404 if st == 404 else 400)

def _glory_settle(docid):
    """Called once the glory server finished (or gave up) an order. Refunds whatever was not delivered. Safe to call many times."""
    d, ut = fget("guildGlory/" + docid)
    if not d or d.get("settled") or not d.get("jobId"): return d
    st, j = glory_call("GET", "/api/public/job/" + str(d["jobId"]), timeout=20)
    job = j.get("job") if st == 200 else None
    if st == 404: job = {"status": "failed", "joined": 0, "error": "order not found on glory server"}
    if not job or job.get("status") not in ("running", "failed", "finished"): return d
    bots, price, uid = to_i(d.get("bots")), to_i(d.get("price")), d.get("userId")
    joined = to_i(job.get("joined")) if job.get("status") != "failed" else 0
    unit = to_i(d.get("unitPrice")); refund = max(0, price - unit * joined) if price > 0 else 0
    if job.get("status") == "failed": refund = price
    writes = [w_set("guildGlory/" + docid, {"settled": True, "refunded": refund, "joined": joined, "finalStatus": "success" if joined > 0 else "failed",
                                           "error": str(job.get("error") or "")[:200]}, now=["doneAt"], pre={"updateTime": ut})]
    if refund > 0: writes.append(w_set("users/" + uid, {}, mask=[], inc={"balance": refund}))
    if d.get("freeCounted") and joined == 0: writes.append(w_set(d["freeCounted"], {}, mask=[], inc={"count": -1}))
    if joined > 0:
        g = job.get("guild") or {}
        writes.append(w_set("orders/" + uuid.uuid4().hex, {"userId": uid, "userName": d.get("userName", ""), "userEmail": d.get("userEmail", ""), "gameName": "🛡️ Guild Glory", "gameId": "guildglory",
                              "playerId": str(d.get("guildId")), "server": str(d.get("region")), "productName": f"{joined} Bots • {str(g.get('name') or '')[:20]}", "price": max(price - refund, 0),
                              "originalPrice": price, "promoCode": "", "type": "guildglory", "status": "completed", "orderNo": docid[-10:].upper(), "payWith": "balance"}, pre={"exists": False}, now=["date"]))
    try: commit(writes)
    except Exception as ex:
        print("GLORY settle conflict/error", docid, ex); return d      # another thread settled it first
    if joined > 0:
        try: tg_notify("🛡️", "GUILD GLORY ORDER", uid, {"name": d.get("userName", ""), "email": d.get("userEmail", "")}, [("🆔", "Guild ID", d.get("guildId")), ("🌍", "Server", d.get("region")), ("🤖", "Bots", f"{joined}/{bots}"), ("💰", "Price", f"৳{max(price - refund, 0)}")])
        except Exception: pass
    return fget("guildGlory/" + docid)[0]

def _glory_watch(docid):
    for _ in range(240):                       # up to ~20 minutes
        time.sleep(5)
        try:
            d = _glory_settle(docid)
            if d and d.get("settled"): return
        except Exception as ex: print("GLORY watch error", ex)

@app.post("/guildGlory")
@app.post("/api/guildGlory")
def guild_glory_order():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    gid = "".join(ch for ch in str(b.get("guildId", "")) if ch.isdigit())
    if not 4 <= len(gid) <= 15: raise Err("সঠিক Guild ID দিন (শুধু সংখ্যা)")
    region = str(b.get("region", "")).strip().upper()
    if region not in GLORY_REGIONS: raise Err("আগে সঠিক রিজিয়ন (সার্ভার) সিলেক্ট করুন")
    bots = to_i(b.get("bots"), 0)
    rid = re.sub(r"[^A-Za-z0-9]", "", str(b.get("reqId") or ""))[:40] or uuid.uuid4().hex
    docid = f"{uid[:28]}_{rid}"
    prev, _ = fget("guildGlory/" + docid)
    if prev: return jsonify(ok=True, jobId=prev.get("jobId"), docId=docid, duplicate=True)       # same request twice -> never charge twice
    g, _ = fget("settings/general"); g = g or {}; c = glory_cfg(g)
    if not c["enabled"]: raise Err(c["offMsg"] or "🛡️ Guild Glory সার্ভিস সাময়িকভাবে বন্ধ আছে। পরে আবার চেষ্টা করুন।", 403)
    allowed = [o for o in c["options"] if not (c["mode"] == "free" and o > c["freeBots"])] or ([c["freeBots"]] if c["mode"] == "free" else [])
    if bots not in allowed and not (c["mode"] == "free" and bots == c["freeBots"]):
        raise Err("এই বট সংখ্যাটি এখন চালু নেই, তালিকা থেকে সিলেক্ট করুন" if c["mode"] == "paid" else f"ফ্রি মোডে শুধু {c['freeBots']}টা বট নেওয়া যায়")
    u, ut = fget("users/" + uid)
    if not u: raise Err("User not found", 404)
    if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
    unit = glory_unit_price(u, g, c); price = unit * bots
    if price > 0: check_pay(u, price)
    # make sure the guild really exists BEFORE anything is charged
    st, j = glory_call("GET", "/api/public/guild", params={"guild_id": gid, "region": region})
    if not (st == 200 and j.get("ok")): raise Err(str(j.get("error") or "গিল্ড পাওয়া যায়নি, Guild ID ও রিজিয়ন ঠিক আছে কিনা দেখুন"), 404)
    key = gid + region
    with _glory_lock:
        if key in _glory_busy: raise Err("এই গিল্ডের একটি রিকোয়েস্ট এখন প্রসেস হচ্ছে, একটু অপেক্ষা করুন", 409)
        _glory_busy.add(key)
    try:
        free_doc, free_ut, free_cnt = "", None, 0
        if c["mode"] == "free" and c["freeDaily"] > 0:            # optional daily limit for free orders
            free_doc = f"gloryFree/{uid[:28]}_{datetime.now(timezone(timedelta(hours=6))).strftime('%Y%m%d')}"
            fd, free_ut = fget(free_doc); free_cnt = to_i((fd or {}).get("count"))
            if free_cnt >= c["freeDaily"]: raise Err(f"আজকের ফ্রি লিমিট শেষ (দিনে {c['freeDaily']} বার)। কাল আবার চেষ্টা করুন।", 429)
        rec = {"userId": uid, "userName": u.get("name", ""), "userEmail": u.get("email", ""), "guildId": gid, "region": region, "bots": bots,
               "mode": c["mode"], "unitPrice": unit, "price": price, "status": "pending", "jobId": "", "freeCounted": free_doc}
        writes = [w_set("guildGlory/" + docid, rec, pre={"exists": False}, now=["createdAt"])]
        if price > 0: writes.append(w_set("users/" + uid, {}, mask=[], inc={"balance": -price}, pre={"updateTime": ut}))
        if free_doc: writes.append(w_set(free_doc, {"userId": uid}, inc={"count": 1}, pre=({"updateTime": free_ut} if free_ut else {"exists": False})))
        try: commit(writes)
        except Err: raise Err("রিকোয়েস্ট শুরু করা যায়নি, আবার চেষ্টা করুন", 409)
        try:
            st, j = glory_call("POST", "/api/public/order", payload={"guild_id": gid, "region": region, "bots": bots, "ref": docid.replace("_", "")[-60:]})
            if not (st == 200 and j.get("ok") and j.get("job_id")): raise Err(str(j.get("error") or "অর্ডার শুরু করা যায়নি"), 502)
        except Exception as ex:
            msg = ex.m if isinstance(ex, Err) else "অর্ডার শুরু করা যায়নি"
            if price > 0: refund_balance(uid, price)
            wr = [w_set("guildGlory/" + docid, {"status": "failed", "settled": True, "refunded": price, "error": str(msg)[:200]}, now=["doneAt"])]
            if free_doc: wr.append(w_set(free_doc, {}, mask=[], inc={"count": -1}))
            try: commit(wr)
            except Exception as e2: print("GLORY unstick error", e2)
            raise Err(str(msg) + (" — টাকা ফেরত দেওয়া হয়েছে" if price > 0 else ""), getattr(ex, "c", 500))
        commit([w_set("guildGlory/" + docid, {"status": "started", "jobId": str(j["job_id"])})])
        threading.Thread(target=_glory_watch, args=(docid,), daemon=True).start()
        return jsonify(ok=True, jobId=str(j["job_id"]), docId=docid, price=price, mode=c["mode"])
    finally:
        with _glory_lock: _glory_busy.discard(key)

@app.post("/guildGloryStatus")
@app.post("/api/guildGloryStatus")
def guild_glory_status():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    docid = re.sub(r"[^A-Za-z0-9_]", "", str(b.get("docId") or ""))[:80]
    if not docid.startswith(uid[:28] + "_"): raise Err("Not found", 404)
    d, _ = fget("guildGlory/" + docid)
    if not d: return jsonify(found=False)
    if not d.get("jobId"): return jsonify(found=True, job={"status": "failed" if d.get("settled") else "queued", "stage": "queued", "error": d.get("error", "")}, price=to_i(d.get("price")), refunded=to_i(d.get("refunded")))
    st, j = glory_call("GET", "/api/public/job/" + str(d["jobId"]), timeout=20)
    job = j.get("job") if st == 200 else None
    if st == 404: job = {"status": "failed", "stage": "failed", "joined": 0, "error": "order not found on glory server"}
    if not job: return jsonify(found=True, job={"status": "processing", "stage": "queued"}, price=to_i(d.get("price")), unreachable=True)
    if job.get("status") in ("running", "failed", "finished") and not d.get("settled"):
        try: d = _glory_settle(docid) or d
        except Exception as ex: print("GLORY settle(status) error", ex)
    return jsonify(found=True, job=job, price=to_i(d.get("price")), refunded=to_i(d.get("refunded")), settled=bool(d.get("settled")), mode=d.get("mode"))

# External API system (resellers only): personal API keys -> Like API for websites / Telegram bots
#   GET|POST /api/v1/external/like       ?apikey=KEY&uid=PLAYER_UID&region=BD   (charged only when likes are delivered)
#   GET|POST /api/v1/external/user-info  ?apikey=KEY&uid=PLAYER_UID&region=BD   (free player lookup)
#   GET|POST /api/v1/external/balance    ?apikey=KEY
# Key can also be sent in the X-API-KEY header. Optional request_id makes retries safe (never charged twice).
# ======================================================================
KEY_ALPHA = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
KEY_OK = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
NAME_OK = re.compile(r"^[A-Za-z0-9_]{3,32}$")
API_ROLES = ("reseller", "admin")
API_MIN_DEPOSIT = 100   # default; admin can change it from the panel (settings/general.apiMinDeposit)

def api_min_dep(g):
    v = to_i((g or {}).get("apiMinDeposit"), API_MIN_DEPOSIT)
    return v if v >= 0 else API_MIN_DEPOSIT
def api_ok(u, g):
    """API unlock rule: REAL paid deposits only (users.totalDeposit never includes promo/bonus money), or reseller/admin role."""
    return (u.get("role") or "user") in API_ROLES or to_i(u.get("totalDeposit")) >= api_min_dep(g)
def bd_day(): return datetime.now(timezone(timedelta(hours=6))).strftime("%Y-%m-%d")
def iso_ok(v):
    v = str(v or "").strip()
    if not v: return ""
    try: datetime.fromisoformat(v.replace("Z", "+00:00"))
    except Exception: raise Err("Expiry date সঠিক নয়")
    return v
_LOCKS = [threading.Lock() for _ in range(256)]          # striped per-user locks (bounded memory)
def user_lock(uid): return _LOCKS[int(hashlib.md5(str(uid).encode()).hexdigest(), 16) % len(_LOCKS)]
_rl, _rl_lock = {}, threading.Lock()

def rate_ok(key, limit):
    now = time.time()
    with _rl_lock:
        dq = _rl.get(key)
        if dq is None: dq = _rl[key] = deque()
        while dq and now - dq[0] > 60: dq.popleft()
        if len(dq) >= limit: return False, max(1, int(60 - (now - dq[0])) + 1)
        dq.append(now)
        if len(_rl) > 20000:
            for k in [k for k, v in _rl.items() if not v or now - v[-1] > 60][:10000]: _rl.pop(k, None)
    return True, 0

class ApiErr(Exception):
    def __init__(s, code, bn, en, http=400, status="failed", **extra): s.code, s.bn, s.en, s.http, s.status, s.extra = code, bn, en, http, status, extra
@app.errorhandler(ApiErr)
def _api_err(e):
    body = {"ok": False, "status": e.status, "error": e.code, "message": e.bn, "message_en": e.en}; body.update(e.extra)
    r = jsonify(body); r.status_code = e.http
    if e.http == 429 and e.extra.get("retry_after"): r.headers["Retry-After"] = str(e.extra["retry_after"])
    return r

def ext_guard(fn):
    @functools.wraps(fn)
    def w(*a, **k):
        try: return fn(*a, **k)
        except ApiErr: raise
        except Err as e: raise ApiErr("error", e.m, e.m, e.c)
        except requests.exceptions.RequestException as ex:
            print("EXT network error", type(ex).__name__, ex)
            raise ApiErr("network_error", "নেটওয়ার্ক সমস্যা, আবার চেষ্টা করুন।", "Network error, please try again.", 503)
        except Exception as ex:
            print("EXT unexpected", type(ex).__name__, ex)
            raise ApiErr("server_error", "সার্ভারে সমস্যা হয়েছে, আবার চেষ্টা করুন।", "Internal server error, please try again.", 500)
    return w

def to_i(x, d=0):
    try: return int(float(str(x).replace(",", "").strip()))
    except Exception: return d
def new_api_key(): return "MHX-" + "".join(secrets.choice(KEY_ALPHA) for _ in range(20))
def pin_of(uid, u): return str((u or {}).get("supportPin") or str(uid)[:6]).upper()
def api_off(g):
    en = g.get("apiEnabled", True)
    return en is False or str(en).strip().lower() in ("off", "false", "0")
def clean_key(k):
    k = str(k or "").strip()
    if not KEY_OK.match(k): raise Err("API key সঠিক নয়", 400)
    return k
def opt_int(b, name, hi):
    v = b.get(name)
    if v in (None, ""): return None
    try: n = int(float(v))
    except Exception: raise Err(f"{name} সঠিক সংখ্যা দিন")
    if n < 0 or n > hi: raise Err(f"{name} ০ থেকে {hi} এর মধ্যে দিন")
    return n

def api_price_for(u, g, kd=None):
    """Price charged per delivered like through the API: key custom price > global API price > reseller/daily-like price."""
    if (kd or {}).get("free"): return 0
    cp = (kd or {}).get("customPrice")
    if cp not in (None, ""): return max(to_i(cp), 0)
    if g.get("apiLikePrice") not in (None, ""): return max(to_i(g["apiLikePrice"]), 0)
    return like_price_for(u, g)
def rate_limit_of(kd, g): return max(1, to_i(kd.get("rateLimit") or g.get("apiRateLimit") or 30, 30))

# ---- API wallet: main balance only ----
def api_pick_wallet(u, g, price):
    """-> (field, amount) or (None, 0)"""
    if price <= 0: return "balance", 0
    if to_i(u.get("balance")) >= price: return "balance", price
    return None, 0

# ---------- request helpers ----------
def req_param(*names):
    for n in names:
        v = request.args.get(n)
        if v not in (None, ""): return str(v).strip()
    j = request.get_json(silent=True)
    if isinstance(j, dict):
        for n in names:
            v = j.get(n)
            if v not in (None, ""): return str(v).strip()
    for n in names:
        v = request.form.get(n)
        if v not in (None, ""): return str(v).strip()
    return ""

def req_api_key():
    k = (request.headers.get("X-API-KEY") or request.headers.get("X-Api-Key") or "").strip() or req_param("apikey", "api_key", "key")
    if not k: raise ApiErr("missing_api_key", "API key দিন (?apikey=... অথবা X-API-KEY হেডার)", "API key is required (?apikey=... or X-API-KEY header)", 401)
    if not KEY_OK.match(k): raise ApiErr("invalid_api_key", "API key সঠিক নয়", "Invalid API key", 401)
    return k

def ext_auth():
    key = req_api_key()
    g, _ = fget("settings/general"); g = g or {}
    if api_off(g): raise ApiErr("api_disabled", "API সিস্টেম সাময়িকভাবে বন্ধ আছে", "The API system is temporarily disabled", 503)
    kd, _ = fget("apiKeys/" + key)
    if not kd: raise ApiErr("invalid_api_key", "API key সঠিক নয়", "Invalid API key", 401)
    owner = str(kd.get("userId") or "")
    if not owner: raise ApiErr("key_not_assigned", "এই API key কারো নামে অ্যাক্টিভ করা নেই", "This API key is not assigned to an account", 403)
    if kd.get("adminDisabled") or (kd.get("status") or "active") != "active":
        raise ApiErr("key_disabled", "এই API key বন্ধ (revoked/disabled) আছে", "This API key is revoked or disabled", 403)
    exp = str(kd.get("expiresAt") or "")
    if exp:
        try:
            if datetime.fromisoformat(exp.replace("Z", "+00:00")) < datetime.now(timezone.utc): raise ApiErr("key_expired", "এই API key এর মেয়াদ শেষ", "This API key has expired", 403)
        except ValueError: pass
    ml = to_i(kd.get("maxLikes"))
    if ml and to_i(kd.get("likesGiven")) >= ml: raise ApiErr("limit_reached", "এই API key এর মোট লাইক লিমিট শেষ", "This API key reached its total likes limit", 403)
    ok, wait = rate_ok(key, rate_limit_of(kd, g))
    if not ok: raise ApiErr("rate_limited", f"অনেক বেশি রিকোয়েস্ট। {wait} সেকেন্ড পরে আবার চেষ্টা করুন।", f"Rate limit exceeded. Retry in {wait}s.", 429, retry_after=wait)
    u, ut = fget("users/" + owner)
    if not u: raise ApiErr("user_not_found", "অ্যাকাউন্ট পাওয়া যায়নি", "Account not found", 404)
    if u.get("banned"): raise ApiErr("account_banned", "অ্যাকাউন্টটি ব্যান করা আছে", "Account is banned", 403)
    if not api_ok(u, g) and kd.get("source") != "admin": raise ApiErr("deposit_required", f"API ব্যবহার করতে ৳{api_min_dep(g)} ডিপোজিট করুন", f"Deposit {api_min_dep(g)} to use the API", 403)
    return key, kd, owner, u, ut, g

def ext_uid_region():
    player = "".join(ch for ch in req_param("uid", "player_uid") if ch.isdigit())
    if not 5 <= len(player) <= 15: raise ApiErr("invalid_uid", "সঠিক UID দিন (৫-১৫ সংখ্যা)", "Invalid UID (5-15 digits required)", 400)
    region = (req_param("region", "server") or "BD").upper()
    if region not in LIKE_REGIONS: raise ApiErr("invalid_region", "রিজিয়ন সঠিক নয়", "Invalid region", 400, regions=list(LIKE_REGIONS))
    return player, region

def api_record(key, kd, owner, player, region, result, given=0, price=0, pkg="", kind="like"):
    """Usage counters + per-key log + admin log. Never raises (a logging problem must not affect the customer)."""
    lid = uuid.uuid4().hex
    log = {"keyId": key, "userId": owner, "label": str(kd.get("label") or ""), "uid": player, "region": region, "result": result, "kind": kind,
           "likes": int(given or 0), "price": int(price or 0), "package": pkg}
    logs = [w_set(f"apiKeys/{key}/logs/{lid}", log, pre={"exists": False}, now=["createdAt"]), w_set(f"apiLogs/{lid}", log, pre={"exists": False}, now=["createdAt"])]
    if kind == "like":
        inc = {"requests": 1}
        sd = {}
        if result == "success":
            inc.update(likesGiven=int(given or 0), spent=int(price or 0))
            today = bd_day(); prev = to_i(kd.get("dayLikes")) if kd.get("dayKey") == today else 0
            sd = {"dayKey": today, "dayLikes": prev + int(given or 0)}
        stat = w_set("apiKeys/" + key, sd, inc=inc, now=["lastUsedAt"], pre={"exists": True})
    elif kind == "autolike":
        inc = {"requests": 1}
        if result == "success": inc.update(spent=int(price or 0))
        stat = w_set("apiKeys/" + key, {}, mask=[], inc=inc, now=["lastUsedAt"], pre={"exists": True})
    else:
        stat = w_set("apiKeys/" + key, {}, mask=[], now=["lastUsedAt"], pre={"exists": True})
    try: commit([stat] + logs)
    except Exception:
        try: commit(logs)
        except Exception as ex: print("API LOG error", ex)

# ---------- public endpoints ----------
@app.route("/api/v1/external/balance", methods=["GET", "POST"])
@app.route("/v1/external/balance", methods=["GET", "POST"])
@ext_guard
def ext_balance():
    key, kd, owner, u, ut, g = ext_auth()
    return jsonify(ok=True, status="success", balance=to_i(u.get("balance")), price_per_like=api_price_for(u, g, kd), role=u.get("role") or "user",
                   key_label=str(kd.get("label") or ""), rate_limit_per_minute=rate_limit_of(kd, g))

def find_nick(o):
    if isinstance(o, dict):
        for k, v in o.items():
            if str(k).lower() in _NICK_KEYS and isinstance(v, str) and v: return v
        for v in o.values():
            x = find_nick(v)
            if x: return x
    return None

@app.route("/api/v1/external/user-info", methods=["GET", "POST"])
@app.route("/v1/external/user-info", methods=["GET", "POST"])
@ext_guard
def ext_user_info():
    key, kd, owner, u, ut, g = ext_auth()
    player, region = ext_uid_region()
    try: j, code = fetch_player_info(player, region)
    except Exception as ex: raise ApiErr("info_unreachable", "ইনফো সার্ভারে পৌঁছানো যায়নি", "Could not reach the info server (" + type(ex).__name__ + ")", 502)
    if j is None or not has_player(j):
        api_record(key, kd, owner, player, region, "failed", kind="info")
        raise ApiErr("player_not_found", "প্লেয়ারের তথ্য পাওয়া যায়নি। UID ও রিজিয়ন চেক করুন।", "Player not found. Check the UID and region.", 404)
    api_record(key, kd, owner, player, region, "info", kind="info")
    return jsonify(ok=True, status="success", uid=player, region=region, player_name=find_nick(j) or "N/A", data=j)

@app.route("/api/v1/external/like", methods=["GET", "POST"])
@app.route("/v1/external/like", methods=["GET", "POST"])
@ext_guard
def ext_like():
    key, kd, owner, u, ut, g = ext_auth()
    player, region = ext_uid_region()
    if like_off(g): raise ApiErr("service_off", str(g.get("dailyLikeOffMsg") or "🔥 ডেইলি লাইক সার্ভিস সাময়িকভাবে বন্ধ আছে।"), "The like service is temporarily off", 503)
    rid = re.sub(r"[^A-Za-z0-9_-]", "", req_param("request_id", "reqid"))[:40]
    docid = hashlib.sha1(f"{key}:{rid}".encode()).hexdigest() if rid else uuid.uuid4().hex
    if rid:   # the same request_id sent again (retry / double click) -> return the stored answer, never charge twice
        prev, _ = fget("apiOrders/" + docid)
        if prev:
            if prev.get("status") == "pending": raise ApiErr("in_progress", "এই রিকোয়েস্ট প্রসেস হচ্ছে, একটু অপেক্ষা করুন", "This request is still processing", 409)
            try: return jsonify(json.loads(prev.get("resp") or "{}"))
            except Exception: raise ApiErr("duplicate_request", "রিকোয়েস্টটি আগেই প্রসেস হয়েছে", "This request was already processed", 409)
    dl = to_i(kd.get("dailyLimit"))
    if dl and kd.get("dayKey") == bd_day() and to_i(kd.get("dayLikes")) >= dl:
        raise ApiErr("daily_limit", "এই API key এর আজকের ডেইলি লিমিট শেষ", "Daily limit reached for this API key", 429, daily_limit=dl)
    price = api_price_for(u, g, kd); amount_label = to_i(g.get("dailyLikeAmount"), 220) or 220
    bkey = player + region
    with _like_lock:
        if bkey in _like_busy: raise ApiErr("uid_busy", "এই UID-তে একটি রিকোয়েস্ট এখন প্রসেস হচ্ছে, একটু অপেক্ষা করুন", "A request for this UID is already being processed", 409)
        _like_busy.add(bkey)
    try:
        # 1) reserve the price atomically (per-user lock + updateTime precondition => no double-spend, balance can never go negative)
        bal_before = None
        for attempt in range(5):
            with user_lock(owner):
                u2, ut2 = fget("users/" + owner)
                if not u2 or u2.get("banned"): raise ApiErr("account_banned", "অ্যাকাউন্টটি ব্যান করা আছে", "Account is banned", 403)
                if not api_ok(u2, g) and kd.get("source") != "admin": raise ApiErr("deposit_required", f"API ব্যবহার করতে ৳{api_min_dep(g)} ডিপোজিট করুন", f"Deposit {api_min_dep(g)} to use the API", 403)
                bal = to_i(u2.get("balance"))
                pf, charge = api_pick_wallet(u2, g, price)
                if pf is None: raise ApiErr("insufficient_balance", "আপনার পর্যাপ্ত ব্যালেন্স নেই। ওয়ালেটে টাকা অ্যাড করুন।", "Insufficient wallet balance", 402, balance=bal, price=price)
                try:
                    commit([w_set("users/" + owner, {}, mask=[], inc={pf: -charge}, pre={"updateTime": ut2}),
                            w_set("apiOrders/" + docid, {"userId": owner, "keyId": key, "playerId": player, "region": region, "price": charge, "payWith": pf, "status": "pending"}, pre={"exists": False}, now=["createdAt"])])
                    bal_before = to_i(u2.get(pf)); break
                except Err: pass
            time.sleep(0.2 * (attempt + 1))
        price = charge   # from here on "price" = what was actually taken
        if bal_before is None: raise ApiErr("busy_retry", "রিকোয়েস্ট শুরু করা যায়নি, আবার চেষ্টা করুন", "Could not start the request, please retry", 409)
        settled = False
        try:
            # 2) call the like provider
            res = call_like_api(player, region)
            try: st = int(res.get("status"))
            except Exception: st = 0
            name = str(res.get("PlayerNickname") or "N/A"); shown_uid = str(res.get("UID") or player); shown_region = str(res.get("Region") or region); now = bd_now()
            if st == 1:
                given = to_i(res.get("LikesGivenByAPI"), 0)
                before, after = to_int(res.get("LikesBeforeCommand")), to_int(res.get("LikesAfterCommand"))
                remaining = bal_before - price
                try:
                    _uu = fget("users/" + owner)[0] or {}; remaining = to_i(_uu.get("balance"), remaining)
                except Exception: pass
                out = {"ok": True, "status": "success", "uid": shown_uid, "region": shown_region, "player_name": name, "likes_given": given, "before_likes": before, "after_likes": after,
                       "price_charged": price, "paid_from": pf, "remaining_balance": remaining, "order_no": str(res.get("OrderNo") or "N/A"), "package": str(res.get("Package") or f"{amount_label} LIKE INSTANT"),
                       "time": now, "message": "লাইক সফলভাবে পাঠানো হয়েছে।", "message_en": "Likes delivered successfully."}
                pname = f"{given} Likes" + (f" • {before} → {after}" if before is not None and after is not None else "")
                try:
                    commit([w_set("apiOrders/" + docid, {"status": "success", "resp": json.dumps(out, ensure_ascii=False), "player": name, "given": given}, now=["doneAt"]),
                            w_set("orders/" + uuid.uuid4().hex, {"userId": owner, "userName": u.get("name", ""), "userEmail": u.get("email", ""), "gameName": "🔗 API Like", "gameId": "apilike", "playerId": shown_uid, "server": shown_region,
                                  "productName": pname, "price": price, "originalPrice": price, "promoCode": "", "type": "apilike", "status": "completed", "orderNo": out["order_no"]}, pre={"exists": False}, now=["date"])])
                except Exception as ex: print("API LIKE record error", ex)
                api_record(key, kd, owner, player, region, "success", given=given, price=price, pkg=pname)
                tg_notify("🔗", "API LIKE SENT", owner, u, [("🎮", "UID", shown_uid), ("🌍", "Server", shown_region), ("📈", "Likes given", given), ("💰", "Price", f"৳{price}")])
                settled = True
                return jsonify(out)
            # not delivered -> give the reserved money back (customer pays nothing)
            refund_balance(owner, price, pf); settled = True
            remaining = bal_before
            try: remaining = to_i((fget("users/" + owner)[0] or {}).get("balance"), bal_before)
            except Exception: pass
            if st == 2:
                out = {"ok": False, "status": "already", "uid": shown_uid, "region": shown_region, "player_name": name, "likes_given": 0, "price_charged": 0, "refunded": price, "remaining_balance": remaining, "time": now,
                       "message": str(res.get("message") or "এই UID আজকের লাইক ইতিমধ্যে পেয়ে গেছে। কোনো টাকা কাটা হয়নি।"), "message_en": "This UID already received today's likes. You were not charged."}
                code = 200
            else:
                err = str(res.get("error") or "unknown")
                out = {"ok": False, "status": "failed", "error": err, "uid": player, "region": region, "likes_given": 0, "price_charged": 0, "refunded": price, "remaining_balance": remaining, "time": now,
                       "message": str(res.get("message") or "লাইক পাঠানো যায়নি। কোনো টাকা কাটা হয়নি।"), "message_en": "Likes could not be delivered. You were not charged."}
                code = 400 if err == "bad_request" else (429 if err in ("rate_limited", "order_in_progress") else 502)
            try: commit([w_set("apiOrders/" + docid, {"status": out["status"], "resp": json.dumps(out, ensure_ascii=False), "player": name}, now=["doneAt"])])
            except Exception as ex: print("API LIKE record error", ex)
            api_record(key, kd, owner, player, region, out["status"], price=0)
            resp = jsonify(out); resp.status_code = code
            return resp
        except (ApiErr, Err): raise
        except Exception as ex:
            print("API LIKE unexpected", type(ex).__name__, ex)
            if not settled:
                refund_balance(owner, price, pf)
                try: commit([w_set("apiOrders/" + docid, {"status": "failed", "resp": json.dumps({"ok": False, "status": "failed", "error": "server_error", "likes_given": 0, "price_charged": 0, "refunded": price, "message": "অপ্রত্যাশিত সমস্যা হয়েছে। টাকা ফেরত দেওয়া হয়েছে, আবার চেষ্টা করুন।", "message_en": "Unexpected error. Your balance was refunded, please retry."}, ensure_ascii=False)}, now=["doneAt"])])
                except Exception as e2: print("API LIKE unstick error", e2)
            raise ApiErr("server_error", "অপ্রত্যাশিত সমস্যা হয়েছে। টাকা ফেরত দেওয়া হয়েছে, আবার চেষ্টা করুন।", "Unexpected error. Your balance was refunded, please retry.", 500, refunded=price)
    finally:
        with _like_lock: _like_busy.discard(bkey)

# ---------- external API: Auto Like subscription ----------
@app.route("/api/v1/external/autolike-info", methods=["GET", "POST"])
@app.route("/v1/external/autolike-info", methods=["GET", "POST"])
@ext_guard
def ext_autolike_info():
    key, kd, owner, u, ut, g = ext_auth()
    cfg = al_settings(); s, _ = fget("autoLikes/" + owner)
    return jsonify(ok=True, status="success", enabled=cfg["enabled"], send_time=al_send_time(cfg), balance=to_i(u.get("balance")),
                   packages=[{"package_id": p["id"], "name": p["name"], "days": p["days"], "price": al_price(p, u)} for p in cfg["packages"] if p["active"]],
                   subscription=(al_view(s, cfg) if s else None))

@app.route("/api/v1/external/autolike", methods=["GET", "POST"])
@app.route("/v1/external/autolike", methods=["GET", "POST"])
@ext_guard
def ext_autolike():
    key, kd, owner, u, ut, g = ext_auth()
    player, region = ext_uid_region()
    cfg = al_settings()
    if not cfg["enabled"]: raise ApiErr("service_off", cfg["offMsg"] or "🤖 অটো লাইক সার্ভিস সাময়িকভাবে বন্ধ আছে।", "Auto Like is temporarily off", 503)
    pid_ = req_param("package_id", "package", "packageId")
    pkg = next((p for p in cfg["packages"] if p["active"] and p["id"] == str(pid_)), None) if pid_ else None
    if not pkg: raise ApiErr("package_not_found", "প্যাকেজটি পাওয়া যায়নি (package_id দিন)", "Package not found (send package_id)", 404, packages=[p["id"] for p in cfg["packages"] if p["active"]])
    rid = re.sub(r"[^A-Za-z0-9]", "", req_param("request_id", "reqid") or "")[:40] or uuid.uuid4().hex
    pid = f"api_{hashlib.sha1((key + ':' + rid).encode()).hexdigest()[:30]}"
    prev, _ = fget("autoLikePurchases/" + pid)
    if prev: return jsonify(ok=True, status="success", already=True, message="এই রিকোয়েস্ট আগেই প্রসেস হয়েছে", message_en="This request was already processed")
    for attempt in range(5):
        with user_lock(owner):
            u2, ut2 = fget("users/" + owner)
            if not u2 or u2.get("banned"): raise ApiErr("account_banned", "অ্যাকাউন্টটি ব্যান করা আছে", "Account is banned", 403)
            if not api_ok(u2, g) and kd.get("source") != "admin": raise ApiErr("deposit_required", f"API ব্যবহার করতে ৳{api_min_dep(g)} ডিপোজিট করুন", f"Deposit {api_min_dep(g)} to use the API", 403)
            s, sut = fget("autoLikes/" + owner)
            active = al_active(s)
            if active and (str(s.get("playerId")) != player or str(s.get("region")) != region):
                raise ApiErr("other_uid_active", f"এই অ্যাকাউন্টের অটো লাইক অন্য UID তে চালু আছে ({s.get('playerId')} • {s.get('region')})", "Auto Like is already active for another UID on this account", 409)
            price = al_price(pkg, u2)
            pf, charge = api_pick_wallet(u2, g, price)
            if pf is None: raise ApiErr("insufficient_balance", "আপনার পর্যাপ্ত ব্যালেন্স নেই। ওয়ালেটে টাকা অ্যাড করুন।", "Insufficient wallet balance", 402, balance=to_i(u2.get("balance")), price=price)
            name = u2.get("name") or (u2.get("email") or "User").split("@")[0]
            writes = [w_set("users/" + owner, {}, mask=[], inc={pf: -charge}, pre={"updateTime": ut2}),
                      w_set("autoLikePurchases/" + pid, {"userId": owner, "packageId": pkg["id"], "packageName": pkg["name"], "days": pkg["days"], "price": charge, "playerId": player, "region": region, "payWith": pf, "viaApi": True}, pre={"exists": False}, now=["createdAt"])]
            if active:
                writes.append(w_set("autoLikes/" + owner, {"packageName": pkg["name"], "status": "active"}, inc={"remaining": pkg["days"], "totalDays": pkg["days"]}, pre={"updateTime": sut}))
            else:
                writes.append(w_set("autoLikes/" + owner, {"userId": owner, "userName": name, "userEmail": u2.get("email", ""), "playerId": player, "region": region, "packageId": pkg["id"], "packageName": pkg["name"],
                                                         "status": "active", "remaining": pkg["days"], "totalDays": pkg["days"], "daysSent": 0, "likesTotal": 0, "lastDay": "", "lastResult": "", "lastMsg": "",
                                                         "lastAt": "", "triesDay": "", "tries": 0, "lastTryMs": 0, "uidChanges": 0, "paid": charge},
                                    pre=({"updateTime": sut} if sut else {"exists": False}), now=["startedAt"]))
            writes.append(w_set("orders/" + uuid.uuid4().hex, {"userId": owner, "userName": name, "userEmail": u2.get("email", ""), "gameName": "🤖 Auto Like (API)", "gameId": "autolike", "playerId": player, "server": region,
                                                              "productName": f"{pkg['name']} • daily at {al_send_time(cfg)}", "price": charge, "originalPrice": pkg["price"], "promoCode": "", "type": "autolike", "status": "completed"}, pre={"exists": False}, now=["date"]))
            try:
                commit(writes)
                api_record(key, kd, owner, player, region, "success", price=charge, pkg=f"AutoLike {pkg['name']}", kind="autolike")
                tg_notify("🤖", "AUTO LIKE BOUGHT (API)", owner, u2, [("📦", "Package", pkg["name"]), ("🎮", "UID", f"{player} ({region})"), ("💰", "Price", f"৳{charge}")])
                return jsonify(ok=True, status="success", uid=player, region=region, package_id=pkg["id"], package=pkg["name"], days=pkg["days"], extended=bool(active), price_charged=charge, paid_from=pf,
                               remaining_balance=to_i(u2.get("balance")) - charge, send_time=al_send_time(cfg),
                               message="অটো লাইক সফলভাবে চালু হয়েছে।", message_en="Auto Like activated successfully.")
            except Err: pass
        time.sleep(0.2 * (attempt + 1))
    raise ApiErr("busy_retry", "রিকোয়েস্ট শুরু করা যায়নি, আবার চেষ্টা করুন", "Could not process the request, please retry", 409)

# ---------- key management: users (Firebase login) ----------
def key_view(key, kd, u, g):
    return {"key": key, "label": kd.get("label") or "", "status": kd.get("status") or "active", "adminDisabled": bool(kd.get("adminDisabled")), "source": kd.get("source") or "user",
            "createdAt": kd.get("createdAt"), "lastUsedAt": kd.get("lastUsedAt"), "requests": to_i(kd.get("requests")), "likesGiven": to_i(kd.get("likesGiven")), "spent": to_i(kd.get("spent")),
            "price": api_price_for(u, g, kd), "customPrice": kd.get("customPrice"), "rateLimit": rate_limit_of(kd, g),
            "dailyLimit": kd.get("dailyLimit"), "maxLikes": kd.get("maxLikes"), "expiresAt": kd.get("expiresAt") or "", "note": kd.get("note") or "", "free": bool(kd.get("free")),
            "todayLikes": to_i(kd.get("dayLikes")) if kd.get("dayKey") == bd_day() else 0}

def my_keys(uid):
    ks = fquery("apiKeys", "userId", uid, limit=20)
    ks.sort(key=lambda t: str(t[1].get("createdAt") or ""))
    return ks

@app.post("/apiKeyManage")
@app.post("/api/apiKeyManage")
def api_key_manage():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}; act = str(b.get("action") or "info")
    u, ut = fget("users/" + uid)
    if not u: raise Err("User not found", 404)
    g, _ = fget("settings/general"); g = g or {}
    role = u.get("role") or "user"; eligible = api_ok(u, g)
    if act == "info":
        keys = my_keys(uid); logs = []
        for k, d in keys[:5]:
            for _, l in fquery("logs", order="createdAt", desc=True, limit=30, parent="apiKeys/" + k): logs.append(l)
        logs.sort(key=lambda l: str(l.get("createdAt") or ""), reverse=True)
        enabled = not api_off(g)
        return jsonify(ok=True, eligible=eligible, enabled=enabled, role=role, banned=bool(u.get("banned")), balance=to_i(u.get("balance")), price=api_price_for(u, g),
                       totalDeposit=to_i(u.get("totalDeposit")), paidDeposit=to_i(u.get("totalDeposit")), apiMinDeposit=api_min_dep(g), apiEligible=eligible, threshold=api_min_dep(g),
                       canCreate=bool(eligible and enabled and not u.get("banned") and not any((d.get("source") or "user") == "user" for _, d in keys)),
                       keys=[key_view(k, d, u, g) for k, d in keys],
                       logs=[{"createdAt": l.get("createdAt"), "uid": l.get("uid"), "region": l.get("region"), "result": l.get("result"), "kind": l.get("kind") or "like", "likes": to_i(l.get("likes")),
                              "price": to_i(l.get("price")), "package": l.get("package") or "", "label": l.get("label") or ""} for l in logs[:30]])
    if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
    if act == "create":
        if not eligible: raise Err(f"API আনলক করতে আসল ডিপোজিট ৳{api_min_dep(g)} করুন (শুধু আসল ডিপোজিট গণ্য)", 403)
        if api_off(g): raise Err("API সিস্টেম সাময়িকভাবে বন্ধ আছে", 403)
        name = str(b.get("name") or "").strip().lstrip("@")
        if not NAME_OK.match(name): raise Err("আপনার টেলিগ্রাম ইউজারনেম লিখুন (৩-৩২ অক্ষর: ইংরেজি অক্ষর, সংখ্যা ও _)")
        busy = Err("আপনার আগে থেকেই একটি API key আছে। নতুন বানাতে আগে ওটি ডিলিট করুন।", 409)
        if fget("apiKeyOwners/" + uid)[0]: raise busy
        for _ in range(3):
            key = new_api_key()
            try:
                commit([w_set("apiKeys/" + key, {"userId": uid, "userEmail": u.get("email", ""), "userName": u.get("name", ""), "supportPin": pin_of(uid, u), "label": name, "status": "active", "adminDisabled": False,
                                              "source": "user", "customPrice": None, "rateLimit": None, "requests": 0, "likesGiven": 0, "spent": 0}, pre={"exists": False}, now=["createdAt"]),
                        w_set("apiKeyOwners/" + uid, {"key": key, "userId": uid}, pre={"exists": False}, now=["createdAt"])])
                return jsonify(ok=True, key=key)
            except Err:
                if fget("apiKeyOwners/" + uid)[0]: raise busy
        raise Err("key তৈরি করা যায়নি, আবার চেষ্টা করুন", 500)
    if act in ("revoke", "restore", "delete"):
        key = clean_key(b.get("key")); kd, _ = fget("apiKeys/" + key)
        if not kd or kd.get("userId") != uid: raise Err("API key পাওয়া যায়নি", 404)
        if act == "revoke": commit([w_set("apiKeys/" + key, {"status": "revoked"}, pre={"exists": True})])
        elif act == "restore":
            if kd.get("adminDisabled"): raise Err("এই key টি অ্যাডমিন বন্ধ করেছেন, সাপোর্টে যোগাযোগ করুন", 403)
            commit([w_set("apiKeys/" + key, {"status": "active"}, pre={"exists": True})])
        else:
            if kd.get("adminDisabled") or (kd.get("source") or "user") != "user": raise Err("এই key টি আপনি ডিলিট করতে পারবেন না, সাপোর্টে যোগাযোগ করুন", 403)
            ow, _ = fget("apiKeyOwners/" + uid)
            commit([w_del("apiKeys/" + key)] + ([w_del("apiKeyOwners/" + uid)] if ow and ow.get("key") == key else []))
        return jsonify(ok=True)
    raise Err("Unknown action")

# ---------- key management: admin panel ----------
def find_user(ident):
    ident = str(ident or "").strip()
    if not ident: raise Err("ইউজার খুঁজে পাওয়া যায়নি", 404)
    if "@" in ident:
        for e in dict.fromkeys([ident, ident.lower()]):
            r = fquery("users", "email", e, limit=1)
            if r: return r[0]
    else:
        if len(ident) >= 20 and re.match(r"^[A-Za-z0-9]+$", ident):
            u, _ = fget("users/" + ident)
            if u: return ident, u
        pin = ident.upper()
        for e in dict.fromkeys([pin, ident]):
            r = fquery("users", "supportPin", e, limit=1)
            if r: return r[0]
        for id_, f in flist("users", mask=["supportPin"]):
            if str(f.get("supportPin") or id_[:6]).upper() == pin:
                u, _ = fget("users/" + id_)
                if u: return id_, u
    raise Err("এই Support PIN / ইমেইল / UID এর কোনো ইউজার পাওয়া যায়নি", 404)

@app.post("/adminApiKeys")
@app.post("/api/adminApiKeys")
def admin_api_keys():
    uid, _ = auth_user(); me, _ = fget("users/" + uid)
    if not me or me.get("role") != "admin": raise Err("Admin only", 403)
    pm = me.get("perms")
    if isinstance(pm, dict) and not pm.get("settings"): raise Err("এই কাজের পারমিশন নেই", 403)
    b = request.get_json(silent=True) or {}; act = str(b.get("action") or "list")
    g, _ = fget("settings/general"); g = g or {}
    if act == "list":
        rows = flist("apiKeys", limit=2000)
        uids = sorted({str(f.get("userId")) for _, f in rows if f.get("userId")})
        def one(x):
            try: return x, fget("users/" + x)[0]
            except Exception: return x, None
        with ThreadPoolExecutor(max_workers=8) as ex: users = dict(ex.map(one, uids))
        keys = []
        for k, f in rows:
            o = str(f.get("userId") or ""); uu = users.get(o) if o else None
            holder = {"userId": o, "name": uu.get("name") or f.get("userName") or "", "email": uu.get("email") or f.get("userEmail") or "", "pin": pin_of(o, uu), "role": uu.get("role") or "user",
                      "balance": to_i(uu.get("balance")), "banned": bool(uu.get("banned"))} if uu else ({"userId": o, "name": f.get("userName") or "", "email": f.get("userEmail") or "", "pin": f.get("supportPin") or "", "role": "?", "balance": 0, "banned": False, "missing": True} if o else None)
            v = key_view(k, f, uu or {}, g); v["holder"] = holder; keys.append(v)
        keys.sort(key=lambda v: str(v.get("createdAt") or ""), reverse=True)
        logs = [{"createdAt": l.get("createdAt"), "keyId": l.get("keyId"), "label": l.get("label") or "", "userId": l.get("userId"), "uid": l.get("uid"), "region": l.get("region"), "result": l.get("result"),
                 "kind": l.get("kind") or "like", "likes": to_i(l.get("likes")), "price": to_i(l.get("price"))} for _, l in fquery("apiLogs", order="createdAt", desc=True, limit=100)]
        return jsonify(ok=True, keys=keys, logs=logs, settings={"apiEnabled": not api_off(g), "apiLikePrice": g.get("apiLikePrice"), "apiRateLimit": g.get("apiRateLimit"), "apiMinDeposit": api_min_dep(g)},
                       stats={"keys": len(keys), "active": sum(1 for v in keys if v["status"] == "active" and not v["adminDisabled"]), "requests": sum(v["requests"] for v in keys), "spent": sum(v["spent"] for v in keys)})
    if act == "generate":
        count = max(1, min(50, to_i(b.get("count"), 1))); label = str(b.get("label") or "").strip()[:40]
        cp, rl = opt_int(b, "customPrice", 100000), opt_int(b, "rateLimit", 10000)
        dl, ml = opt_int(b, "dailyLimit", 10000000), opt_int(b, "maxLikes", 100000000)
        exp = iso_ok(b.get("expiresAt")); note = str(b.get("note") or "").strip()[:120]; free = bool(b.get("free"))
        ck = str(b.get("customKey") or "").strip()
        if ck:
            if count != 1: raise Err("কাস্টম key কোড দিলে একসাথে একটাই বানানো যাবে")
            if not KEY_OK.match(ck): raise Err("Key code এ শুধু English letter, number, - ও _ (৮-৬৪ অক্ষর) দিন")
            if fget("apiKeys/" + ck)[0]: raise Err("এই key code আগে থেকেই আছে, অন্য কোড দিন", 409)
        ident = str(b.get("ident") or "").strip(); owner, ou = ("", None)
        if ident: owner, ou = find_user(ident)
        made, writes = [], []
        for i in range(count):
            key = ck or new_api_key(); made.append(key)
            writes.append(w_set("apiKeys/" + key, {"userId": owner, "userEmail": (ou or {}).get("email", ""), "userName": (ou or {}).get("name", ""), "supportPin": pin_of(owner, ou) if owner else "",
                                                  "label": (label + (f" #{i + 1}" if count > 1 and label else "")) if label else "", "status": "active", "adminDisabled": False, "source": "admin",
                                                  "customPrice": cp, "rateLimit": rl or None, "dailyLimit": dl or None, "maxLikes": ml or None, "expiresAt": exp, "note": note, "free": free,
                                                  "requests": 0, "likesGiven": 0, "spent": 0}, pre={"exists": False}, now=["createdAt"]))
        commit(writes)
        return jsonify(ok=True, keys=made, holder=({"name": ou.get("name") or "", "email": ou.get("email") or "", "pin": pin_of(owner, ou), "role": ou.get("role") or "user"} if owner else None))
    key = clean_key(b.get("key")); kd, _ = fget("apiKeys/" + key)
    if not kd: raise Err("API key পাওয়া যায়নি", 404)
    if act == "update":
        data = {}
        if "label" in b: data["label"] = str(b.get("label") or "").strip()[:40]
        if "customPrice" in b: data["customPrice"] = opt_int(b, "customPrice", 100000)
        if "rateLimit" in b: data["rateLimit"] = opt_int(b, "rateLimit", 10000) or None
        if "adminDisabled" in b:
            data["adminDisabled"] = bool(b.get("adminDisabled"))
            if not data["adminDisabled"]: data["status"] = "active"
        if "dailyLimit" in b: data["dailyLimit"] = opt_int(b, "dailyLimit", 10000000) or None
        if "maxLikes" in b: data["maxLikes"] = opt_int(b, "maxLikes", 100000000) or None
        if "expiresAt" in b: data["expiresAt"] = iso_ok(b.get("expiresAt"))
        if "note" in b: data["note"] = str(b.get("note") or "").strip()[:120]
        if "free" in b: data["free"] = bool(b.get("free"))
        nk = str(b.get("newKey") or "").strip()
        if nk and nk != key:
            if not KEY_OK.match(nk): raise Err("Key code এ শুধু English letter, number, - ও _ (৮-৬৪ অক্ষর) দিন")
            if fget("apiKeys/" + nk)[0]: raise Err("এই key code আগে থেকেই আছে, অন্য কোড দিন", 409)
            full = dict(kd); full.update(data); full.pop("createdAt", None)
            writes = [w_set("apiKeys/" + nk, full, pre={"exists": False}, now=["createdAt"]), w_del("apiKeys/" + key)]
            o = str(kd.get("userId") or "")
            if o and (kd.get("source") or "user") == "user":
                ow, _ = fget("apiKeyOwners/" + o)
                if ow and ow.get("key") == key: writes.append(w_set("apiKeyOwners/" + o, {"key": nk}))
            commit(writes); return jsonify(ok=True, key=nk)
        if not data: raise Err("কিছু পরিবর্তন করা হয়নি")
        commit([w_set("apiKeys/" + key, data, pre={"exists": True})]); return jsonify(ok=True)
    if act == "assign":
        if (kd.get("source") or "user") == "user": raise Err("ইউজারের নিজের বানানো key অন্য কাউকে দেওয়া যায় না")
        ident = str(b.get("ident") or "").strip(); owner, ou = (find_user(ident) if ident else ("", None))
        commit([w_set("apiKeys/" + key, {"userId": owner, "userEmail": (ou or {}).get("email", ""), "userName": (ou or {}).get("name", ""), "supportPin": pin_of(owner, ou) if owner else ""}, pre={"exists": True})])
        return jsonify(ok=True, holder=({"name": ou.get("name") or "", "email": ou.get("email") or "", "pin": pin_of(owner, ou)} if owner else None))
    if act == "regenerate":
        new = new_api_key(); keep = ("userId", "userEmail", "userName", "supportPin", "label", "status", "adminDisabled", "source", "customPrice", "rateLimit", "dailyLimit", "maxLikes", "expiresAt", "note", "free", "requests", "likesGiven", "spent")
        writes = [w_set("apiKeys/" + new, {k: kd.get(k) for k in keep if k in kd}, pre={"exists": False}, now=["createdAt"]), w_del("apiKeys/" + key)]
        o = str(kd.get("userId") or "")
        if o and (kd.get("source") or "user") == "user":
            ow, _ = fget("apiKeyOwners/" + o)
            if ow and ow.get("key") == key: writes.append(w_set("apiKeyOwners/" + o, {"key": new}))
        commit(writes); return jsonify(ok=True, key=new)
    if act == "delete":
        writes = [w_del("apiKeys/" + key)]; o = str(kd.get("userId") or "")
        if o:
            ow, _ = fget("apiKeyOwners/" + o)
            if ow and ow.get("key") == key: writes.append(w_del("apiKeyOwners/" + o))
        commit(writes); return jsonify(ok=True)
    raise Err("Unknown action")

# ---------- App (APK) hosting: admin uploads the APK, users download it from the site ----------
APK_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "apk_files")
APK_PATH, APK_META = os.path.join(APK_DIR, "app.apk"), os.path.join(APK_DIR, "app.json")
APK_MAX = 300 * 1024 * 1024
_apk_lock = threading.Lock()

def apk_meta():
    try:
        with open(APK_META, encoding="utf-8") as f: m = json.load(f)
        if os.path.isfile(APK_PATH): return m
    except Exception: pass
    return None
def require_admin():
    uid, _ = auth_user(); me, _ = fget("users/" + uid)
    if not me or me.get("role") != "admin": raise Err("Admin only", 403)
    pm = me.get("perms")
    if isinstance(pm, dict) and not pm.get("settings"): raise Err("এই কাজের পারমিশন নেই", 403)
    return uid

@app.get("/apkInfo")
@app.get("/api/apkInfo")
def apk_info():
    m = apk_meta()
    return jsonify(ok=True, exists=bool(m), **(m or {}))

@app.post("/adminUploadApk")
@app.post("/api/adminUploadApk")
def admin_upload_apk():
    require_admin()
    f = request.files.get("file")
    if not f or not f.filename: raise Err("কোনো ফাইল সিলেক্ট করা হয়নি")
    if not f.filename.lower().endswith(".apk"): raise Err("শুধু .apk ফাইল আপলোড করা যাবে")
    os.makedirs(APK_DIR, exist_ok=True)
    tmp = os.path.join(APK_DIR, "upload-" + uuid.uuid4().hex + ".tmp")
    try:
        f.save(tmp); size = os.path.getsize(tmp)
        if size < 50 * 1024: raise Err("ফাইলটি খুব ছোট, সঠিক APK দিন")
        if size > APK_MAX: raise Err("ফাইল অনেক বড় (সর্বোচ্চ 300 MB)")
        with open(tmp, "rb") as fh:
            if fh.read(2) != b"PK": raise Err("এটি সঠিক APK ফাইল নয়")
        name = re.sub(r"[^A-Za-z0-9._-]", "_", os.path.basename(str(request.form.get("name") or f.filename)))[:60] or "app.apk"
        if not name.lower().endswith(".apk"): name += ".apk"
        meta = {"name": name, "size": size, "updatedAt": int(time.time() * 1000), "sha256": ""}
        h = hashlib.sha256()
        with open(tmp, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""): h.update(chunk)
        meta["sha256"] = h.hexdigest()
        with _apk_lock:
            os.replace(tmp, APK_PATH)
            with open(APK_META, "w", encoding="utf-8") as mf: json.dump(meta, mf)
    finally:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except Exception: pass
    return jsonify(ok=True, **meta)

@app.post("/adminDeleteApk")
@app.post("/api/adminDeleteApk")
def admin_delete_apk():
    require_admin()
    with _apk_lock:
        for p in (APK_PATH, APK_META):
            try: os.remove(p)
            except FileNotFoundError: pass
    return jsonify(ok=True)

@app.get("/download/app.apk")
@app.get("/api/download/app.apk")
def download_apk():
    from flask import send_file
    m = apk_meta()
    if not m: raise Err("অ্যাপ এখনো আপলোড করা হয়নি", 404)
    r = send_file(APK_PATH, mimetype="application/vnd.android.package-archive", as_attachment=True, download_name=m.get("name") or "app.apk", conditional=True, max_age=0)
    r.headers["Cache-Control"] = "no-cache, must-revalidate"
    return r

# ---------- Push notifications (Firebase Cloud Messaging) ----------
FCM_URL = f"https://fcm.googleapis.com/v1/projects/{PROJECT}/messages:send"
def tok_id(t): return hashlib.sha1(t.encode()).hexdigest()

@app.post("/savePushToken")
@app.post("/api/savePushToken")
def save_push_token():
    b = request.get_json(silent=True) or {}; token = str(b.get("token") or "").strip()
    if not 50 <= len(token) <= 4096: raise Err("Bad token")
    data = {"token": token, "userAgent": request.headers.get("User-Agent", "")[:120]}
    if request.headers.get("Authorization", "").startswith("Bearer "):
        try: data["userId"] = auth_user()[0]          # link the device to the logged-in user (optional)
        except Err: pass
    commit([w_set("pushTokens/" + tok_id(token), data, now=["updatedAt"])])
    return jsonify(ok=True)

def all_push_tokens():
    out, page = [], None
    while True:
        params = {"pageSize": 300}
        if page: params["pageToken"] = page
        r = S().get(ROOT + DB + "/pushTokens", params=params)
        if r.status_code == 404: break
        r.raise_for_status(); j = r.json()
        for d in j.get("documents", []):
            t = (d.get("fields", {}).get("token") or {}).get("stringValue")
            if t: out.append((d["name"].rsplit("/", 1)[1], t))
        page = j.get("nextPageToken")
        if not page: break
    return out

def fcm_send(token, title, body, link, icon):
    wn = {"icon": icon, "badge": icon} if icon else {}
    msg = {"message": {"token": token, "notification": {"title": title, "body": body},
                       "webpush": {"notification": wn, "fcm_options": {"link": link}, "headers": {"Urgency": "high", "TTL": "86400"}}}}
    try: r = S().post(FCM_URL, json=msg, timeout=20)
    except Exception as ex: print("FCM conn error", type(ex).__name__); return "err"
    if r.ok: return "ok"
    if r.status_code == 404 or "UNREGISTERED" in r.text or "INVALID_ARGUMENT" in r.text: return "dead"
    print("FCM error", r.status_code, r.text[:300]); return "err"

def push_worker(toks, title, body, link, icon):
    def one(it):
        res = fcm_send(it[1], title, body, link, icon)
        if res == "dead":
            try: S().delete(ROOT + DB + "/pushTokens/" + it[0])
            except Exception: pass
        return res
    with ThreadPoolExecutor(max_workers=16) as ex: results = list(ex.map(one, toks))
    print(f"PUSH done: {results.count('ok')} sent, {results.count('dead')} removed, {results.count('err')} failed (of {len(toks)})")

@app.post("/sendPush")
@app.post("/api/sendPush")
def send_push():
    uid, _ = auth_user(); u, _ = fget("users/" + uid)
    if not u or u.get("role") != "admin": raise Err("Admin only", 403)
    b = request.get_json(silent=True) or {}
    title = str(b.get("title") or "").strip()[:80] or "M H X TopUp"; body = str(b.get("message") or "").strip()[:300]
    if not body: raise Err("Message required")
    link = str(b.get("link") or "").strip(); link = link if link.startswith("https://") else DEFAULT_ORIGIN
    g, _ = fget("settings/general"); icon = str((g or {}).get("logoUrl") or ""); icon = icon if icon.startswith("https://") else ""
    toks = all_push_tokens()
    if toks: threading.Thread(target=push_worker, args=(toks, title, body, link, icon), daemon=True).start()
    return jsonify(ok=True, queued=len(toks))

# ======================================================================
# AUTO LIKE (subscription)
#   User buys a 3 / 7 / 15 / 30-day package for ONE UID. Every day at the admin-set Bangladesh time (default 04:05)
#   the server sends the Daily Like to that UID automatically. Money is taken once when the package is bought and is
#   never refunded when a daily like cannot be delivered.
#   Admin controls (Firestore doc settings/autolike): enabled, sendHour, sendMinute, changeFee, packages[]
#   Per-user subscription doc: autoLikes/{userId}   (+ autoLikes/{userId}/logs/{id})
# ======================================================================
AL_DEFAULT_PACKAGES = [
    {"id": "d3", "name": "3 Days", "days": 3, "price": 15, "active": True},
    {"id": "d7", "name": "7 Days", "days": 7, "price": 35, "active": True},
    {"id": "d15", "name": "15 Days", "days": 15, "price": 70, "active": True},
    {"id": "d30", "name": "1 Month", "days": 30, "price": 130, "active": True},
]
AL_MAX_TRIES = 6                 # a failed daily send is retried up to this many times per day
AL_RETRY_MS = 8 * 60 * 1000      # ... with this gap between two tries
_al_pool = ThreadPoolExecutor(max_workers=3)
_al_running, _al_run_lock = set(), threading.Lock()

def bd_dt(): return datetime.now(timezone(timedelta(hours=6)))
def bd_tomorrow(): return (bd_dt() + timedelta(days=1)).strftime("%Y-%m-%d")

def al_settings():
    g, _ = fget("settings/autolike"); g = g or {}
    raw = g.get("packages")
    if not isinstance(raw, list): raw = AL_DEFAULT_PACKAGES
    pk = []
    for i, p in enumerate(raw):
        if not isinstance(p, dict): continue
        days = to_i(p.get("days"))
        if days <= 0: continue
        rp = p.get("resellerPrice")
        pk.append({"id": str(p.get("id") or f"p{i}"), "name": str(p.get("name") or f"{days} Days"), "days": days, "price": max(to_i(p.get("price")), 0),
                   "resellerPrice": (max(to_i(rp), 0) if rp not in (None, "") else None), "active": p.get("active") is not False,
                   "desc": str(p.get("desc") or "").strip()[:1500]})
    en = g.get("enabled", True)
    return {"enabled": not (en is False or str(en).strip().lower() in ("off", "false", "0")), "packages": pk,
            "sendHour": min(23, max(0, to_i(g.get("sendHour"), 4))), "sendMinute": min(59, max(0, to_i(g.get("sendMinute"), 5))),
            "changeFee": max(0, to_i(g.get("changeFee"), 10)) if g.get("changeFee") not in (None, "") else 10,
            "offMsg": str(g.get("offMsg") or "")}

def al_send_time(cfg): return f"{cfg['sendHour']:02d}:{cfg['sendMinute']:02d}"
def al_price(pkg, u):
    if (u.get("role") or "user") in ("reseller", "admin") and pkg.get("resellerPrice") is not None: return pkg["resellerPrice"]
    return pkg["price"]
def al_active(s): return bool(s) and (s.get("status") or "active") == "active" and to_i(s.get("remaining")) > 0

def al_next_date(s):
    today = bd_day()
    if s.get("lastDay") == today or (s.get("triesDay") == today and to_i(s.get("tries")) >= AL_MAX_TRIES): return bd_tomorrow()
    return today

def al_view(s, cfg):
    act = al_active(s)
    return {"status": "active" if act else ("cancelled" if s.get("status") == "cancelled" else "expired"), "uid": str(s.get("playerId") or ""), "region": str(s.get("region") or ""),
            "package": str(s.get("packageName") or ""), "remaining": max(to_i(s.get("remaining")), 0), "total": to_i(s.get("totalDays")), "daysSent": to_i(s.get("daysSent")),
            "likesTotal": to_i(s.get("likesTotal")), "lastDay": str(s.get("lastDay") or ""), "lastResult": str(s.get("lastResult") or ""), "lastMsg": str(s.get("lastMsg") or ""),
            "lastAt": str(s.get("lastAt") or ""), "uidChanges": to_i(s.get("uidChanges")), "nextDate": al_next_date(s) if act else "", "sendTime": al_send_time(cfg)}

def al_valid_target(b):
    player = "".join(ch for ch in str(b.get("uid", "")) if ch.isdigit())
    if not 5 <= len(player) <= 15: raise Err("সঠিক UID দিন (শুধু সংখ্যা)")
    region = str(b.get("region", "")).strip().upper()
    if region not in LIKE_REGIONS: raise Err("আগে সঠিক রিজিয়ন (সার্ভার) সিলেক্ট করুন")
    return player, region

@app.get("/autoLikeInfo")
@app.get("/api/autoLikeInfo")
def auto_like_info():
    cfg = al_settings()
    return jsonify(ok=True, enabled=cfg["enabled"], offMsg=cfg["offMsg"], sendTime=al_send_time(cfg), changeFee=cfg["changeFee"], regions=list(LIKE_REGIONS),
                   packages=[{k: p[k] for k in ("id", "name", "days", "price", "resellerPrice", "desc")} for p in cfg["packages"] if p["active"]])

@app.post("/autoLikeMine")
@app.post("/api/autoLikeMine")
def auto_like_mine():
    uid, _ = auth_user(); cfg = al_settings()
    s, _ = fget("autoLikes/" + uid)
    hist = []
    if s:
        try: hist = [{"createdAt": l.get("createdAt"), "day": l.get("day"), "result": l.get("result"), "given": to_i(l.get("given")), "before": l.get("before"), "after": l.get("after"), "message": l.get("message") or "", "kind": l.get("kind") or "send"}
                     for _, l in fquery("logs", order="createdAt", desc=True, limit=15, parent="autoLikes/" + uid)]
        except Exception as ex: print("AUTOLIKE hist error", ex)
    return jsonify(ok=True, subscription=(al_view(s, cfg) if s else None), history=hist, changeFee=cfg["changeFee"], sendTime=al_send_time(cfg))

@app.post("/autoLikeBuy")
@app.post("/api/autoLikeBuy")
def auto_like_buy():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    cfg = al_settings()
    if not cfg["enabled"]: raise Err(cfg["offMsg"] or "🤖 অটো লাইক সার্ভিস সাময়িকভাবে বন্ধ আছে।", 403)
    pkg = next((p for p in cfg["packages"] if p["active"] and p["id"] == str(b.get("packageId"))), None)
    if not pkg: raise Err("প্যাকেজটি পাওয়া যায়নি", 404)
    player, region = al_valid_target(b)
    rid = re.sub(r"[^A-Za-z0-9]", "", str(b.get("reqId") or ""))[:40] or uuid.uuid4().hex
    pid = f"{uid[:28]}_{rid}"
    if fget("autoLikePurchases/" + pid)[0]: return jsonify(ok=True, already=True)          # same request sent twice -> never charge twice
    u, ut = fget("users/" + uid)
    if not u: raise Err("User not found", 404)
    if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
    s, sut = fget("autoLikes/" + uid)
    active = al_active(s)
    if active and (str(s.get("playerId")) != player or str(s.get("region")) != region):
        raise Err(f"আপনার অটো লাইক চালু আছে (UID {s.get('playerId')} • {s.get('region')})। একই UID তে প্যাকেজ বাড়াতে পারবেন, অন্য UID হলে প্রোফাইল মেনুর 'Your Auto Like UID' থেকে UID Change করুন।", 409)
    price = al_price(pkg, u)
    pf = "balance"; price = check_pay(u, price)
    name = u.get("name") or (u.get("email") or "User").split("@")[0]
    writes = [w_set("users/" + uid, {}, mask=[], inc={pf: -price}, pre={"updateTime": ut}),
              w_set("autoLikePurchases/" + pid, {"userId": uid, "packageId": pkg["id"], "packageName": pkg["name"], "days": pkg["days"], "price": price, "playerId": player, "region": region, "payWith": pf}, pre={"exists": False}, now=["createdAt"])]
    if active:
        writes.append(w_set("autoLikes/" + uid, {"packageName": pkg["name"], "status": "active"}, inc={"remaining": pkg["days"], "totalDays": pkg["days"]}, pre={"updateTime": sut}))
    else:
        writes.append(w_set("autoLikes/" + uid, {"userId": uid, "userName": name, "userEmail": u.get("email", ""), "playerId": player, "region": region, "packageId": pkg["id"], "packageName": pkg["name"],
                                                 "status": "active", "remaining": pkg["days"], "totalDays": pkg["days"], "daysSent": 0, "likesTotal": 0, "lastDay": "", "lastResult": "", "lastMsg": "",
                                                 "lastAt": "", "triesDay": "", "tries": 0, "lastTryMs": 0, "uidChanges": 0, "paid": price},
                            pre=({"updateTime": sut} if sut else {"exists": False}), now=["startedAt"]))
    writes.append(w_set("orders/" + uuid.uuid4().hex, {"userId": uid, "userName": name, "userEmail": u.get("email", ""), "gameName": "🤖 Auto Like", "gameId": "autolike", "playerId": player, "server": region,
                                                      "productName": f"{pkg['name']} • daily at {al_send_time(cfg)}", "price": price, "originalPrice": pkg["price"], "promoCode": "", "type": "autolike", "status": "completed"},
                        pre={"exists": False}, now=["date"]))
    writes.append(w_set("recentOrders/" + uuid.uuid4().hex, {"name": str(name)[:3] + "***", "productName": f"Auto Like {pkg['name']}", "price": price}, pre={"exists": False}, now=["date"]))
    try: commit(writes)
    except Err: raise Err("অর্ডার সম্পন্ন করা যায়নি, আবার চেষ্টা করুন", 409)
    tg_notify("🤖", "AUTO LIKE BOUGHT" + (" (extended)" if active else ""), uid, u, [("📦", "Package", pkg["name"]), ("🎮", "UID", f"{player} ({region})"), ("💰", "Price", f"৳{price}")])
    return jsonify(ok=True, price=price, days=pkg["days"], extended=bool(active), sendTime=al_send_time(cfg))

@app.post("/autoLikeChangeUid")
@app.post("/api/autoLikeChangeUid")
def auto_like_change_uid():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    player, region = al_valid_target(b); cfg = al_settings()
    u, ut = fget("users/" + uid)
    if not u: raise Err("User not found", 404)
    if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
    s, sut = fget("autoLikes/" + uid)
    if not al_active(s): raise Err("আপনার কোনো সক্রিয় অটো লাইক প্যাকেজ নেই", 404)
    if str(s.get("playerId")) == player and str(s.get("region")) == region: raise Err("এটি আপনার বর্তমান UID ই, নতুন UID দিন")
    fee = cfg["changeFee"]; pf = "balance"
    if fee > 0:
        try: fee = check_pay(u, fee)
        except Err: raise Err(f"UID চেঞ্জ করতে ৳{fee} লাগবে, আপনার পর্যাপ্ত ব্যালেন্স নেই।")
    name = u.get("name") or (u.get("email") or "User").split("@")[0]
    writes = [w_set("autoLikes/" + uid, {"playerId": player, "region": region}, inc={"uidChanges": 1}, pre={"updateTime": sut}),
              w_set(f"autoLikes/{uid}/logs/{uuid.uuid4().hex}", {"kind": "uid_change", "result": "uid_change", "day": bd_day(), "message": f"{s.get('playerId')} ({s.get('region')}) → {player} ({region}) • fee ৳{fee}"}, pre={"exists": False}, now=["createdAt"])]
    if fee > 0:
        writes.insert(0, w_set("users/" + uid, {}, mask=[], inc={pf: -fee}, pre={"updateTime": ut}))
        writes.append(w_set("orders/" + uuid.uuid4().hex, {"userId": uid, "userName": name, "userEmail": u.get("email", ""), "gameName": "🤖 Auto Like UID Change", "gameId": "autolike", "playerId": player, "server": region,
                                                          "productName": "UID change", "price": fee, "originalPrice": fee, "promoCode": "", "type": "autolike", "status": "completed"}, pre={"exists": False}, now=["date"]))
    try: commit(writes)
    except Err: raise Err("UID পরিবর্তন করা যায়নি, আবার চেষ্টা করুন", 409)
    return jsonify(ok=True, fee=fee, uid=player, region=region)

# ---------- the daily sender ----------
def al_log(uid, day, result, given=0, before=None, after=None, message="", order=""):
    try: commit([w_set(f"autoLikes/{uid}/logs/{uuid.uuid4().hex}", {"kind": "send", "day": day, "result": result, "given": int(given or 0), "before": before if before is not None else "", "after": after if after is not None else "",
                                                                  "message": str(message)[:200], "orderNo": order}, pre={"exists": False}, now=["createdAt"])])
    except Exception as ex: print("AUTOLIKE log error", ex)

def al_settle(uid, today, result, given, msg):
    """Day is used (success / already received): lower the counter by 1 -- read again first so an admin edit or a pack extension made meanwhile is never lost."""
    for _ in range(4):
        s2, st2 = fget("autoLikes/" + uid)
        if not s2: return
        rem = max(to_i(s2.get("remaining")) - 1, 0)
        try:
            commit([w_set("autoLikes/" + uid, {"remaining": rem, "status": ("active" if rem > 0 else "expired"), "lastDay": today, "lastResult": result, "lastMsg": msg[:160], "lastAt": bd_now(),
                                              "daysSent": to_i(s2.get("daysSent")) + 1, "likesTotal": to_i(s2.get("likesTotal")) + int(given or 0)}, pre={"updateTime": st2})]); return
        except Err: time.sleep(0.4)
    print("!!! AUTOLIKE settle failed", uid)

def al_process(uid, force=False):
    with _al_run_lock:
        if uid in _al_running: return
        _al_running.add(uid)
    try:
        today = bd_day()
        s, st = fget("autoLikes/" + uid)
        if not s or not st or not al_active(s) or s.get("lastDay") == today: return
        tries = to_i(s.get("tries")) if s.get("triesDay") == today else 0
        if not force and (tries >= AL_MAX_TRIES or (tries and now_ms() - to_i(s.get("lastTryMs")) < AL_RETRY_MS)): return
        player, region = str(s.get("playerId")), str(s.get("region"))
        if not (5 <= len(player) <= 15) or region not in LIKE_REGIONS: return
        bkey = player + region
        with _like_lock:
            if bkey in _like_busy: return            # a manual Daily Like for this UID is running right now -> try again in a few minutes
            _like_busy.add(bkey)
        try:
            try: commit([w_set("autoLikes/" + uid, {"triesDay": today, "tries": tries + 1, "lastTryMs": now_ms()}, pre={"updateTime": st})])     # claim: only ONE worker can ever send for this user today
            except Err: return
            res = call_like_api(player, region)
            try: stt = int(res.get("status"))
            except Exception: stt = 0
            if stt == 1:
                given = to_int(res.get("LikesGivenByAPI")) or 0
                al_settle(uid, today, "success", given, "")
                al_log(uid, today, "success", given, to_int(res.get("LikesBeforeCommand")), to_int(res.get("LikesAfterCommand")), "", str(res.get("OrderNo") or ""))
                print("AUTOLIKE ok", uid, player, region, given)
            elif stt == 2:
                msg = str(res.get("message") or "এই UID আজকের লাইক ইতিমধ্যে পেয়ে গেছে।")
                al_settle(uid, today, "already", 0, msg); al_log(uid, today, "already", 0, message=msg)
                print("AUTOLIKE already", uid, player, region)
            else:
                msg = str(res.get("message") or "লাইক পাঠানো যায়নি।")
                final = tries + 1 >= AL_MAX_TRIES
                try: commit([w_set("autoLikes/" + uid, {"lastResult": "failed", "lastMsg": msg[:160], "lastAt": bd_now()})])
                except Exception as ex: print("AUTOLIKE fail-mark error", ex)
                if final or tries == 0: al_log(uid, today, "failed", 0, message=msg + (" (আজকের শেষ চেষ্টা — প্যাকেজের দিন কাটা হয়নি)" if final else " (আবার চেষ্টা করা হবে)"))
                print("AUTOLIKE failed", uid, player, region, tries + 1, msg[:80])
        finally:
            with _like_lock: _like_busy.discard(bkey)
    except Exception as ex:
        print("AUTOLIKE process error", uid, type(ex).__name__, ex)
    finally:
        with _al_run_lock: _al_running.discard(uid)

_al_next_scan = {"t": 0.0}
def al_tick(force=False):
    if not force and (time.time() < _al_next_scan["t"] or fs_cooling()): return 0       # nothing was pending last time -> don't re-read every user again so soon
    cfg = al_settings()
    if not force:
        if not cfg["enabled"]: return 0
        now = bd_dt()
        if (now.hour, now.minute) < (cfg["sendHour"], cfg["sendMinute"]): return 0      # before the admin-set time (default 04:05 Bangladesh time)
    today, n = bd_day(), 0
    for uid, s in fquery("autoLikes", "status", "active", limit=5000):
        if to_i(s.get("remaining")) <= 0:
            try: commit([w_set("autoLikes/" + uid, {"status": "expired"})])
            except Exception: pass
            continue
        if s.get("lastDay") == today: continue
        _al_pool.submit(al_process, uid, force); n += 1
    if not force: _al_next_scan["t"] = time.time() + (60 if n else 600)   # idle -> look again in 10 min (this was a 5000-read scan every 30 s)
    return n

def al_loop():
    time.sleep(20)
    while True:
        try: al_tick()
        except Exception as ex: print("AUTOLIKE tick error", type(ex).__name__, ex)
        time.sleep(300 if fs_cooling() else 45)

@app.post("/adminAutoLikeRun")
@app.post("/api/adminAutoLikeRun")
def admin_auto_like_run():
    require_admin(); b = request.get_json(silent=True) or {}
    one = str(b.get("userId") or "").strip()
    if one:
        _al_pool.submit(al_process, one, True); return jsonify(ok=True, queued=1)
    n = al_tick(force=True)
    return jsonify(ok=True, queued=n)

# ================= LEADERBOARD + PROFILE CUSTOMISE =================
# Leaderboard = ranked by users.lbScore = TOTAL money ever ADDED to the wallet (deposits, promo cash, ads rewards, referral bonus, admin "add").
# Spending money lowers users.balance but NEVER lbScore. Refunds and admin deductions do not touch lbScore either.
# Cached a few seconds per page-size so many viewers cost only one Firestore read.
_LB = {}
_LB_LOCK = threading.Lock()
LB_TTL = 45
LB_STEP = 20
LB_MAX = 500
AVATAR_MAX = 60000            # max characters of the stored profile picture (the browser shrinks it to ~160px first, usually 6-15 KB)

def lb_migrate():
    """One-time catch-up: users that have no lbScore yet start with their current balance (so nobody disappears from the board)."""
    try:
        done = fget("settings/lbMigrated")[0]
        if done: return
        n = 0
        for uid, u in flist("users"):
            if "lbScore" in u: continue
            start = max(to_i(u.get("balance")), 0)
            try: commit([w_set("users/" + uid, {"lbScore": start}, mask=["lbScore"], pre={"exists": True})]); n += 1
            except Exception: pass
        commit([w_set("settings/lbMigrated", {"done": True, "count": n})])
        lb_drop_cache()
    except Exception as e:
        print("lb_migrate failed:", e)

def lb_rows(limit):
    with _LB_LOCK:
        c = _LB.get(limit)
        if c and time.time() - c[0] < LB_TTL: return c[1]
    try: raw = fquery("users", order="lbScore", desc=True, limit=limit + 16)
    except Exception:
        with _LB_LOCK: c = _LB.get(limit)
        if c: return c[1]                                    # Firestore busy -> show the last leaderboard instead of an error
        raise
    valid = []
    for uid, u in raw:
        sc = to_i(u.get("lbScore"))
        if sc <= 0: break                                     # sorted high -> low, so nobody after this has any total
        if u.get("banned") or (u.get("role") or "user") == "admin": continue
        av = str(u.get("avatar") or "")
        if not av.startswith("data:image/"): av = ""
        valid.append({"uid": uid, "name": (str(u.get("name") or "").strip() or "User")[:20], "avatar": av, "score": sc, "role": u.get("role") or "user"})
    out = {"rows": valid[:limit], "hasMore": len(valid) > limit}
    for i, r in enumerate(out["rows"]): r["rank"] = i + 1
    with _LB_LOCK:
        if len(_LB) > 20: _LB.clear()
        _LB[limit] = (time.time(), out)
    return out

def lb_drop_cache():
    with _LB_LOCK: _LB.clear()

@app.get("/leaderboard")
@app.get("/api/leaderboard")
def leaderboard():
    _g, _ = fget("settings/general")
    if (_g or {}).get("leaderboardEnabled") is False: raise Err("Leaderboard is turned off", 403)
    limit = min(max(to_i(request.args.get("limit"), LB_STEP), LB_STEP), LB_MAX); limit = -(-limit // LB_STEP) * LB_STEP
    data = lb_rows(limit); rows = data["rows"]; me = None
    h = request.headers.get("Authorization", "")
    if h.startswith("Bearer "):
        try:
            uid, _ = auth_user()
            mine = next((r for r in rows if r["uid"] == uid), None)
            me = {"uid": uid, "rank": mine["rank"] if mine else None, "score": mine["score"] if mine else None}
        except Exception: me = None
    pub = []
    for r in rows:
        d = {k: v for k, v in r.items() if k != "uid"}; d["mine"] = bool(me and r["uid"] == me["uid"]); pub.append(d)
    resp = jsonify(ok=True, rows=pub, me=({"rank": me["rank"], "score": me["score"]} if me else None), size=limit, hasMore=data["hasMore"], updated=now_ms())
    resp.headers["Cache-Control"] = "no-store"
    return resp

_NAME_BAD = re.compile(r"[<>{}\\`\u0000-\u001f]")
@app.post("/updateProfile")
@app.post("/api/updateProfile")
def update_profile():
    """Lets a logged-in user change their own display name and/or profile picture (sent as a small data-URL image)."""
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    u, _t = fget("users/" + uid)
    if u is None: raise Err("User not found", 404)
    if u.get("banned"): raise Err("Account banned", 403)
    upd = {}
    if "name" in b:
        nm = re.sub(r"\s+", " ", str(b.get("name") or "")).strip()
        if len(nm) < 2: raise Err("নাম কমপক্ষে ২ অক্ষরের হতে হবে")
        if len(nm) > 20: raise Err("নাম সর্বোচ্চ ২০ অক্ষরের হতে পারবে")
        if _NAME_BAD.search(nm): raise Err("নামে অনুমোদিত নয় এমন চিহ্ন আছে")
        upd["name"] = nm
    if "avatar" in b:
        av = b.get("avatar")
        if av in (None, ""): upd["avatar"] = ""                      # remove picture
        else:
            av = str(av)
            if not re.match(r"^data:image/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+$", av): raise Err("ছবির ফরম্যাট সঠিক নয়")
            if len(av) > AVATAR_MAX: raise Err("ছবিটি অনেক বড়, ছোট ছবি দিন")
            upd["avatar"] = av
    if not upd: raise Err("কিছু পরিবর্তন করা হয়নি")
    ok, _wait = rate_ok(f"prof:{uid}", 12)
    if not ok: raise Err("একটু পরে আবার চেষ্টা করুন", 429)
    commit([w_set("users/" + uid, upd, mask=list(upd.keys()), pre={"exists": True})])
    lb_drop_cache()
    return jsonify(ok=True, name=upd.get("name"), avatar=upd.get("avatar"))

# =====================================================================================================
#  ID SELL  (sell Free Fire / Gmail / Facebook IDs)  +  ID CODE  (customer redeems a code -> gets the login)
#  * Account logins (email + password) are ENCRYPTED here on the server and stored only in the private
#    Firestore collection "idvault". The website / admin panel can never read them back -- a login is
#    handed out ONLY to the logged-in buyer who owns the code.
#  * Encryption key: set IDVAULT_KEY in .env (best). If you don't, a random key is created once in the file
#    "idvault.key" next to main.py. KEEP A BACKUP of that key: without it the saved logins cannot be opened.
#  * Do NOT add any Firestore security rule that lets clients read "idvault" / "idListings".
# =====================================================================================================
import base64, hmac, random
try:
    from cryptography.fernet import Fernet, InvalidToken
except Exception:
    Fernet = None
    class InvalidToken(Exception): pass

_BASE = os.path.dirname(os.path.abspath(__file__))
_vk = None
def _vault_key():
    global _vk
    if _vk: return _vk
    s = os.getenv("IDVAULT_KEY", "").strip()
    if not s:
        p = os.path.join(_BASE, "idvault.key")
        try:
            if os.path.exists(p): s = open(p, encoding="utf-8").read().strip()
            if not s:
                s = secrets.token_urlsafe(48)
                with open(p, "w", encoding="utf-8") as f: f.write(s)
                try: os.chmod(p, 0o600)
                except Exception: pass
        except Exception:
            raise Err("Vault key error: set IDVAULT_KEY in your .env file", 500)
    _vk = hashlib.sha256(("mhx-idvault|" + s).encode()).digest()
    return _vk
def _ks(key, nonce, n):
    out, i = b"", 0
    while len(out) < n:
        out += hmac.new(key, nonce + i.to_bytes(4, "big"), hashlib.sha256).digest(); i += 1
    return out[:n]
def vault_enc(code, data):
    raw = json.dumps({"c": code, "d": data}, ensure_ascii=False).encode("utf-8"); k = _vault_key()
    if Fernet: return "f1:" + Fernet(base64.urlsafe_b64encode(k)).encrypt(raw).decode()
    nonce = secrets.token_bytes(16); ct = bytes(a ^ b for a, b in zip(raw, _ks(k, nonce, len(raw))))
    mac = hmac.new(hashlib.sha256(k + b"mac").digest(), nonce + ct, hashlib.sha256).digest()
    return "s1:" + base64.urlsafe_b64encode(nonce + mac + ct).decode()
def vault_dec(code, blob):
    k = _vault_key(); blob = str(blob or "")
    try:
        if blob.startswith("f1:"):
            if not Fernet: raise Err("Server needs the 'cryptography' package to open this login", 500)
            raw = Fernet(base64.urlsafe_b64encode(k)).decrypt(blob[3:].encode())
        elif blob.startswith("s1:"):
            b = base64.urlsafe_b64decode(blob[3:].encode()); nonce, mac, ct = b[:16], b[16:48], b[48:]
            if not hmac.compare_digest(mac, hmac.new(hashlib.sha256(k + b"mac").digest(), nonce + ct, hashlib.sha256).digest()): raise InvalidToken()
            raw = bytes(a ^ c for a, c in zip(ct, _ks(k, nonce, len(ct))))
        else: raise InvalidToken()
        o = json.loads(raw.decode("utf-8"))
    except Err: raise
    except Exception: raise Err("এই আইডির তথ্য খোলা যায়নি, সাপোর্টে যোগাযোগ করুন", 500)
    if o.get("c") != code: raise Err("এই আইডির তথ্য খোলা যায়নি, সাপোর্টে যোগাযোগ করুন", 500)
    return o.get("d") or {}

CODE_ALPHA = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
def clean_code(c): return re.sub(r"[^A-Z0-9-]", "", str(c or "").upper().replace(" ", ""))[:48]
def gen_code():
    for _ in range(20):
        c = "MHX-" + "-".join("".join(secrets.choice(CODE_ALPHA) for _ in range(4)) for _ in range(3))
        if not fget("idvault/" + c)[0]: return c
    raise Err("কোড তৈরি করা যায়নি, আবার চেষ্টা করুন", 500)
def mask_acct(a):
    a = str(a or "")
    if "@" in a: l, d = a.split("@", 1); return l[:2] + "***@" + d
    return a[:2] + "***" if a else ""
def safe_id(v):
    v = str(v or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", v): raise Err("Listing not found", 404)
    return v

_ids_cache = {"t": 0, "v": None, "lt": 0, "lv": None}
def ids_drop(): _ids_cache["t"] = 0; _ids_cache["lt"] = 0
def ids_listings():
    if _ids_cache["lv"] is not None and time.time() - _ids_cache["lt"] < 45: return _ids_cache["lv"]
    v = flist("idListings"); _ids_cache["lt"], _ids_cache["lv"] = time.time(), v; return v
def ids_stock():
    """{listingId: [available, sold]} (cached 8s)"""
    if _ids_cache["v"] is not None and time.time() - _ids_cache["t"] < 45: return _ids_cache["v"]
    st = {}
    for _c, d in flist("idvault", mask=["listingId", "status"]):
        a = st.setdefault(d.get("listingId") or "", [0, 0])
        a[0 if d.get("status") == "available" else 1] += 1
    _ids_cache["t"], _ids_cache["v"] = time.time(), st
    return st
def listing_out(i, d, st):
    s = st.get(i, [0, 0])
    return {"id": i, "title": d.get("title") or "", "videoUrl": d.get("videoUrl") or "", "level": d.get("level") or "", "likes": d.get("likes") or "",
            "details": d.get("details") or "", "price": to_i(d.get("price")), "active": d.get("active") is not False, "sort": to_i(d.get("sort")),
            "image": d.get("image") or "", "closed": bool(d.get("closed")), "closedMsg": d.get("closedMsg") or "", "autoClosed": bool(d.get("autoClosed")),
            "available": s[0], "sold": s[1]}

@app.get("/idListings")
@app.get("/api/idListings")
def id_listings():
    st = ids_stock(); out = [listing_out(i, d, st) for i, d in ids_listings() if d.get("active") is not False]
    out.sort(key=lambda x: (x["sort"], x["title"].lower()))
    return jsonify(ok=True, items=out)

@app.post("/buyId")
@app.post("/api/buyId")
def buy_id():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    g, _ = fget("settings/general"); g = g or {}
    if g.get("maintenance"): raise Err("ওয়েবসাইট এখন মেইনটেন্যান্সে আছে, একটু পরে চেষ্টা করুন", 503)
    ok, _w = rate_ok(f"idbuy:{uid}", 6)
    if not ok: raise Err("একটু পরে আবার চেষ্টা করুন", 429)
    lid = safe_id(b.get("listingId")); lst, _ = fget("idListings/" + lid)
    if not lst or lst.get("active") is False: raise Err("এই আইডিটি পাওয়া যায়নি", 404)
    if lst.get("closed"): raise Err(str(lst.get("closedMsg") or "এই আইডিটি বিক্রি হয়ে গেছে"), 409)
    price = to_i(lst.get("price"))
    if price <= 0: raise Err("এই আইডির প্রাইস সেট করা নেই")
    with user_lock(uid):
        for attempt in range(4):
            u, ut = fget("users/" + uid)
            if not u: raise Err("User not found", 404)
            if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
            if to_i(u.get("balance")) < price: raise Err("আপনার পর্যাপ্ত ব্যালেন্স নেই।")
            avail = [c for c, d in fquery("idvault", "listingId", lid, limit=500) if d.get("status") == "available"]
            if not avail: raise Err("এই আইডিটি এখন স্টকে নেই", 409)
            code = random.choice(avail); ud, uut = fget("idvault/" + code)
            if not ud or ud.get("status") != "available": continue
            name = u.get("name") or (u.get("email") or "User").split("@")[0]; oid = uuid.uuid4().hex
            try:
                commit([
                    w_set("users/" + uid, {}, mask=[], inc={"balance": -price}, pre={"updateTime": ut}),
                    w_set("idvault/" + code, {"status": "sold", "soldTo": uid, "soldToEmail": u.get("email", ""), "price": price}, pre={"updateTime": uut}, now=["soldAt"]),
                    w_set("orders/" + oid, {"userId": uid, "userName": name, "userEmail": u.get("email", ""), "gameName": "ID Sell", "gameId": "idsell", "playerId": "",
                          "productName": lst.get("title", ""), "price": price, "originalPrice": price, "promoCode": "", "type": "idsell", "status": "success", "redeemCode": code},
                          pre={"exists": False}, now=["date"])])
            except Err:
                time.sleep(0.15); continue
            if len(avail) <= 1:        # that was the last one in stock -> the ID card on the website closes automatically
                try: commit([w_set("idListings/" + lid, {"closed": True, "autoClosed": True, "closedMsg": ""}, pre={"exists": True})])
                except Exception: pass
            ids_drop()
            tg_notify("🪪", "ID BOUGHT", uid, u, [("📦", "ID", lst.get("title", "")), ("💰", "Price", f"৳{price}")])
            return jsonify(ok=True, code=code, price=price, title=lst.get("title", ""))
    raise Err("অর্ডার সম্পন্ন করা যায়নি, আবার চেষ্টা করুন", 409)

@app.post("/redeemIdCode")
@app.post("/api/redeemIdCode")
def redeem_id_code():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    ok, _w = rate_ok(f"idcr:{uid}", 8)
    if not ok: raise Err("অনেকবার চেষ্টা করা হয়েছে, এক মিনিট পরে আবার চেষ্টা করুন", 429)
    u, _ = fget("users/" + uid)
    if not u or u.get("banned"): raise Err("Account banned", 403)
    code = clean_code(b.get("code"))
    d, _ = fget("idvault/" + code) if len(code) >= 6 else (None, None)
    if not d or d.get("status") != "sold" or d.get("soldTo") != uid: raise Err("কোডটি সঠিক নয়")
    data = vault_dec(code, d.get("enc"))
    try: commit([w_set("idvault/" + code, {}, mask=[], inc={"views": 1})])
    except Exception: pass
    return jsonify(ok=True, title=d.get("title") or "", loginType=data.get("loginType") or "", account=data.get("account") or "",
                   password=data.get("password") or "", note=data.get("note") or "")

@app.post("/myIdCodes")
@app.post("/api/myIdCodes")
def my_id_codes():
    uid, _ = auth_user(); out = []
    for c, d in fquery("idvault", "soldTo", uid, limit=100):
        if d.get("status") == "sold": out.append({"code": c, "title": d.get("title") or "", "loginType": d.get("loginType") or "", "soldAt": str(d.get("soldAt") or "")})
    out.sort(key=lambda x: x["soldAt"], reverse=True)
    return jsonify(ok=True, items=out)

# ---------- admin ----------
LOGIN_TYPES = ("Gmail", "Facebook", "Other")
@app.post("/adminIdSell")
@app.post("/api/adminIdSell")
def admin_id_sell():
    require_admin(); b = request.get_json(silent=True) or {}; act = str(b.get("action") or "")
    if act == "list":
        st = ids_stock(); ids = flist("idListings"); units = {}
        for c, d in flist("idvault", mask=["listingId", "status", "loginType", "acct", "soldToEmail", "price"]):
            units.setdefault(d.get("listingId") or "", []).append({"code": c, "status": d.get("status") or "available", "loginType": d.get("loginType") or "",
                                                                "hint": d.get("acct") or "", "soldToEmail": d.get("soldToEmail") or "", "price": to_i(d.get("price"))})
        items = []
        for i, d in ids:
            o = listing_out(i, d, st); o["units"] = sorted(units.get(i, []), key=lambda x: (x["status"] != "available", x["code"])); items.append(o)
        items.sort(key=lambda x: (x["sort"], x["title"].lower()))
        return jsonify(ok=True, items=items)
    if act == "genCode": return jsonify(ok=True, code=gen_code())
    if act == "saveListing":
        title = str(b.get("title") or "").strip()[:80]
        if not title: raise Err("টাইটেল লিখুন")
        url = str(b.get("videoUrl") or "").strip()[:500]
        if url and not url.lower().startswith(("http://", "https://")): raise Err("ভিডিও লিংক https:// দিয়ে শুরু হতে হবে")
        img = str(b.get("image") or "").strip()
        if img and not (img.lower().startswith(("http://", "https://")) or img.startswith("data:image/")): raise Err("ছবির লিংক https:// দিয়ে শুরু হতে হবে")
        if len(img) > 400000: raise Err("ছবিটি অনেক বড়, ছোট ছবি বা লিংক ব্যবহার করুন")
        data = {"title": title, "image": img, "videoUrl": url, "level": str(b.get("level") or "").strip()[:30], "likes": str(b.get("likes") or "").strip()[:30],
                "details": str(b.get("details") or "").strip()[:800], "price": max(0, to_i(b.get("price"))), "active": bool(b.get("active", True)), "sort": to_i(b.get("sort"))}
        lid = str(b.get("id") or "").strip()
        if lid:
            lid = safe_id(lid)
            if not fget("idListings/" + lid)[0]: raise Err("Listing not found", 404)
            commit([w_set("idListings/" + lid, data, pre={"exists": True})])
        else:
            lid = uuid.uuid4().hex[:12]; commit([w_set("idListings/" + lid, data, pre={"exists": False}, now=["createdAt"])])
        ids_drop(); return jsonify(ok=True, id=lid)
    if act == "deleteListing":
        lid = safe_id(b.get("id")); ws = [w_del("idListings/" + lid)]
        for c, d in fquery("idvault", "listingId", lid, limit=500):
            if d.get("status") == "available": ws.append(w_del("idvault/" + c))
        for k in range(0, len(ws), 400): commit(ws[k:k + 400])
        ids_drop(); return jsonify(ok=True)
    if act == "addUnit":
        lid = safe_id(b.get("listingId")); lst, _ = fget("idListings/" + lid)
        if not lst: raise Err("Listing not found", 404)
        acct = str(b.get("account") or "").strip()[:200]; pw = str(b.get("password") or "")[:200]
        if not acct or not pw: raise Err("আইডি (ইমেইল/নম্বর) ও পাসওয়ার্ড দুটোই দিন")
        lt = str(b.get("loginType") or "Gmail"); lt = lt if lt in LOGIN_TYPES else "Other"
        code = clean_code(b.get("code")) or gen_code()
        if len(code) < 6: raise Err("কোড কমপক্ষে ৬ অক্ষরের হতে হবে")
        data = {"loginType": lt, "account": acct, "password": pw, "note": str(b.get("note") or "").strip()[:400]}
        try:
            commit([w_set("idvault/" + code, {"listingId": lid, "title": lst.get("title", ""), "status": "available", "loginType": lt, "acct": mask_acct(acct), "enc": vault_enc(code, data)},
                          pre={"exists": False}, now=["createdAt"])])
        except Err: raise Err("এই কোডটি আগে থেকেই আছে, অন্য কোড দিন")
        if lst.get("autoClosed"):
            try: commit([w_set("idListings/" + lid, {"closed": False, "autoClosed": False, "closedMsg": ""}, pre={"exists": True})])
            except Exception: pass
        ids_drop(); return jsonify(ok=True, code=code)
    if act == "setClosed":
        lid = safe_id(b.get("id"))
        if not fget("idListings/" + lid)[0]: raise Err("Listing not found", 404)
        on = bool(b.get("closed")); msg = str(b.get("msg") or "").strip()[:200] if on else ""
        commit([w_set("idListings/" + lid, {"closed": on, "autoClosed": False, "closedMsg": msg}, pre={"exists": True})])
        ids_drop(); return jsonify(ok=True)
    if act == "deleteUnit":
        code = clean_code(b.get("code")); d, _ = fget("idvault/" + code)
        if not d: raise Err("Code not found", 404)
        if d.get("status") != "available": raise Err("বিক্রি হয়ে যাওয়া কোড ডিলিট করা যায় না")
        commit([w_del("idvault/" + code)]); ids_drop(); return jsonify(ok=True)
    raise Err("Unknown action")



# ======================================================================
#  TCP BOARD  (M H X TOOLS TCP bot — paid access passwords)
#   settings/tcp        : enabled, offMsg, tcpUrl, packagesJson
#   tcpKeys/{KEY}       : days(0=unlimited), maxUsers(0=unlimited), ownerId, label, price, startMs, expiresMs, devices, revoked
#   tcpAccounts/{guest} : password, jwt, region, used   (stock of guest accounts, used by the "MHX TCP" button)
#   The TCP server calls /tcpVerify /tcpClaim /tcpRegisterUrl with header X-Tcp-Secret (= env TCP_SECRET).
#   The countdown is stored here (absolute time), so it keeps running even when the TCP python is stopped.
# ======================================================================
import hmac as _hmac
def _tcp_secret():
    v = os.getenv("TCP_SECRET", "").strip()
    if v: return v
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "tcp_secret.txt"), encoding="utf-8") as f: return f.read().strip()   # or put the secret in tcp_secret.txt
    except Exception: return ""
TCP_SECRET = _tcp_secret()
TCP_DEFAULT_PACKAGES = [{"id": f"d{d}", "name": f"{d} Days", "days": d, "price": p, "active": True}
                        for d, p in ((3, 30), (5, 45), (7, 60), (10, 80), (12, 95), (15, 110), (20, 140), (25, 170), (30, 200))]
TCP_ALPHA = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

def tcp_settings():
    g, _ = fget("settings/tcp"); g = g or {}
    try: raw = json.loads(g.get("packagesJson") or "null")
    except Exception: raw = None
    if not isinstance(raw, list): raw = TCP_DEFAULT_PACKAGES
    pk = []
    for i, p in enumerate(raw):
        if not isinstance(p, dict): continue
        days = to_i(p.get("days"))
        if days <= 0: continue
        rp = p.get("resellerPrice")
        pk.append({"id": str(p.get("id") or f"p{i}"), "name": str(p.get("name") or f"{days} Days"), "days": days, "price": max(to_i(p.get("price")), 0),
                   "resellerPrice": (max(to_i(rp), 0) if rp not in (None, "") else None), "active": p.get("active") is not False})
    en = g.get("enabled", True)
    return {"enabled": not (en is False or str(en).strip().lower() in ("off", "false", "0")), "packages": pk,
            "offMsg": str(g.get("offMsg") or ""), "tcpUrl": str(g.get("tcpUrl") or ""),
            "hidden": g.get("hidden") is True or str(g.get("hidden")).strip().lower() in ("true", "1", "on")}

def tcp_new_key(): return "MHX-" + "-".join("".join(secrets.choice(TCP_ALPHA) for _ in range(4)) for _ in range(3))
def tcp_clean_key(k): return re.sub(r"[^A-Z0-9\-]", "", str(k or "").upper())[:40]
def tcp_secret_ok():
    if not TCP_SECRET: raise Err("TCP_SECRET is not set on the website backend (only needed for automatic link registration; set the link in Admin -> TCP BOT instead)", 503)
    if not _hmac.compare_digest(request.headers.get("X-Tcp-Secret", "").encode(), TCP_SECRET.encode()): raise Err("Forbidden", 403)
def tcp_state(k, now=None):
    now = now or now_ms()
    if k.get("revoked") is True: return "revoked"
    if to_i(k.get("expiresMs")) and now >= to_i(k.get("expiresMs")): return "expired"
    return "active" if to_i(k.get("startMs")) else "unused"
def tcp_view(key, k, url=""):
    return {"key": key, "label": str(k.get("label") or ""), "state": tcp_state(k), "days": to_i(k.get("days")), "maxUsers": to_i(k.get("maxUsers")),
            "users": len([x for x in str(k.get("devices") or "").split(",") if x]), "startMs": to_i(k.get("startMs")), "expiresMs": to_i(k.get("expiresMs")),
            "price": to_i(k.get("price")), "source": str(k.get("source") or ""), "ownerId": str(k.get("ownerId") or "")}
def tcp_make_key(days, max_users, label, owner="", price=0, source="admin", custom=""):
    key = tcp_clean_key(custom) or tcp_new_key()
    data = {"days": int(days or 0), "maxUsers": int(max_users or 0), "label": str(label or "")[:60], "ownerId": owner, "price": int(price or 0), "source": source,
            "startMs": 0, "expiresMs": 0, "devices": "", "revoked": False}
    commit([w_set("tcpKeys/" + key, data, pre={"exists": False}, now=["createdAt"])])
    return key

@app.get("/tcpInfo")
@app.get("/api/tcpInfo")
def tcp_info():
    c = tcp_settings()
    return jsonify(ok=True, enabled=c["enabled"], hidden=c["hidden"], offMsg=c["offMsg"], contact="mhxffv111",
                   packages=[{k: p[k] for k in ("id", "name", "days", "price", "resellerPrice")} for p in c["packages"] if p["active"]])

@app.post("/tcpBuy")
@app.post("/api/tcpBuy")
def tcp_buy():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}; cfg = tcp_settings()
    if cfg["hidden"] or not cfg["enabled"]: raise Err(cfg["offMsg"] or "TCP বোর্ড সাময়িকভাবে বন্ধ আছে।", 403)
    pkg = next((p for p in cfg["packages"] if p["active"] and p["id"] == str(b.get("packageId"))), None)
    if not pkg: raise Err("প্যাকেজটি পাওয়া যায়নি", 404)
    rid = re.sub(r"[^A-Za-z0-9]", "", str(b.get("reqId") or ""))[:40] or uuid.uuid4().hex
    pid = f"{uid[:28]}_{rid}"
    old, _ = fget("tcpPurchases/" + pid)
    if old: return jsonify(ok=True, already=True, key=old.get("key"))
    u, ut = fget("users/" + uid)
    if not u: raise Err("User not found", 404)
    if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
    price = pkg["resellerPrice"] if ((u.get("role") or "user") in ("reseller", "admin") and pkg.get("resellerPrice") is not None) else pkg["price"]
    pf = "balance"; price = check_pay(u, price)
    name = u.get("name") or (u.get("email") or "User").split("@")[0]
    key = tcp_new_key()
    writes = [w_set("users/" + uid, {}, mask=[], inc={pf: -price}, pre={"updateTime": ut}),
              w_set("tcpPurchases/" + pid, {"userId": uid, "key": key, "packageName": pkg["name"], "days": pkg["days"], "price": price, "payWith": pf}, pre={"exists": False}, now=["createdAt"]),
              w_set("tcpKeys/" + key, {"days": pkg["days"], "maxUsers": 1, "label": pkg["name"], "ownerId": uid, "ownerName": str(name)[:40], "price": price, "source": "buy",
                                       "startMs": 0, "expiresMs": 0, "devices": "", "revoked": False}, pre={"exists": False}, now=["createdAt"]),
              w_set("orders/" + uuid.uuid4().hex, {"userId": uid, "userName": name, "userEmail": u.get("email", ""), "gameName": "🛰️ TCP BOT", "gameId": "tcp", "playerId": key, "server": "TCP",
                                                    "productName": pkg["name"], "price": price, "originalPrice": pkg["price"], "promoCode": "", "type": "tcp", "status": "completed"}, pre={"exists": False}, now=["date"])]
    try: commit(writes)
    except Err: raise Err("অর্ডার সম্পন্ন করা যায়নি, আবার চেষ্টা করুন", 409)
    tg_notify("🛰️", "TCP BOT BOUGHT", uid, u, [("📦", "Package", pkg["name"]), ("💰", "Price", f"৳{price}")])
    return jsonify(ok=True, key=key, days=pkg["days"], price=price, tcpUrl=cfg["tcpUrl"])

@app.post("/tcpMine")
@app.post("/api/tcpMine")
def tcp_mine():
    uid, _ = auth_user(); cfg = tcp_settings()
    rows = [tcp_view(k, d) for k, d in fquery("tcpKeys", "ownerId", uid, limit=100)]
    rows.sort(key=lambda r: (r["state"] not in ("active", "unused"), -(r["expiresMs"] or 0)))
    return jsonify(ok=True, keys=rows, tcpUrl=cfg["tcpUrl"], contact="mhxffv111", now=now_ms())

@app.post("/tcpVerify")
@app.post("/api/tcpVerify")
def tcp_verify():
    # No shared secret needed: the password itself is the credential. Brute-force is rate-limited per caller IP.
    ip = (request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or request.remote_addr or "?")
    okr, _w = rate_ok("tcpv:" + ip, 120)
    if not okr: raise Err("Too many attempts, wait a minute.", 429)
    b = request.get_json(silent=True) or {}
    key = tcp_clean_key(b.get("key")); dev = re.sub(r"[^A-Za-z0-9._:-]", "", str(b.get("device") or ""))[:64]; login = b.get("action") == "login"
    k, kt = fget("tcpKeys/" + key) if key else (None, None)
    if not k: raise Err("Wrong password.", 401)
    st = tcp_state(k)
    if st == "revoked": raise Err("This password has been disabled.", 401)
    if st == "expired": raise Err("This password has expired. Buy a new one.", 401)
    devs = [x for x in str(k.get("devices") or "").split(",") if x]
    if login:
        upd = {}
        if dev and dev not in devs:
            if to_i(k.get("maxUsers")) and len(devs) >= to_i(k.get("maxUsers")): raise Err("This password has reached its user limit.", 401)
            devs.append(dev); upd["devices"] = ",".join(devs)
        if not to_i(k.get("startMs")):
            n = now_ms(); upd["startMs"] = n; upd["expiresMs"] = n + to_i(k.get("days")) * 86400000 if to_i(k.get("days")) else 0
            k["startMs"], k["expiresMs"] = upd["startMs"], upd["expiresMs"]
        if upd:
            try: commit([w_set("tcpKeys/" + key, upd, pre={"updateTime": kt})])
            except Err: raise Err("Try again.", 409)
    elif st != "active" or (dev and devs and dev not in devs): raise Err("Session expired. Login again with a valid password.", 401)
    return jsonify(ok=True, expires=to_i(k.get("expiresMs")) // 1000, now=int(time.time()), label=str(k.get("label") or ""))

@app.post("/tcpClaim")
@app.post("/api/tcpClaim")
def tcp_claim():
    b = request.get_json(silent=True) or {}
    ck = tcp_clean_key(b.get("key")); kd, _kt = fget("tcpKeys/" + ck) if ck else (None, None)
    if not kd or tcp_state(kd) != "active": raise Err("Login with a valid password first.", 401)
    free = [(i, d) for i, d in flist("tcpAccounts") if d.get("used") is not True]
    free.sort(key=lambda x: 0 if x[1].get("jwt") else 1)
    for gid, d in free[:5]:
        cur, ct = fget("tcpAccounts/" + gid)
        if not cur or cur.get("used") is True: continue
        try: commit([w_set("tcpAccounts/" + gid, {"used": True}, pre={"updateTime": ct}, now=["usedAt"])])
        except Err: continue
        return jsonify(ok=True, uid=gid, password=str(cur.get("password") or ""), jwt=str(cur.get("jwt") or ""), region=str(cur.get("region") or ""))
    raise Err("No free accounts in stock", 404)

@app.post("/tcpRegisterUrl")
@app.post("/api/tcpRegisterUrl")
def tcp_register_url():
    tcp_secret_ok(); u = str((request.get_json(silent=True) or {}).get("url") or "").strip()
    if not re.match(r"^https?://[^\s]{3,200}$", u): raise Err("bad url")
    commit([w_set("settings/tcp", {"tcpUrl": u})]); return jsonify(ok=True)

@app.post("/adminTcp")
@app.post("/api/adminTcp")
def admin_tcp():
    require_admin(); b = request.get_json(silent=True) or {}; act = str(b.get("action") or "")
    if act == "getConfig":
        c = tcp_settings(); g, _ = fget("settings/general"); g = g or {}
        return jsonify(ok=True, enabled=c["enabled"], hidden=c["hidden"], offMsg=c["offMsg"], tcpUrl=c["tcpUrl"], packages=c["packages"],
                       dailyLikePrice=to_i(g.get("dailyLikePrice"), 6) if g.get("dailyLikePrice") not in (None, "") else 6,
                       dailyLikeResellerPrice=(to_i(g.get("dailyLikeResellerPrice")) if g.get("dailyLikeResellerPrice") not in (None, "") else None))
    if act == "setConfig":
        pk = []
        for i, p in enumerate(b.get("packages") or []):
            d = to_i(p.get("days"))
            if not 1 <= d <= 365: raise Err("প্রতিটি প্যাকেজের দিন (1-365) সঠিক দিন")
            if to_i(p.get("price"), -1) < 0: raise Err("দাম সঠিক দিন")
            rp = p.get("resellerPrice")
            pk.append({"id": str(p.get("id") or f"p{i}")[:20], "name": str(p.get("name") or f"{d} Days")[:30], "days": d, "price": to_i(p.get("price")),
                       "resellerPrice": (to_i(rp) if rp not in (None, "") else None), "active": p.get("active") is not False})
        data = {"enabled": b.get("enabled") is not False, "hidden": b.get("hidden") is True, "offMsg": str(b.get("offMsg") or "")[:200], "packagesJson": json.dumps(pk)}
        if str(b.get("tcpUrl") or "").strip(): data["tcpUrl"] = str(b["tcpUrl"]).strip()[:300]
        ws = [w_set("settings/tcp", data)]
        gp = {}
        if b.get("dailyLikePrice") not in (None, ""): gp["dailyLikePrice"] = max(0, to_i(b["dailyLikePrice"]))
        if b.get("dailyLikeResellerPrice") not in (None, ""): gp["dailyLikeResellerPrice"] = max(0, to_i(b["dailyLikeResellerPrice"]))
        if gp: ws.append(w_set("settings/general", gp))
        commit(ws); return jsonify(ok=True)
    if act == "gen":
        days = to_i(b.get("days")); mu = max(0, to_i(b.get("maxUsers"), 1)); n = min(max(to_i(b.get("count"), 1), 1), 100)
        if days < 0 or days > 3650: raise Err("দিন সঠিক দিন (0 = আনলিমিটেড)")
        custom = tcp_clean_key(b.get("custom")) if n == 1 else ""
        keys = []
        for _ in range(n):
            try: keys.append(tcp_make_key(days, mu, b.get("label") or ("Unlimited" if not days else f"{days} Days"), source="admin", custom=custom))
            except Err: 
                if custom: raise Err("এই পাসওয়ার্ড আগে থেকেই আছে")
        return jsonify(ok=True, keys=keys)
    if act == "list":
        rows = [tcp_view(i, d) for i, d in flist("tcpKeys", limit=3000)]
        rows.sort(key=lambda r: r["key"]); return jsonify(ok=True, keys=rows, now=now_ms())
    if act == "setHidden":   # quick ON/OFF switch: hidden=True hides the TCP BOT button from the whole website
        commit([w_set("settings/tcp", {"hidden": b.get("hidden") is True})]); return jsonify(ok=True, hidden=b.get("hidden") is True)
    if act == "deleteMany":  # delete the first N keys (oldest first) or ALL keys
        rows = flist("tcpKeys", limit=5000); rows.sort(key=lambda r: (str(r[1].get("createdAt") or ""), r[0]))
        raw = str(b.get("count") or "").strip().lower()
        n = len(rows) if raw == "all" else to_i(raw)
        if n < 1: raise Err("কতগুলো ডিলিট করবেন সেই সংখ্যা দিন")
        ids = [i for i, _ in rows[:n]]
        for s in range(0, len(ids), 200): commit([w_del("tcpKeys/" + i) for i in ids[s:s + 200]])
        return jsonify(ok=True, deleted=len(ids))
    key = tcp_clean_key(b.get("key"))
    if act in ("revoke", "unrevoke"):
        if not fget("tcpKeys/" + key)[0]: raise Err("Not found", 404)
        commit([w_set("tcpKeys/" + key, {"revoked": act == "revoke"}, pre={"exists": True})]); return jsonify(ok=True)
    if act == "extend":
        k, kt = fget("tcpKeys/" + key)
        if not k: raise Err("Not found", 404)
        add = max(1, to_i(b.get("days"), 1)); upd = {"days": to_i(k.get("days")) + add}
        if to_i(k.get("expiresMs")): upd["expiresMs"] = to_i(k.get("expiresMs")) + add * 86400000
        commit([w_set("tcpKeys/" + key, upd, pre={"updateTime": kt})]); return jsonify(ok=True)
    if act == "delete":
        commit([w_del("tcpKeys/" + key)]); return jsonify(ok=True)
    if act == "accAdd":
        n = 0
        for ln in str(b.get("text") or "").splitlines():
            parts = [x for x in re.split(r"[\s,|:;]+", ln.strip()) if x]
            if not parts or not parts[0].isdigit(): continue
            uidg, pw, jwt_t, reg = parts[0], "", "", str(b.get("region") or "").upper()
            for x in parts[1:]:
                if x.count(".") == 2 and len(x) > 40: jwt_t = x
                elif x.upper() in LIKE_REGIONS: reg = x.upper()
                elif not pw: pw = x
            try: commit([w_set("tcpAccounts/" + uidg, {"password": pw, "jwt": jwt_t, "region": reg, "used": False}, pre={"exists": False}, now=["createdAt"])]); n += 1
            except Err: pass
        return jsonify(ok=True, added=n)
    if act == "accList":
        rows = [{"uid": i, "region": d.get("region") or "", "hasJwt": bool(d.get("jwt")), "hasPw": bool(d.get("password")), "used": d.get("used") is True} for i, d in flist("tcpAccounts", limit=5000)]
        return jsonify(ok=True, accounts=rows)
    if act == "accDelete":
        commit([w_del("tcpAccounts/" + re.sub(r"\D", "", str(b.get("uid") or "")))]); return jsonify(ok=True)
    if act == "accReset":
        commit([w_set("tcpAccounts/" + re.sub(r"\D", "", str(b.get("uid") or "")), {"used": False}, pre={"exists": True})]); return jsonify(ok=True)
    raise Err("Unknown action")


# ================= ADS / EARN  (shortener steps -> real money in the main balance) =================
# User taps "Start" on a step -> server remembers the start time and returns that step's shortener link.
# The shortener's destination is  <site>/?ads=<step secret key>.  When the user lands there, the site calls /adsComplete.
# Server checks: steps are done in order, the step was started, and at least `minSeconds` passed (too fast = auto-ban, if enabled).
# After the last step the reward is added to users/<uid>.balance (the one and only wallet).  Ad codes are only sent to the browser by /adsStatus (i.e. only when the user opens the Ads page).
ADS_DEFAULT_CODES = {"pageScripts": "", "banners": []}   # ad codes are set in index.html (MHX_AD_SCRIPT / MHX_AD_BANNERS)
ADS_KEY_RE = re.compile(r"[^A-Za-z0-9_-]")

def ads_day(): return datetime.now(timezone(timedelta(hours=6))).strftime("%Y%m%d")   # Bangladesh day
def ads_new_key(): return secrets.token_urlsafe(9)

def ads_settings():
    g, _ = fget("adsConfig/main"); g = g or {}
    try: raw = json.loads(g.get("stepsJson") or "null")
    except Exception: raw = None
    steps, seen = [], set()
    for i, s in enumerate(raw if isinstance(raw, list) else []):
        if not isinstance(s, dict): continue
        key = ADS_KEY_RE.sub("", str(s.get("key") or ""))[:40]
        if len(key) < 8 or key in seen: key = ads_new_key()
        seen.add(key)
        steps.append({"id": str(s.get("id") or f"s{i}")[:20], "name": str(s.get("name") or f"Step {len(steps) + 1}")[:40],
                      "url": str(s.get("url") or "").strip()[:500], "key": key})
    try: codes = json.loads(g.get("codesJson") or "null")
    except Exception: codes = None
    if not isinstance(codes, dict): codes = ADS_DEFAULT_CODES
    banners = [str(x) for x in (codes.get("banners") or []) if isinstance(x, str) and x.strip()][:2]   # max 2 banners
    en = g.get("enabled", False)
    return {"enabled": en is True or str(en).strip().lower() in ("on", "true", "1"), "offMsg": str(g.get("offMsg") or ""),
            "minSeconds": max(0, to_i(g.get("minSeconds"), 90)) if g.get("minSeconds") not in (None, "") else 90,
            "banOnFast": g.get("banOnFast", True) is not False, "reward": max(0, to_i(g.get("reward", g.get("bonus")), 0)),
            "dailyLimit": max(0, to_i(g.get("dailyLimit"), 0)), "siteUrl": str(g.get("siteUrl") or ""),
            "steps": steps, "codes": {"pageScripts": "", "banners": banners}}

def ads_sess(uid):
    s, st = fget("adsSessions/" + uid); s = s or {}
    day = ads_day()
    if s.get("day") != day: s["day"] = day; s["rounds"] = 0
    return s, st

def ads_user(uid):
    u, ut = fget("users/" + uid)
    if not u: raise Err("User not found", 404)
    if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
    return u, ut

def ads_sw(uid, st, s, **chg):
    """write for the user's progress doc (guarded by its updateTime so two taps can't both win)"""
    d = {"day": s.get("day") or ads_day(), "rounds": to_i(s.get("rounds")), "done": to_i(s.get("done")),
         "startStep": to_i(s.get("startStep"), -1), "startMs": to_i(s.get("startMs")), "roundId": str(s.get("roundId") or uuid.uuid4().hex)}
    d.update(chg)
    return w_set("adsSessions/" + uid, d, pre=({"updateTime": st} if st else {"exists": False}))

@app.post("/adsStatus")
@app.post("/api/adsStatus")
def ads_status():
    uid, _ = auth_user(); cfg = ads_settings(); ads_user(uid); s, _st = ads_sess(uid)
    total = len(cfg["steps"]); done = min(to_i(s.get("done")), total); on = cfg["enabled"] and total > 0
    return jsonify(ok=True, enabled=on, offMsg=cfg["offMsg"], reward=cfg["reward"], total=total, done=done, roundsToday=to_i(s.get("rounds")),
                   dailyLimit=cfg["dailyLimit"], warn=cfg["banOnFast"], steps=[{"name": x["name"], "done": i < done} for i, x in enumerate(cfg["steps"])],
                   codes=(cfg["codes"] if on else None))

@app.post("/adsStart")
@app.post("/api/adsStart")
def ads_start():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    okr, _w = rate_ok("adsst:" + uid, 20)
    if not okr: raise Err("Too many attempts, wait a minute.", 429)
    cfg = ads_settings()
    if not cfg["enabled"] or not cfg["steps"]: raise Err(cfg["offMsg"] or "এই মুহূর্তে অ্যাডস টাস্ক বন্ধ আছে।", 403)
    ads_user(uid); s, st = ads_sess(uid); done = to_i(s.get("done")); idx = to_i(b.get("step"), -1)
    if cfg["dailyLimit"] and to_i(s.get("rounds")) >= cfg["dailyLimit"]: raise Err("আজকের লিমিট শেষ হয়েছে, কাল আবার চেষ্টা করুন।")
    if idx != done or not 0 <= idx < len(cfg["steps"]): raise Err("আগের ধাপটি আগে শেষ করুন")
    url = cfg["steps"][idx]["url"]
    if not re.match(r"^https?://[^\s]{3,500}$", url): raise Err("এই ধাপের লিংক এখনো সেট করা হয়নি")
    try: commit([ads_sw(uid, st, s, startStep=idx, startMs=now_ms())])
    except Err: raise Err("আবার চেষ্টা করুন", 409)
    return jsonify(ok=True, url=url)

@app.post("/adsComplete")
@app.post("/api/adsComplete")
def ads_complete():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    okr, _w = rate_ok("adscp:" + uid, 20)
    if not okr: raise Err("Too many attempts, wait a minute.", 429)
    cfg = ads_settings()
    if not cfg["enabled"] or not cfg["steps"]: raise Err(cfg["offMsg"] or "এই মুহূর্তে অ্যাডস টাস্ক বন্ধ আছে।", 403)
    key = ADS_KEY_RE.sub("", str(b.get("key") or ""))[:40]
    idx = next((i for i, x in enumerate(cfg["steps"]) if key and _hmac.compare_digest(x["key"].encode(), key.encode())), -1)
    if idx < 0: raise Err("লিংকটি সঠিক নয় বা মেয়াদ শেষ")
    u, ut = ads_user(uid); s, st = ads_sess(uid); done = to_i(s.get("done")); total = len(cfg["steps"])
    if idx < done: raise Err("এই ধাপটি আপনি আগেই শেষ করেছেন")
    if idx > done: raise Err(f"ধাপগুলো ক্রমানুসারে শেষ করতে হবে — আগে ধাপ {done + 1} শেষ করুন")
    start = to_i(s.get("startMs"))
    if to_i(s.get("startStep"), -1) != idx or not start: raise Err("আগে অ্যাডস পেজ থেকে এই ধাপের ‘Start’ বাটন চাপুন")
    el = (now_ms() - start) / 1000.0
    if el < cfg["minSeconds"]:
        if cfg["banOnFast"]:
            fraud_ban(uid, "Ads: step completed too fast", f"step {idx + 1}: {el:.0f}s < {cfg['minSeconds']}s")
            raise Err("খুব দ্রুত ধাপ শেষ করায় আপনার অ্যাকাউন্ট ব্যান করা হয়েছে।", 403)
        try: commit([ads_sw(uid, st, s, startStep=-1, startMs=0)])
        except Err: pass
        raise Err("খুব দ্রুত শেষ হয়েছে। আবার ‘Start’ চেপে ধাপটি ঠিকভাবে শেষ করুন।")
    done += 1; reward = 0; com = 0; ref_by = None; rid = str(s.get("roundId") or uuid.uuid4().hex)
    if done >= total:
        reward = cfg["reward"]
        writes = [ads_sw(uid, st, s, done=0, rounds=to_i(s.get("rounds")) + 1, startStep=-1, startMs=0, roundId=uuid.uuid4().hex)]
        if reward > 0:
            # referral commission on ADS earnings only (default 5%, editable in Admin -> Referral Settings). Fractions are carried over so small rewards still add up.
            com, rem_new, ref_by, extra = 0, None, u.get("referredBy"), []
            try:
                pct = ref_ads_percent()
                if pct > 0 and ref_by and ref_by != uid and not u.get("refVoid"):
                    rf_u = fget("users/" + str(ref_by))[0]
                    if rf_u is not None and not rf_u.get("banned"):
                        tot = float(u.get("refAdsRem") or 0) + reward * pct / 100.0
                        com = int(tot + 1e-9); rem_new = round(tot - com, 4)
                        if com > 0:
                            extra = [w_set("users/" + str(ref_by), {}, mask=[], inc={"balance": com, "lbScore": com, "refEarned": com}),
                                     w_set("refEarnings/" + uuid.uuid4().hex, {"referrerId": ref_by, "fromUserId": uid, "fromName": (u.get("name") or "User")[:20], "amount": com, "base": reward, "kind": "ads"}, pre={"exists": False}, now=["date"])]
            except Exception as ex: print("ADS COMMISSION skipped:", type(ex).__name__, ex); com, rem_new, extra = 0, None, []
            writes += [w_set("users/" + uid, ({"refAdsRem": rem_new} if rem_new is not None else {}), mask=(["refAdsRem"] if rem_new is not None else []), inc={"balance": reward, "lbScore": reward}, pre={"updateTime": ut}),
                       w_set("deposits/" + uuid.uuid4().hex, {"userId": uid, "userEmail": u.get("email", ""), "amount": 0, "reward": reward, "method": "Ads reward", "status": "approved", "trxId": "ADS-" + rid[:10]},
                             pre={"exists": False}, now=["createdAt", "date"])] + extra
    else:
        writes = [ads_sw(uid, st, s, done=done, startStep=-1, startMs=0, roundId=rid)]
    try: commit(writes)
    except Err: raise Err("আবার চেষ্টা করুন", 409)
    if reward > 0:
        tg_notify("🎁", "ADS COMPLETED - BONUS", uid, u, [("💰", "Bonus", f"+৳{reward}"), ("📊", "Rounds today", to_i(s.get("rounds")) + 1)] + ([("🤝", f"Referral commission to {ref_by}", f"+৳{com}")] if com > 0 else []))
    return jsonify(ok=True, done=min(done, total), total=total, reward=reward, finished=done >= total)

@app.post("/adminAds")
@app.post("/api/adminAds")
def admin_ads():
    require_admin(); b = request.get_json(silent=True) or {}; act = str(b.get("action") or "")
    if act == "getConfig":
        c = ads_settings(); return jsonify(ok=True, **c)
    if act == "setConfig":
        steps, seen = [], set()
        for i, s in enumerate((b.get("steps") or [])[:12]):
            if not isinstance(s, dict): continue
            url = str(s.get("url") or "").strip()
            if url and not re.match(r"^https?://[^\s]{3,500}$", url): raise Err(f"ধাপ {i + 1}: শর্টনার লিংক সঠিক নয় (https:// দিয়ে শুরু হতে হবে)")
            key = ADS_KEY_RE.sub("", str(s.get("key") or ""))[:40]
            if len(key) < 8 or key in seen: key = ads_new_key()
            seen.add(key); steps.append({"id": str(s.get("id") or f"s{i}")[:20], "name": str(s.get("name") or f"Step {len(steps) + 1}")[:40], "url": url, "key": key})
        cd = b.get("codes") or {}
        codes = {"pageScripts": "", "banners": [str(x)[:6000] for x in (cd.get("banners") or []) if isinstance(x, str) and x.strip()][:2]}
        site = str(b.get("siteUrl") or "").strip()
        if site and not re.match(r"^https?://[^\s]{3,200}$", site): raise Err("Website URL সঠিক নয়")
        data = {"enabled": b.get("enabled") is True, "offMsg": str(b.get("offMsg") or "")[:200], "minSeconds": min(max(to_i(b.get("minSeconds")), 0), 3600),
                "banOnFast": b.get("banOnFast") is not False, "reward": min(max(to_i(b.get("reward", b.get("bonus"))), 0), 100000), "dailyLimit": min(max(to_i(b.get("dailyLimit")), 0), 1000),
                "siteUrl": site, "stepsJson": json.dumps(steps), "codesJson": json.dumps(codes)}
        commit([w_set("adsConfig/main", data)]); return jsonify(ok=True, steps=steps)
    if act == "stats":
        rows = flist("adsSessions", limit=5000)
        return jsonify(ok=True, users=len(rows), roundsToday=sum(to_i(d.get("rounds")) for _, d in rows if d.get("day") == ads_day()))
    raise Err("Unknown action")


# ================= WITHDRAW (Ads/Earn balance -> bKash / Nagad) =================
# User sends a request  -> the amount is taken from the balance right away (held) and a code like WD-XXXX-XXXX is created.
# Admin searches that code in Admin -> Withdraw Requests, sends the money by hand, then sets  Success / Cancelled(+reason) / Pending.
# Cancelled = the held money goes back to the user's balance.  Success / Pending = the money stays held.
WD_MIN = 200
WD_METHODS = ("bKash", "Nagad")
WD_STATES = ("pending", "success", "cancelled")
WD_NUM = re.compile(r"^01[3-9]\d{8}$")
WD_ALPHA = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

WD_DEFAULT_MSG = "⚠️ উইথড্র শুধু প্রতি মাসের ১ থেকে ৫ তারিখের মধ্যে করা যায়। অনুগ্রহ করে ১ তারিখ থেকে ৫ তারিখের মধ্যে আবার যোগাযোগ করুন। ধন্যবাদ!"
def wd_open(): return 1 <= datetime.now(timezone(timedelta(hours=6))).day <= 5      # Bangladesh date: 1,2,3,4,5 only
def wd_msg():
    d, _ = fget("settings/withdraw")
    return str((d or {}).get("warnMsg") or "").strip() or WD_DEFAULT_MSG

@app.post("/withdrawInfo")
@app.post("/api/withdrawInfo")
def withdraw_info():
    auth_user()
    return jsonify(ok=True, open=wd_open(), message=wd_msg(), min=WD_MIN)

def wd_code(): return "WD-" + "".join(secrets.choice(WD_ALPHA) for _ in range(4)) + "-" + "".join(secrets.choice(WD_ALPHA) for _ in range(4))
def wd_clean_code(c): return re.sub(r"[^A-Z0-9-]", "", str(c or "").upper().replace(" ", ""))[:20]
def wd_number(v):
    n = re.sub(r"\D", "", str(v or ""))
    if n.startswith("880") and len(n) == 13: n = "0" + n[3:]
    return n
def wd_view(code, d, admin=False):
    o = {"code": code, "amount": to_i(d.get("amount")), "method": d.get("method") or "", "number": d.get("number") or "", "status": d.get("status") or "pending",
         "reason": d.get("reason") or "", "ts": to_i(d.get("ts"))}
    if admin: o.update(userId=d.get("userId") or "", userName=d.get("userName") or "", userEmail=d.get("userEmail") or "", doneBy=d.get("doneBy") or "")
    return o

@app.post("/withdrawRequest")
@app.post("/api/withdrawRequest")
def withdraw_request():
    uid, _ = auth_user(); b = request.get_json(silent=True) or {}
    ok, _w = rate_ok(f"wd:{uid}", 5)
    if not ok: raise Err("একটু পরে আবার চেষ্টা করুন", 429)
    if not wd_open(): raise Err(wd_msg(), 403)
    method = str(b.get("method") or "")
    if method not in WD_METHODS: raise Err("bKash অথবা Nagad সিলেক্ট করুন")
    amt = to_i(b.get("amount"))
    if amt < WD_MIN: raise Err(f"মিনিমাম উইথড্র ৳{WD_MIN}")
    num, num2 = wd_number(b.get("number")), wd_number(b.get("number2"))
    if not WD_NUM.match(num): raise Err("সঠিক মোবাইল নাম্বার দিন (01XXXXXXXXX)")
    if num != num2: raise Err("দুটি নাম্বার মিলছে না")
    with user_lock(uid):
        for attempt in range(4):
            u, ut = fget("users/" + uid)
            if not u: raise Err("User not found", 404)
            if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
            if sum(1 for _, d in fquery("withdrawals", "userId", uid, limit=100) if d.get("status") == "pending") >= 3:
                raise Err("আপনার ৩টি উইথড্র রিকোয়েস্ট পেন্ডিং আছে, আগেরগুলো শেষ হওয়ার পর নতুন দিন")
            if to_i(u.get("balance")) < amt: raise Err("আপনার ব্যালেন্সে এত টাকা নেই")
            code = wd_code(); name = u.get("name") or (u.get("email") or "User").split("@")[0]
            try:
                commit([w_set("users/" + uid, {}, mask=[], inc={"balance": -amt}, pre={"updateTime": ut}),
                        w_set("withdrawals/" + code, {"userId": uid, "userName": str(name)[:40], "userEmail": u.get("email", ""), "amount": amt, "method": method, "number": num,
                                                      "status": "pending", "reason": "", "ts": now_ms()}, pre={"exists": False}, now=["createdAt"])])
                return jsonify(ok=True, code=code, amount=amt, method=method, number=num)
            except Err: time.sleep(0.2 * (attempt + 1))
    raise Err("রিকোয়েস্ট করা যায়নি, আবার চেষ্টা করুন", 409)

@app.post("/withdrawMine")
@app.post("/api/withdrawMine")
def withdraw_mine():
    uid, _ = auth_user()
    rows = [wd_view(c, d) for c, d in fquery("withdrawals", "userId", uid, limit=100)]
    rows.sort(key=lambda r: r["ts"], reverse=True)
    return jsonify(ok=True, items=rows[:50], min=WD_MIN)

@app.post("/adminWithdraw")
@app.post("/api/adminWithdraw")
def admin_withdraw():
    me = require_admin(); b = request.get_json(silent=True) or {}; act = str(b.get("action") or "list")
    if act == "getConfig": return jsonify(ok=True, message=wd_msg(), isDefault=not str((fget("settings/withdraw")[0] or {}).get("warnMsg") or "").strip(), open=wd_open())
    if act == "setConfig":
        m = str(b.get("message") or "").strip()[:500]
        commit([w_set("settings/withdraw", {"warnMsg": m})]); return jsonify(ok=True, message=wd_msg())
    if act == "update":
        code = wd_clean_code(b.get("code")); new = str(b.get("status") or ""); reason = str(b.get("reason") or "").strip()[:200]
        if new not in WD_STATES: raise Err("Status সঠিক নয়")
        if new == "cancelled" and not reason: raise Err("ক্যান্সেলের কারণ লিখুন")
        d0, _ = fget("withdrawals/" + code)
        if not d0: raise Err("Withdraw request পাওয়া যায়নি", 404)
        uid = str(d0.get("userId") or "")
        with user_lock(uid):
            d, dt = fget("withdrawals/" + code); old = d.get("status") or "pending"; amt = to_i(d.get("amount"))
            if old == new and new != "cancelled": return jsonify(ok=True, same=True)
            held = lambda s: s in ("pending", "success")
            ws = [w_set("withdrawals/" + code, {"status": new, "reason": reason if new == "cancelled" else "", "doneBy": "admin:" + me}, pre={"updateTime": dt}, now=["doneAt"])]
            if held(old) and not held(new): ws.append(w_set("users/" + uid, {}, mask=[], inc={"balance": amt}))   # money goes back
            elif held(new) and not held(old):                                                                                    # cancelled -> pending/success: take it again
                u, ut = fget("users/" + uid)
                if not u or to_i(u.get("balance")) < amt: raise Err("ইউজারের ব্যালেন্সে এখন এই টাকা নেই, তাই আবার পেন্ডিং/সাকসেস করা যাবে না")
                ws.append(w_set("users/" + uid, {}, mask=[], inc={"balance": -amt}, pre={"updateTime": ut}))
            try: commit(ws)
            except Err: raise Err("আবার চেষ্টা করুন", 409)
        return jsonify(ok=True, status=new)
    q = str(b.get("q") or "").strip(); flt = str(b.get("filter") or "all")
    if q:
        code = wd_clean_code(q); d, _ = fget("withdrawals/" + code) if len(code) >= 6 else (None, None)
        if d: return jsonify(ok=True, items=[wd_view(code, d, True)], stats=None)
    rows = fquery("withdrawals", order="ts", limit=300)
    stats = {"pending": sum(1 for _, d in rows if d.get("status") == "pending"), "pendingAmount": sum(to_i(d.get("amount")) for _, d in rows if d.get("status") == "pending")}
    items = [wd_view(c, d, True) for c, d in rows]
    if q:
        ql = q.lower(); items = [x for x in items if ql in (x["number"] + " " + x["userEmail"] + " " + x["userName"] + " " + x["userId"] + " " + x["code"]).lower()]
    if flt in WD_STATES: items = [x for x in items if x["status"] == flt]
    return jsonify(ok=True, items=items[:150], stats=stats)

if os.getenv("AUTOLIKE_SCHEDULER", "1") != "0":
    threading.Thread(target=al_loop, daemon=True, name="autolike").start()

threading.Thread(target=lb_migrate, daemon=True, name="lbmigrate").start()

if os.getenv("REFERRAL_WORKER", "1") != "0":
    threading.Thread(target=rf_loop, daemon=True, name="refrelease").start()

print("MHX backend started, version", APP_VERSION, flush=True)

# ======================================================================
# TOURNAMENT  (own wallet: tUsers/{uid}.balance — never touches users/{uid}.balance)
#   settings/tourney : hidden, colorsJson, refFixed, refPercent, minWithdraw, minDeposit, supportUrl, offMsg
#   tCats/{id}       : name, sub, image, active, order           (the square buttons on the tournament home)
#   tMatches/{id}    : catId, title, entryFee, prize, maxPlayers, startMs, revealMs, roomId, roomPass, status(open|done), playersJson, winnersJson, active
#   tUsers/{uid}     : name, photo, balance, refCode, refBy, refCount, refEarned, wins
#   tWithdrawals/{key}, tWins/{date_uid}, payments/{ref} (wallet="t")
# ======================================================================
T_BD = timezone(timedelta(hours=6))
T_COLORS = {"primary": "#059669", "bg": "#f7fdfa", "card": "#ffffff", "text": "#10201a", "accent": "#10b981"}
def _tf(x, d=0.0):
    try: return float(x)
    except Exception: return d
def t_cfg():
    g, _ = fget("settings/tourney"); g = g or {}
    try: col = json.loads(g.get("colorsJson") or "{}")
    except Exception: col = {}
    c = dict(T_COLORS); c.update({k: v for k, v in (col or {}).items() if k in c and re.fullmatch(r"#[0-9a-fA-F]{6}", str(v))})
    has = lambda k: g.get(k) not in (None, "")
    return {"hidden": g.get("hidden") is True or str(g.get("hidden")).lower() in ("true", "1", "on"), "colors": c,
            "refFixed": max(0, to_i(g.get("refFixed"))) if has("refFixed") else 1, "refPercent": max(0.0, _tf(g.get("refPercent"))) if has("refPercent") else 10.0,
            "minWithdraw": max(1, to_i(g.get("minWithdraw"))) if has("minWithdraw") else 50, "minLevel": max(0, to_i(g.get("minLevel"))) if has("minLevel") else 40, "minDeposit": max(1, to_i(g.get("minDeposit"))) if has("minDeposit") else 10,
            "supportUrl": str(g.get("supportUrl") or "https://t.me/mhxffv111"), "offMsg": str(g.get("offMsg") or "")}
def t_num_id():
    """unique 8-digit numeric user id shown to everybody"""
    for _ in range(30):
        n = secrets.randbelow(90000000) + 10000000
        if not fquery("tUsers", "numId", n, limit=1): return n
    raise Err("ID তৈরি করা যায়নি, আবার চেষ্টা করুন", 500)
def t_find(q):
    """admin lookup: numeric user id, support pin, or firebase uid -> (uid, doc) or raise"""
    q = str(q or "").strip()
    if not q: raise Err("ইউজার আইডি দিন")
    if q.isdigit() and len(q) <= 9:
        r = fquery("tUsers", "numId", int(q), limit=1)
        if r: return r[0]
    r = fquery("tUsers", "pin", q.upper(), limit=1)
    if r: return r[0]
    d, _ = fget("tUsers/" + re.sub(r"[^A-Za-z0-9]", "", q))
    if d: return re.sub(r"[^A-Za-z0-9]", "", q), d
    raise Err("এই আইডির কোনো ইউজার পাওয়া যায়নি (ইউজারকে একবার Tournament খুলতে হবে)", 404)
def t_gate(uid):
    tu, _ = fget("tUsers/" + uid)
    if tu and tu.get("banned") is True:
        raise Err("আপনি ব্যান হয়েছেন। কারণ: " + str(tu.get("banReason") or "অজানা"), 403)
def t_level_of(o, d=0):
    if d > 6: return None
    if isinstance(o, dict):
        for k, v in o.items():
            if str(k).lower().replace("_", "") in ("level", "accountlevel", "playerlevel", "lv") and str(v).strip().isdigit(): return int(str(v).strip())
        for v in o.values():
            x = t_level_of(v, d + 1)
            if x is not None: return x
    if isinstance(o, list):
        for v in o[:5]:
            x = t_level_of(v, d + 1)
            if x is not None: return x
    return None
def t_players(m):
    try: p = json.loads(m.get("playersJson") or "[]")
    except Exception: p = []
    return p if isinstance(p, list) else []
def t_open():
    c = t_cfg()
    if c["hidden"]: raise Err(c["offMsg"] or "Tournament সাময়িকভাবে বন্ধ আছে।", 403)
    return c
def t_pin(uid):
    u, _ = fget("users/" + uid); u = u or {}
    return (u.get("supportPin") or uid[:6]).upper(), u
def t_profile(uid, tu):
    pin, _ = t_pin(uid)
    return {"name": tu.get("name") or "Player", "photo": tu.get("photo") or "", "balance": to_i(tu.get("balance")), "refCode": tu.get("refCode") or uid[:8].upper(),
            "supportPin": pin, "numId": to_i(tu.get("numId")), "banned": tu.get("banned") is True, "banReason": tu.get("banReason") or "", "wins": to_i(tu.get("wins")), "refCount": to_i(tu.get("refCount")), "refEarned": to_i(tu.get("refEarned"))}
def t_credit_ref(referrer, amount, kind):
    """add money to a referrer's tournament wallet (+ log numbers)"""
    if not referrer or amount <= 0: return
    try: commit([w_set("tUsers/" + referrer, {}, mask=[], inc={"balance": amount, "refEarned": amount}, pre={"exists": True})])
    except Err: pass

@app.get("/tInfo")
@app.get("/api/tInfo")
def t_info():
    c = t_cfg(); _g, _ = fget("settings/general"); _g = _g or {}
    def _hx(k):
        v = str(_g.get(k) or "")
        return v if re.fullmatch(r"#[0-9a-fA-F]{6}", v) else ""
    return jsonify(ok=True, themeColor=_hx("themeColor"), bgColor=_hx("bgColor"), textColor=_hx("textColor"), hidden=c["hidden"], colors=c["colors"], supportUrl=c["supportUrl"], minWithdraw=c["minWithdraw"], minDeposit=c["minDeposit"])

@app.post("/tInit")
@app.post("/api/tInit")
def t_init():
    uid, claims = auth_user(); c = t_open(); b = request.get_json(silent=True) or {}
    tu, _ = fget("tUsers/" + uid)
    if not tu:
        pin0, u = t_pin(uid); name = u.get("name") or (claims.get("email") or "Player").split("@")[0]
        tu = {"name": str(name)[:30], "photo": "", "balance": 0, "refCode": uid[:8].upper(), "refBy": "", "refCount": 0, "refEarned": 0, "wins": 0, "numId": t_num_id(), "pin": pin0}
        refc = re.sub(r"[^A-Z0-9]", "", str(b.get("ref") or "").upper())[:12]
        if refc:
            for rid, rd in fquery("tUsers", "refCode", refc, limit=1):
                if rid != uid: tu["refBy"] = rid; t_credit_ref(rid, c["refFixed"], "signup"); commit([w_set("tUsers/" + rid, {}, mask=[], inc={"refCount": 1})])
        commit([w_set("tUsers/" + uid, tu, pre={"exists": False}, now=["createdAt"])])
    upd = {}
    if not tu.get("numId"): upd["numId"] = t_num_id()
    if not tu.get("pin"): upd["pin"] = t_pin(uid)[0]
    if upd: commit([w_set("tUsers/" + uid, upd, pre={"exists": True})]); tu.update(upd)
    return jsonify(ok=True, profile=t_profile(uid, tu), supportUrl=c["supportUrl"], minWithdraw=c["minWithdraw"], minDeposit=c["minDeposit"])

def t_match_view(mid, m, uid=None):
    pl = t_players(m)
    return {"id": mid, "catId": m.get("catId") or "", "title": m.get("title") or "", "entryFee": to_i(m.get("entryFee")), "prize": to_i(m.get("prize")),
            "maxPlayers": to_i(m.get("maxPlayers")), "joined": len(pl), "startMs": to_i(m.get("startMs")), "status": m.get("status") or "open",
            "players": [p.get("gameName") for p in pl], "mine": any(p.get("uid") == uid for p in pl), "winners": json.loads(m.get("winnersJson") or "[]") if m.get("winnersJson") else []}

@app.post("/tHome")
@app.post("/api/tHome")
def t_home():
    uid, _ = auth_user(); t_open(); t_gate(uid)
    cats = [{"id": i, "name": d.get("name") or "", "sub": d.get("sub") or "", "image": d.get("image") or "", "order": to_i(d.get("order"))} for i, d in flist("tCats", limit=200) if d.get("active") is not False]
    cats.sort(key=lambda x: (x["order"], x["name"]))
    ms = [t_match_view(i, d, uid) for i, d in flist("tMatches", limit=1000) if d.get("active") is not False]
    ms.sort(key=lambda x: x["startMs"])
    tu, _ = fget("tUsers/" + uid)
    return jsonify(ok=True, cats=cats, matches=ms, balance=to_i((tu or {}).get("balance")), now=now_ms())

@app.post("/tJoin")
@app.post("/api/tJoin")
def t_join():
    uid, _ = auth_user(); t_open(); t_gate(uid); b = request.get_json(silent=True) or {}
    mid = re.sub(r"[^A-Za-z0-9_-]", "", str(b.get("matchId") or ""))[:40]
    gname = str(b.get("gameName") or "").strip()[:30]; guid = re.sub(r"\D", "", str(b.get("gameUid") or ""))[:15]
    if not gname or len(guid) < 5: raise Err("ইন-গেম আইডির নাম ও সঠিক UID দিন")
    with user_lock(uid):
        for a in range(6):
            m, mt = fget("tMatches/" + mid); tu, ut = fget("tUsers/" + uid); _, u = t_pin(uid)
            if not m or m.get("active") is False: raise Err("ম্যাচ পাওয়া যায়নি", 404)
            if not tu: raise Err("আগে Tournament-এ ঢুকুন")
            if u.get("banned"): raise Err("আপনার অ্যাকাউন্টটি ব্যান করা আছে", 403)
            if (m.get("status") or "open") != "open" or (to_i(m.get("startMs")) and now_ms() >= to_i(m.get("startMs"))): raise Err("এই ম্যাচের জয়েন শেষ")
            pl = t_players(m)
            if any(p.get("uid") == uid for p in pl): raise Err("আপনি আগেই জয়েন করেছেন")
            if len(pl) >= to_i(m.get("maxPlayers")): raise Err("ম্যাচ ফুল হয়ে গেছে")
            fee = to_i(m.get("entryFee"))
            if to_i(tu.get("balance")) < fee: raise Err("Tournament ব্যালেন্সে পর্যাপ্ত টাকা নেই, Add Money করুন")
            pl.append({"uid": uid, "name": tu.get("name") or "", "gameName": gname, "gameUid": guid, "ts": now_ms()})
            try:
                commit([w_set("tUsers/" + uid, {}, mask=[], inc={"balance": -fee}, pre={"updateTime": ut}),
                        w_set("tMatches/" + mid, {"playersJson": json.dumps(pl)}, pre={"updateTime": mt})])
                return jsonify(ok=True, balance=to_i(tu.get("balance")) - fee)
            except Err: time.sleep(0.15 * (a + 1))
    raise Err("জয়েন করা যায়নি, আবার চেষ্টা করুন", 409)

@app.post("/tRoom")
@app.post("/api/tRoom")
def t_room():
    uid, _ = auth_user(); t_open(); t_gate(uid); mid = re.sub(r"[^A-Za-z0-9_-]", "", str((request.get_json(silent=True) or {}).get("matchId") or ""))[:40]
    m, _ = fget("tMatches/" + mid)
    if not m or not any(p.get("uid") == uid for p in t_players(m)): raise Err("আপনি এই ম্যাচে জয়েন করেননি", 403)
    rev = to_i(m.get("revealMs")) or to_i(m.get("startMs"))
    if now_ms() < rev or not m.get("roomId"): return jsonify(ok=True, ready=False, revealMs=rev)
    return jsonify(ok=True, ready=True, roomId=m.get("roomId"), roomPass=m.get("roomPass") or "")

@app.post("/tProfile")
@app.post("/api/tProfile")
def t_profile_save():
    uid, _ = auth_user(); t_open(); t_gate(uid); b = request.get_json(silent=True) or {}; upd = {}
    if str(b.get("name") or "").strip(): upd["name"] = str(b["name"]).strip()[:30]
    ph = str(b.get("photo") or "")
    if ph:
        if not ph.startswith("data:image/") or len(ph) > 60000: raise Err("ছবি ছোট করে দিন")
        upd["photo"] = ph
    if not upd: raise Err("কিছু পরিবর্তন করা হয়নি")
    commit([w_set("tUsers/" + uid, upd, pre={"exists": True})]); tu, _ = fget("tUsers/" + uid)
    return jsonify(ok=True, profile=t_profile(uid, tu))

@app.post("/tDepositCreate")
@app.post("/api/tDepositCreate")
def t_deposit_create():
    if not API_KEY: raise Err("Server setup: ZRPay API key is empty.", 500)
    uid, claims = auth_user(); c = t_open(); t_gate(uid); b = request.get_json(silent=True) or {}
    amount = to_i(b.get("amount"))
    if amount < c["minDeposit"] or amount > 100000: raise Err(f"মিনিমাম ডিপোজিট ৳{c['minDeposit']}")
    tu, _ = fget("tUsers/" + uid)
    if not tu: raise Err("আগে Tournament-এ ঢুকুন")
    _, u = t_pin(uid)
    if u.get("banned"): raise Err("Account banned", 403)
    origin = str(b.get("origin", "")).rstrip("/"); origin = origin if origin.startswith("http") else DEFAULT_ORIGIN
    ref = uuid.uuid4().hex
    ctx = {"amt": amount, "ref": ref, "uid": uid, "nm": tu.get("name") or "User", "em": u.get("email") or claims.get("email") or "user@example.com",
           "ph": str(u.get("phone") or u.get("mobile") or "01700000000"), "ok": f"{origin}/tournament.html?payok={ref}", "bad": f"{origin}/tournament.html", "hook": f"{origin}/api/health"}
    url, res = gateway_create(ctx); gid = str(pick(res, ["invoice_id", "payment_id", "transaction_id", "trx_id", "id"]) or ref)
    commit([w_set(f"payments/{ref}", {"userId": uid, "email": u.get("email", ""), "amount": amount, "status": "pending", "gatewayId": gid, "wallet": "t"}, pre={"exists": False}, now=["createdAt"])])
    return jsonify(paymentUrl=url)

@app.post("/tDepositVerify")
@app.post("/api/tDepositVerify")
def t_deposit_verify():
    uid, _ = auth_user(); ref = str((request.get_json(silent=True) or {}).get("ref", ""))
    p, pt = fget("payments/" + ref)
    if not p or p.get("userId") != uid or p.get("wallet") != "t": raise Err("Payment not found", 404)
    if p.get("status") == "paid": return jsonify(ok=True, amount=p["amount"], already=True)
    res = gateway_verify(p.get("gatewayId") or ref, ref)
    if not is_paid(res): return jsonify(ok=False, status=str(pick(res, ["status"]) or ""))
    paid = to_i(pick(res, ["amount"]))
    if paid and paid < p["amount"]: raise Err("Amount mismatch", 400)
    amt = to_i(p["amount"]); c = t_cfg(); tu, _ = fget("tUsers/" + uid)
    ws = [w_set("payments/" + ref, {"status": "paid"}, pre={"updateTime": pt}, now=["paidAt"]), w_set("tUsers/" + uid, {}, mask=[], inc={"balance": amt}, pre={"exists": True})]
    commit(ws)     # the updateTime check on the payment doc makes a double credit impossible
    rb = (tu or {}).get("refBy")
    if rb: t_credit_ref(rb, int(amt * c["refPercent"] / 100), "commission")
    return jsonify(ok=True, amount=amt)

@app.post("/tWdRequest")
@app.post("/api/tWdRequest")
def t_wd_request():
    uid, _ = auth_user(); c = t_open(); t_gate(uid); b = request.get_json(silent=True) or {}
    ok, _w = rate_ok(f"twd:{uid}", 5)
    if not ok: raise Err("একটু পরে আবার চেষ্টা করুন", 429)
    method = str(b.get("method") or "")
    if method not in ("bKash", "Nagad"): raise Err("bKash অথবা Nagad সিলেক্ট করুন")
    amt = to_i(b.get("amount"))
    if amt < c["minWithdraw"]: raise Err(f"মিনিমাম উইথড্র ৳{c['minWithdraw']}")
    num = re.sub(r"\D", "", str(b.get("number") or ""))
    if num.startswith("880") and len(num) == 13: num = "0" + num[3:]
    if not re.fullmatch(r"01\d{9}", num) or num != (re.sub(r"\D", "", str(b.get("number2") or "")) if not str(b.get("number2") or "").startswith("880") else "0" + re.sub(r"\D", "", str(b.get("number2")))[3:]): raise Err("সঠিক নাম্বার দিন, দুটি নাম্বার মিলতে হবে")
    with user_lock(uid):
        for a in range(4):
            tu, ut = fget("tUsers/" + uid)
            if not tu: raise Err("User not found", 404)
            if to_i(tu.get("balance")) < amt: raise Err("Tournament ব্যালেন্সে এত টাকা নেই")
            key = "TW-" + "".join(secrets.choice(TCP_ALPHA) for _ in range(4)) + "-" + "".join(secrets.choice(TCP_ALPHA) for _ in range(4))
            try:
                commit([w_set("tUsers/" + uid, {}, mask=[], inc={"balance": -amt}, pre={"updateTime": ut}),
                        w_set("tWithdrawals/" + key, {"userId": uid, "userName": tu.get("name") or "", "amount": amt, "method": method, "number": num, "status": "pending", "reason": "", "ts": now_ms()}, pre={"exists": False}, now=["createdAt"])])
                return jsonify(ok=True, key=key, amount=amt)
            except Err: time.sleep(0.2 * (a + 1))
    raise Err("রিকোয়েস্ট করা যায়নি, আবার চেষ্টা করুন", 409)

def t_wd_view(k, d, admin=False):
    o = {"key": k, "amount": to_i(d.get("amount")), "method": d.get("method") or "", "number": d.get("number") or "", "status": d.get("status") or "pending", "reason": d.get("reason") or "", "ts": to_i(d.get("ts"))}
    if admin: o.update(userId=d.get("userId") or "", userName=d.get("userName") or "")
    return o

@app.post("/tWdMine")
@app.post("/api/tWdMine")
def t_wd_mine():
    uid, _ = auth_user(); rows = [t_wd_view(k, d) for k, d in fquery("tWithdrawals", "userId", uid, limit=100)]
    rows.sort(key=lambda r: r["ts"], reverse=True); return jsonify(ok=True, items=rows[:60])

_T_LB = {}
@app.post("/tLeaderboard")
@app.post("/api/tLeaderboard")
def t_leaderboard():
    auth_user(); today = datetime.now(T_BD).date(); y = (today - timedelta(days=1)).isoformat()   # one finished day -> changes once a day
    if y in _T_LB: return jsonify(ok=True, date=y, rows=_T_LB[y])
    rows = fquery("tWins", "date", y, limit=500); day = y
    if not rows: rows = fquery("tWins", "date", today.isoformat(), limit=500); day = today.isoformat()
    out = sorted([{"name": d.get("name") or "Player", "photo": d.get("photo") or "", "wins": to_i(d.get("wins")), "amount": to_i(d.get("amount"))} for _, d in rows], key=lambda r: (-r["wins"], -r["amount"]))[:20]
    if day == y: _T_LB.clear(); _T_LB[y] = out
    return jsonify(ok=True, date=day, rows=out)

def t_id(v): return re.sub(r"[^A-Za-z0-9_-]", "", str(v or ""))[:40]

@app.post("/adminTourney")
@app.post("/api/adminTourney")
def admin_tourney():
    require_admin(); b = request.get_json(silent=True) or {}; act = str(b.get("action") or "")
    if act == "getConfig": return jsonify(ok=True, **t_cfg())
    if act == "setConfig":
        cols = {k: v for k, v in (b.get("colors") or {}).items() if k in T_COLORS and re.fullmatch(r"#[0-9a-fA-F]{6}", str(v))}
        d = {"hidden": b.get("hidden") is True, "colorsJson": json.dumps(cols), "refFixed": max(0, to_i(b.get("refFixed"))), "refPercent": max(0.0, _tf(b.get("refPercent"))),
             "minWithdraw": max(1, to_i(b.get("minWithdraw"), 50)), "minLevel": max(0, to_i(b.get("minLevel"), 40)), "minDeposit": max(1, to_i(b.get("minDeposit"), 10)), "supportUrl": str(b.get("supportUrl") or "")[:300], "offMsg": str(b.get("offMsg") or "")[:200]}
        commit([w_set("settings/tourney", d)]); return jsonify(ok=True)
    if act == "setHidden": commit([w_set("settings/tourney", {"hidden": b.get("hidden") is True})]); return jsonify(ok=True)
    if act == "catList": return jsonify(ok=True, cats=[dict(d, id=i) for i, d in flist("tCats", limit=300)])
    if act == "catSave":
        cid = t_id(b.get("id")) or "c" + uuid.uuid4().hex[:8]; img = str(b.get("image") or "")
        if img and not (img.startswith("https://") or (img.startswith("data:image/") and len(img) < 200000)): raise Err("ছবি ঠিক নেই / বেশি বড়")
        commit([w_set("tCats/" + cid, {"name": str(b.get("name") or "")[:40], "sub": str(b.get("sub") or "")[:60], "image": img, "order": to_i(b.get("order")), "active": b.get("active") is not False})]); return jsonify(ok=True)
    if act == "catDel": commit([w_del("tCats/" + t_id(b.get("id")))]); return jsonify(ok=True)
    if act == "matchList":
        rows = [dict(d, id=i, joined=len(t_players(d))) for i, d in flist("tMatches", limit=1000)]; rows.sort(key=lambda r: -to_i(r.get("startMs")))
        for r in rows: r["players"] = t_players(r); r.pop("playersJson", None)
        return jsonify(ok=True, matches=rows)
    if act == "matchSave":
        mid = t_id(b.get("id")) or "m" + uuid.uuid4().hex[:8]
        d = {"catId": t_id(b.get("catId")), "title": str(b.get("title") or "")[:80], "entryFee": max(0, to_i(b.get("entryFee"))), "prize": max(0, to_i(b.get("prize"))),
             "maxPlayers": max(1, to_i(b.get("maxPlayers"), 2)), "startMs": to_i(b.get("startMs")), "revealMs": to_i(b.get("revealMs")), "roomId": str(b.get("roomId") or "")[:40],
             "roomPass": str(b.get("roomPass") or "")[:40], "active": b.get("active") is not False}
        old, _ = fget("tMatches/" + mid)
        if not old: d.update(status="open", playersJson="[]")
        commit([w_set("tMatches/" + mid, d)]); return jsonify(ok=True, id=mid)
    if act == "matchDel":
        mid = t_id(b.get("id")); m, _ = fget("tMatches/" + mid); ws = []
        if m and (m.get("status") or "open") == "open":      # refund everybody who joined an unfinished match
            for p in t_players(m): ws.append(w_set("tUsers/" + p["uid"], {}, mask=[], inc={"balance": to_i(m.get("entryFee"))}, pre={"exists": True}))
        for s in range(0, len(ws), 100): commit(ws[s:s + 100])
        commit([w_del("tMatches/" + mid)]); return jsonify(ok=True, refunded=len(ws))
    if act == "settle":      # winners: [{uid, amount}]  -> credit tournament wallet + daily leaderboard
        mid = t_id(b.get("matchId")); m, mt = fget("tMatches/" + mid)
        if not m or (m.get("status") or "open") == "done": raise Err("ম্যাচ পাওয়া যায়নি / আগেই শেষ")
        day = datetime.now(T_BD).date().isoformat(); ws = []; win = []; pl = {p["uid"]: p for p in t_players(m)}
        for w in b.get("winners") or []:
            wu, am = str(w.get("uid") or ""), max(0, to_i(w.get("amount")))
            if wu not in pl or am <= 0: raise Err("উইনার বা টাকা সঠিক নয়")
            tu, _ = fget("tUsers/" + wu)
            ws += [w_set("tUsers/" + wu, {}, mask=[], inc={"balance": am, "wins": 1}, pre={"exists": True}),
                   w_set(f"tWins/{day}_{wu}", {"date": day, "uid": wu, "name": (tu or {}).get("name") or pl[wu].get("gameName") or "Player", "photo": ((tu or {}).get("photo") or "")[:60000]}, inc={"wins": 1, "amount": am})]
            win.append({"uid": wu, "name": pl[wu].get("gameName"), "amount": am})
        if not win: raise Err("কমপক্ষে ১জন উইনার দিন")
        ws.append(w_set("tMatches/" + mid, {"status": "done", "winnersJson": json.dumps(win)}, pre={"updateTime": mt}))
        commit(ws); _T_LB.clear(); return jsonify(ok=True)
    if act == "wdList":
        q = t_id(b.get("q")).upper()
        if q: d, _ = fget("tWithdrawals/" + q); rows = [t_wd_view(q, d, True)] if d else []
        else: rows = [t_wd_view(k, d, True) for k, d in flist("tWithdrawals", limit=2000)]
        rows.sort(key=lambda r: r["ts"], reverse=True); return jsonify(ok=True, items=rows[:300])
    if act in ("wdDone", "wdReject"):
        k = t_id(b.get("key")).upper(); d, dt = fget("tWithdrawals/" + k)
        if not d or d.get("status") != "pending": raise Err("পেন্ডিং রিকোয়েস্ট পাওয়া যায়নি")
        ws = [w_set("tWithdrawals/" + k, {"status": "done" if act == "wdDone" else "rejected", "reason": str(b.get("reason") or "")[:200]}, pre={"updateTime": dt})]
        if act == "wdReject": ws.append(w_set("tUsers/" + d["userId"], {}, mask=[], inc={"balance": to_i(d.get("amount"))}, pre={"exists": True}))
        commit(ws); return jsonify(ok=True)
    if act == "findUser":
        uid2, d = t_find(b.get("q")); return jsonify(ok=True, user={"uid": uid2, "numId": to_i(d.get("numId")), "name": d.get("name") or "", "balance": to_i(d.get("balance")), "banned": d.get("banned") is True, "banReason": d.get("banReason") or "", "pin": d.get("pin") or "", "wins": to_i(d.get("wins"))})
    if act == "adjust":      # mode add = balance যোগ, mode sub = balance কাটা  (amount is always a plain positive number)
        am = abs(to_i(b.get("amount"))); mode = str(b.get("mode") or "")
        if am < 1 or mode not in ("add", "sub"): raise Err("টাকার পরিমাণ লিখুন")
        uid2, d = t_find(b.get("q"))
        if mode == "sub" and to_i(d.get("balance")) < am: raise Err(f"ইউজারের ব্যালেন্স মাত্র ৳{to_i(d.get('balance'))}, এত কাটা যাবে না")
        commit([w_set("tUsers/" + uid2, {}, mask=[], inc={"balance": am if mode == "add" else -am}, pre={"exists": True})])
        return jsonify(ok=True, balance=to_i(d.get("balance")) + (am if mode == "add" else -am), name=d.get("name") or "")
    if act in ("ban", "unban"):
        uid2, d = t_find(b.get("q")); reason = str(b.get("reason") or "").strip()[:200]
        if act == "ban" and not reason: raise Err("ব্যান করার কারণ লিখতে হবে")
        commit([w_set("tUsers/" + uid2, {"banned": act == "ban", "banReason": reason if act == "ban" else ""}, pre={"exists": True})]); return jsonify(ok=True)
    if act == "lowList":
        rows = [dict(d, id=i) for i, d in flist("tLowLevel", limit=2000)]; rows.sort(key=lambda r: -to_i(r.get("ts")))
        return jsonify(ok=True, items=[{k: r.get(k) for k in ("id", "numId", "pin", "userName", "gameName", "gameUid", "region", "level", "need", "fee", "matchTitle", "ts")} for r in rows[:500]], total=sum(to_i(r.get("fee")) for r in rows))
    if act == "userList":
        out = []
        for i, d in flist("tUsers", limit=3000):
            upd = {}
            if not d.get("numId"): upd["numId"] = t_num_id()
            if not d.get("pin"): upd["pin"] = t_pin(i)[0]
            if upd:
                try: commit([w_set("tUsers/" + i, upd, pre={"exists": True})]); d.update(upd)
                except Err: pass
            out.append({"uid": i, "numId": to_i(d.get("numId")), "pin": d.get("pin") or "", "name": d.get("name") or "", "balance": to_i(d.get("balance")), "wins": to_i(d.get("wins")), "banned": d.get("banned") is True})
        return jsonify(ok=True, users=out)
    if act == "broadcast":   # instant phone notification to everyone who enabled push
        title = str(b.get("title") or "Tournament").strip()[:80]; body = str(b.get("message") or "").strip()[:300]
        if not body: raise Err("মেসেজ লিখুন")
        g, _ = fget("settings/general"); icon = str((g or {}).get("logoUrl") or ""); icon = icon if icon.startswith("https://") else ""
        toks = all_push_tokens()
        if toks: threading.Thread(target=push_worker, args=(toks, title, body, DEFAULT_ORIGIN + "/tournament.html", icon), daemon=True).start()
        return jsonify(ok=True, queued=len(toks))
    raise Err("Unknown action")


if __name__ == "__main__":
    port = int(os.getenv("SERVER_PORT") or os.getenv("PORT") or 8080)
    app.run(host="0.0.0.0", port=port, threaded=True)
