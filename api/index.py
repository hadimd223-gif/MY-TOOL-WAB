# ZRPayBD payment server (Python) — runs free on Termux / any free host. No paid Firebase plan needed.
# Uses only pure-Python libraries (works on Android Termux).
import os, json, time, uuid, requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.oauth2 import service_account, id_token
from google.auth.transport.requests import Request as GReq, AuthorizedSession

# ---- load .env (keys stay on the server, never in HTML) ----
if os.path.exists(".env"):
    for ln in open(".env", encoding="utf-8"):
        ln = ln.strip()
        if ln and not ln.startswith("#") and "=" in ln:
            k, v = ln.split("=", 1); os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

PROJECT = os.getenv("PROJECT_ID", "m-h-x-wab-7abdf")
KEYFILE = os.getenv("FIREBASE_KEY_FILE", "serviceAccount.json")
API_KEY, SECRET_KEY, BRAND_KEY = (os.getenv(k, "") for k in ("ZRPAYBD_API_KEY", "ZRPAYBD_SECRET_KEY", "ZRPAYBD_BRAND_KEY"))
CREATE_URL = os.getenv("ZRPAYBD_CREATE_URL", "https://pay.zrpaybd.xyz/api/payment/create")
VERIFY_URL = os.getenv("ZRPAYBD_VERIFY_URL", "https://pay.zrpaybd.xyz/api/payment/verify")

SCOPES = ["https://www.googleapis.com/auth/datastore"]
if os.getenv("FIREBASE_KEY_JSON"):   # for Vercel: paste the whole JSON into this variable
    creds = service_account.Credentials.from_service_account_info(json.loads(os.environ["FIREBASE_KEY_JSON"]), scopes=SCOPES)
else:
    creds = service_account.Credentials.from_service_account_file(KEYFILE, scopes=SCOPES)
sess = AuthorizedSession(creds)
DB = f"projects/{PROJECT}/databases/(default)/documents"
ROOT = "https://firestore.googleapis.com/v1/"
app = Flask(__name__); CORS(app)

class Err(Exception):
    def __init__(s, m, c=400): s.m, s.c = m, c
@app.errorhandler(Err)
def _e(e): return jsonify(error=e.m), e.c

# ---- tiny Firestore REST helpers ----
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
def fget(path):
    r = sess.get(ROOT + DB + "/" + path)
    if r.status_code == 404: return None, None
    r.raise_for_status(); d = r.json()
    return {k: dec(v) for k, v in d.get("fields", {}).items()}, d["updateTime"]
def commit(writes):
    r = sess.post(ROOT + DB + ":commit", json={"writes": writes})
    if not r.ok: raise Err("Database error: " + r.text[:200], 500)
def w_set(path, data, mask=None, pre=None, inc=None, now=None):
    w = {"update": {"name": f"{DB}/{path}", "fields": {k: enc(v) for k, v in data.items()}},
         "updateMask": {"fieldPaths": mask if mask is not None else list(data.keys())}}
    tf = [{"fieldPath": k, "increment": {"integerValue": str(v)}} for k, v in (inc or {}).items()]
    tf += [{"fieldPath": k, "setToServerValue": "REQUEST_TIME"} for k in (now or [])]
    if tf: w["updateTransforms"] = tf
    if pre: w["currentDocument"] = pre
    return w

def auth_user():
    h = request.headers.get("Authorization", "")
    if not h.startswith("Bearer "): raise Err("Login required", 401)
    try: c = id_token.verify_firebase_token(h[7:], GReq(), audience=PROJECT)
    except Exception: raise Err("Invalid login, please login again", 401)
    return c.get("user_id") or c.get("sub"), c

def zr(url, body):
    hd = {"Content-Type": "application/json", "Accept": "application/json", "API-KEY": API_KEY,
          "SECRET-KEY": SECRET_KEY, "BRAND-KEY": BRAND_KEY, "RT-UDDOKTAPAY-API-KEY": API_KEY}
    body = {**body, "api_key": API_KEY, "secret_key": SECRET_KEY, "brand_key": BRAND_KEY}
    r = requests.post(url, json=body, headers=hd, timeout=30)
    try: j = r.json()
    except Exception: j = {"raw": r.text[:300]}
    print("ZRPay", url.split("/")[-1], r.status_code, r.text[:400])
    return j

def pick(o, keys):
    if isinstance(o, dict):
        for k in keys:
            if o.get(k) not in (None, "", {}): return o[k]
        for v in o.values():
            x = pick(v, keys)
            if x: return x
    return None

@app.get("/health")
@app.get("/api/health")
def health(): return jsonify(ok=True)

@app.post("/createPayment")
@app.post("/api/createPayment")
def create_payment():
    uid, claims = auth_user(); b = request.get_json(silent=True) or {}
    g, _ = fget("settings/general"); g = g or {}
    try: amount = int(float(b.get("amount", 0)))
    except Exception: raise Err("Invalid amount")
    if g.get("gatewayOff"): raise Err("Payments are temporarily disabled", 403)
    if amount < int(g.get("minDeposit", 10)) or amount > 100000: raise Err("Invalid amount")
    u, _ = fget("users/" + uid); u = u or {}
    if u.get("banned"): raise Err("Account banned", 403)
    origin = str(b.get("origin", "")).rstrip("/")
    if not origin.startswith("http"): raise Err("Bad origin")
    ref = uuid.uuid4().hex
    res = zr(CREATE_URL, {"full_name": u.get("name") or "User", "email": u.get("email") or claims.get("email") or "user@example.com",
        "amount": str(amount), "metadata": {"ref": ref, "uid": uid}, "redirect_url": f"{origin}/index.html?payok={ref}",
        "cancel_url": f"{origin}/index.html", "brand_key": BRAND_KEY})
    url = pick(res, ["payment_url", "paymentUrl", "checkout_url", "redirect_url", "payment_link", "link", "url"])
    if not (isinstance(url, str) and url.startswith("http")):
        raise Err("ZRPay error: " + str(res)[:300], 502)
    gid = str(pick(res, ["invoice_id", "payment_id", "transaction_id", "trx_id", "id"]) or "")
    commit([w_set(f"payments/{ref}", {"userId": uid, "email": u.get("email", ""), "amount": amount, "status": "pending", "gatewayId": gid},
                  pre={"exists": False}, now=["createdAt"])])
    return jsonify(paymentUrl=url)

@app.post("/verifyPayment")
@app.post("/api/verifyPayment")
def verify_payment():
    uid, _ = auth_user(); ref = str((request.get_json(silent=True) or {}).get("ref", ""))
    p, ptime = fget("payments/" + ref)
    if not p or p.get("userId") != uid: raise Err("Payment not found", 404)
    if p.get("status") == "paid": return jsonify(ok=True, amount=p["amount"], already=True)
    gid = p.get("gatewayId")
    res = zr(VERIFY_URL, {"invoice_id": gid, "payment_id": gid, "id": gid, "ref": ref})
    st = str(pick(res, ["status", "payment_status"]) or "").lower()
    if st not in ("completed", "paid", "success", "successful"): return jsonify(ok=False, status=st)
    try: paid = int(float(pick(res, ["amount"]) or 0))
    except Exception: paid = 0
    if paid and paid < p["amount"]: raise Err("Amount mismatch", 400)
    g, _ = fget("settings/general"); th = int((g or {}).get("resellerThreshold", 500))
    u, _ = fget("users/" + uid); u = u or {}
    amt = p["amount"]; total = int(u.get("totalDeposit", 0)) + amt
    promote = total >= th and (u.get("role") or "user") == "user"
    upd = {"role": "reseller"} if promote else {}
    # one atomic commit; precondition on updateTime stops double-crediting
    commit([
        w_set("payments/" + ref, {"status": "paid"}, pre={"updateTime": ptime}, now=["paidAt"]),
        w_set("users/" + uid, upd, mask=list(upd.keys()), inc={"balance": amt, "points": amt, "totalDeposit": amt}),
        w_set("deposits/" + uuid.uuid4().hex, {"userId": uid, "amount": amt, "method": "ZRPay (Auto)", "status": "approved", "trxId": gid or ref},
              pre={"exists": False}, now=["createdAt"]),
    ])
    return jsonify(ok=True, amount=amt, promoted=promote)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
