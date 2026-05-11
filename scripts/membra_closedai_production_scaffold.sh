#!/usr/bin/env bash
set -euo pipefail

# MEMBRA / ClosedAI Production Scaffold
# One-file installer that creates a production-oriented app with:
# - Chat-to-Idea Artifact ingestion
# - Image "reproducidescribe" visual provenance ingestion
# - GitHub provenance commits
# - Pinata/IPFS pinning
# - Stripe Identity KYC sessions
# - Stripe Checkout payment links
# - EVM onchain hash anchoring via real signed transaction
# - Public ledger and proof-of-idea metadata
# - Optional ERC-5192-style non-transferable Proof-of-Idea contract scaffold
#
# NO MOCK MODE:
# This scaffold does not fabricate KYC, payment, IPFS, GitHub, or onchain success.
# Runtime endpoints fail closed unless required real credentials are configured.
#
# PRIVATE KEY WARNING:
# Never paste private keys, seed phrases, identity documents, or secrets into GitHub.
# If using EVM anchoring/deploy, provide EVM_PRIVATE_KEY only as an environment variable
# on your own machine/server. Never send it to ChatGPT.

APP_DIR="${APP_DIR:-membra-closedai-production}"
BRANCH="${BRANCH:-main}"

echo "Creating MEMBRA production scaffold in: ${APP_DIR}"
mkdir -p "${APP_DIR}"/{backend,contracts,scripts,provenance,accounting,visuals,deploy}

cat > "${APP_DIR}/.env.example" <<'EOF'
# ===== CORE APP =====
APP_NAME="MEMBRA ClosedAI Proof-of-Idea PayRail"
APP_ENV=production
PUBLIC_BASE_URL="https://your-domain.example"
DATABASE_URL="postgresql://user:pass@host:5432/membra"
SECRET_KEY="replace-with-32+-char-random-secret"

# ===== OPENAI / LLM STRUCTURING =====
OPENAI_API_KEY="sk-..."
OPENAI_MODEL="gpt-5.5-thinking"

# ===== GITHUB PROVENANCE =====
GITHUB_TOKEN="ghp_or_fine_grained_token_with_repo_contents_write"
GITHUB_OWNER="overandor"
GITHUB_REPO="gas-memory-collateral"
GITHUB_BRANCH="main"

# ===== IPFS / PINATA =====
PINATA_JWT="eyJ..."
PINATA_GATEWAY="https://gateway.pinata.cloud/ipfs/"

# ===== STRIPE IDENTITY + PAYMENTS =====
STRIPE_SECRET_KEY="sk_live_..."
STRIPE_WEBHOOK_SECRET="whsec_..."
STRIPE_SUCCESS_URL="https://your-domain.example/payment/success"
STRIPE_CANCEL_URL="https://your-domain.example/payment/cancel"

# ===== PUBLIC WALLET / ONCHAIN ANCHOR =====
PUBLIC_RECEIVE_WALLET="0xYourPublicWalletOnly"
EVM_RPC_URL="https://polygon-amoy.drpc.org"
EVM_CHAIN_ID="80002"
EVM_PRIVATE_KEY=""        # optional, only if this server anchors txs; never commit
EVM_ANCHOR_TO=""          # optional; defaults to public wallet if empty

# ===== OPTIONAL DEPLOYED PROOF TOKEN CONTRACT =====
PROOF_TOKEN_CONTRACT_ADDRESS=""
EOF

cat > "${APP_DIR}/backend/requirements.txt" <<'EOF'
fastapi==0.115.6
uvicorn[standard]==0.34.0
gunicorn==23.0.0
python-multipart==0.0.20
requests==2.32.3
pydantic==2.10.4
pydantic-settings==2.7.1
psycopg2-binary==2.9.10
python-dotenv==1.0.1
web3==7.6.1
eth-account==0.13.4
jinja2==3.1.5
itsdangerous==2.2.0
EOF

cat > "${APP_DIR}/backend/Dockerfile" <<'EOF'
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8080

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "app:app", "-b", "0.0.0.0:8080", "--workers", "2", "--timeout", "120"]
EOF

cat > "${APP_DIR}/docker-compose.yml" <<'EOF'
services:
  membra-api:
    build: ./backend
    env_file:
      - .env
    ports:
      - "8080:8080"
    restart: unless-stopped
EOF

cat > "${APP_DIR}/backend/app.py" <<'PYEOF'
import os, json, base64, hashlib, uuid, datetime, mimetypes
from typing import Optional, Any, Dict
import requests
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from web3 import Web3
from eth_account import Account

UTC = datetime.timezone.utc

class Settings(BaseSettings):
    APP_NAME: str = "MEMBRA ClosedAI Proof-of-Idea PayRail"
    APP_ENV: str = "production"
    PUBLIC_BASE_URL: str
    DATABASE_URL: str
    SECRET_KEY: str
    OPENAI_API_KEY: str
    OPENAI_MODEL: str = "gpt-5.5-thinking"
    GITHUB_TOKEN: str
    GITHUB_OWNER: str
    GITHUB_REPO: str
    GITHUB_BRANCH: str = "main"
    PINATA_JWT: str
    PINATA_GATEWAY: str = "https://gateway.pinata.cloud/ipfs/"
    STRIPE_SECRET_KEY: str
    STRIPE_WEBHOOK_SECRET: Optional[str] = None
    STRIPE_SUCCESS_URL: str
    STRIPE_CANCEL_URL: str
    PUBLIC_RECEIVE_WALLET: str
    EVM_RPC_URL: Optional[str] = None
    EVM_CHAIN_ID: Optional[int] = None
    EVM_PRIVATE_KEY: Optional[str] = None
    EVM_ANCHOR_TO: Optional[str] = None
    PROOF_TOKEN_CONTRACT_ADDRESS: Optional[str] = None
    class Config:
        env_file = ".env"

settings = Settings()
app = FastAPI(title=settings.APP_NAME, version="1.0.0-production")

def now_iso():
    return datetime.datetime.now(UTC).isoformat()

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def require(name: str, value: Any):
    if value is None or value == "":
        raise HTTPException(status_code=500, detail=f"Missing required production configuration: {name}")

def db():
    require("DATABASE_URL", settings.DATABASE_URL)
    conn = psycopg2.connect(settings.DATABASE_URL)
    conn.autocommit = True
    return conn

def init_db():
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS artifacts (
            id TEXT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            creator TEXT NOT NULL,
            artifact_type TEXT NOT NULL,
            title TEXT NOT NULL,
            raw_hash TEXT NOT NULL,
            summary JSONB NOT NULL,
            github_path TEXT,
            github_commit TEXT,
            ipfs_cid TEXT,
            evm_tx_hash TEXT,
            appraisal_usd NUMERIC,
            payment_status TEXT NOT NULL DEFAULT 'unfunded',
            public_wallet TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ledger (
            id TEXT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            artifact_id TEXT,
            rail TEXT NOT NULL,
            amount NUMERIC,
            currency TEXT,
            tx_hash TEXT,
            status TEXT NOT NULL,
            note TEXT,
            raw JSONB
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS visuals (
            id TEXT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            image_hash TEXT NOT NULL,
            filename TEXT,
            mime TEXT,
            description JSONB NOT NULL,
            github_path TEXT,
            github_commit TEXT,
            ipfs_cid TEXT
        );
        """)

@app.on_event("startup")
def startup():
    init_db()

class ChatArtifactIn(BaseModel):
    creator: str
    text: str = Field(..., min_length=1)
    title: Optional[str] = None
    license_scope: str = "private_until_user_approves"
    claim_type: str = "proof_of_idea"
    public_wallet: Optional[str] = None

class CheckoutIn(BaseModel):
    artifact_id: str
    amount_usd: float = Field(..., gt=0)
    payer_note: str = ""
    purpose: str = "support_or_license"

class AnchorIn(BaseModel):
    artifact_id: str

def openai_structured_text(text: str, claim_type: str, license_scope: str) -> Dict[str, Any]:
    require("OPENAI_API_KEY", settings.OPENAI_API_KEY)
    schema_hint = {
        "title": "short artifact title",
        "summary": "plain-English summary",
        "claim_type": claim_type,
        "commercial_category": "idea|prompt|memory|fieldwork|code|protocol|build_epoch|other",
        "originality_score_0_100": 0,
        "usefulness_score_0_100": 0,
        "implementation_score_0_100": 0,
        "provenance_strength_0_100": 0,
        "risk_flags": [],
        "license_scope": license_scope,
        "appraisal": {"floor_usd": 0, "base_usd": 0, "upside_usd": 0, "method": "brief method"},
        "public_safe_description": "safe text for public ledger"
    }
    payload = {
        "model": settings.OPENAI_MODEL,
        "input": [
            {"role": "system", "content": "You are MEMBRA's production evidence processor. Return strict JSON only. Do not claim official certification, legal ownership, guaranteed payment, or securities status."},
            {"role": "user", "content": "Schema target:\n" + json.dumps(schema_hint) + "\n\nText:\n" + text}
        ],
        "text": {"format": {"type": "json_object"}}
    }
    r = requests.post("https://api.openai.com/v1/responses", headers={"Authorization": f"Bearer {settings.OPENAI_API_KEY}", "Content-Type": "application/json"}, json=payload, timeout=120)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail={"openai_error": r.text})
    data = r.json()
    out = data.get("output_text") or "".join(c.get("text", "") for item in data.get("output", []) for c in item.get("content", []) if c.get("type") in ("output_text", "text"))
    try:
        return json.loads(out)
    except Exception:
        raise HTTPException(status_code=502, detail={"error": "OpenAI did not return parseable JSON", "raw": out})

def openai_reproducidescribe_image(image_bytes: bytes, mime: str, filename: str) -> Dict[str, Any]:
    require("OPENAI_API_KEY", settings.OPENAI_API_KEY)
    b64 = base64.b64encode(image_bytes).decode()
    prompt = """
Create a reproducible design/provenance description for this image. Return strict JSON with:
{
  "image_role": "screenshot|diagram|logo|fieldwork|ui|other",
  "one_sentence_summary": "",
  "visible_text": [],
  "layout_description": "",
  "style_tokens": {"palette": [], "typography": "", "materials": "", "icon_style": "", "spacing_density": "", "mood": ""},
  "membra_ui_delta": {"what_to_adopt": [], "what_to_avoid": [], "component_updates": [], "copy_updates": []},
  "reproduction_prompt": "",
  "privacy_flags": [],
  "artifact_value_notes": []
}
"""
    payload = {"model": settings.OPENAI_MODEL, "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}, {"type": "input_image", "image_url": f"data:{mime};base64,{b64}"}]}], "text": {"format": {"type": "json_object"}}}
    r = requests.post("https://api.openai.com/v1/responses", headers={"Authorization": f"Bearer {settings.OPENAI_API_KEY}", "Content-Type": "application/json"}, json=payload, timeout=180)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail={"openai_error": r.text})
    data = r.json()
    out = data.get("output_text") or "".join(c.get("text", "") for item in data.get("output", []) for c in item.get("content", []) if c.get("type") in ("output_text", "text"))
    try:
        return json.loads(out)
    except Exception:
        raise HTTPException(status_code=502, detail={"error": "OpenAI image description was not parseable JSON", "raw": out})

def github_put(path: str, content: bytes, message: str) -> str:
    require("GITHUB_TOKEN", settings.GITHUB_TOKEN)
    url = f"https://api.github.com/repos/{settings.GITHUB_OWNER}/{settings.GITHUB_REPO}/contents/{path}"
    headers = {"Authorization": f"Bearer {settings.GITHUB_TOKEN}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    get = requests.get(url, headers=headers, params={"ref": settings.GITHUB_BRANCH}, timeout=30)
    body = {"message": message, "content": base64.b64encode(content).decode(), "branch": settings.GITHUB_BRANCH}
    if get.status_code == 200:
        body["sha"] = get.json()["sha"]
    elif get.status_code != 404:
        raise HTTPException(status_code=502, detail={"github_get_error": get.text})
    put = requests.put(url, headers=headers, json=body, timeout=60)
    if put.status_code >= 400:
        raise HTTPException(status_code=502, detail={"github_put_error": put.text})
    return put.json()["commit"]["sha"]

def pin_json(obj: Dict[str, Any], name: str) -> str:
    require("PINATA_JWT", settings.PINATA_JWT)
    r = requests.post("https://api.pinata.cloud/pinning/pinJSONToIPFS", headers={"Authorization": f"Bearer {settings.PINATA_JWT}", "Content-Type": "application/json"}, json={"pinataMetadata": {"name": name}, "pinataContent": obj}, timeout=60)
    if r.status_code >= 400:
        raise HTTPException(status_code=502, detail={"pinata_error": r.text})
    return r.json()["IpfsHash"]

def evm_anchor_hash(hex_hash: str) -> str:
    require("EVM_RPC_URL", settings.EVM_RPC_URL)
    require("EVM_PRIVATE_KEY", settings.EVM_PRIVATE_KEY)
    w3 = Web3(Web3.HTTPProvider(settings.EVM_RPC_URL))
    acct = Account.from_key(settings.EVM_PRIVATE_KEY)
    to_addr = settings.EVM_ANCHOR_TO or settings.PUBLIC_RECEIVE_WALLET
    tx = {"to": Web3.to_checksum_address(to_addr), "value": 0, "data": "0x" + hex_hash, "nonce": w3.eth.get_transaction_count(acct.address), "chainId": int(settings.EVM_CHAIN_ID), "gas": 80000, "maxFeePerGas": w3.eth.gas_price * 2, "maxPriorityFeePerGas": w3.eth.gas_price}
    signed = acct.sign_transaction(tx)
    return w3.eth.send_raw_transaction(signed.raw_transaction).hex()

@app.get("/", response_class=HTMLResponse)
def home():
    return HTMLResponse("""
<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MEMBRA ClosedAI</title>
<style>:root{--bg:#050505;--panel:#111316;--gold:#d5a354;--orange:#ff9d2d;--teal:#54d6c9;--text:#f7f1e8;--muted:#9b9388}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 50% 0,#1c1305,#050505 45%);color:var(--text);font-family:Inter,system-ui,Arial,sans-serif}header{padding:28px 34px;border-bottom:1px solid #2a2118;display:flex;justify-content:space-between;align-items:center}h1{margin:0;font-size:28px;letter-spacing:.08em}.accent{color:var(--gold)}.grid{display:grid;grid-template-columns:320px 1fr 360px;gap:18px;padding:20px}.card{background:linear-gradient(180deg,rgba(255,255,255,.06),rgba(255,255,255,.02));border:1px solid rgba(213,163,84,.35);border-radius:18px;padding:18px;box-shadow:0 0 30px rgba(255,157,45,.08)}.card h2{font-size:16px;text-transform:uppercase;letter-spacing:.08em;color:var(--gold);margin:0 0 14px}.item{border:1px solid rgba(255,255,255,.10);border-radius:12px;padding:12px;margin:10px 0;background:rgba(0,0,0,.25)}.core{display:grid;place-items:center;text-align:center;min-height:420px;border-radius:30px;border:1px solid rgba(255,157,45,.4);background:radial-gradient(circle,rgba(255,157,45,.22),rgba(0,0,0,.2) 50%,rgba(0,0,0,.4))}.core .orb{width:260px;height:260px;border-radius:50%;display:grid;place-items:center;border:2px solid var(--orange);box-shadow:0 0 60px rgba(255,157,45,.35), inset 0 0 40px rgba(255,157,45,.18)}.orb b{font-size:38px;letter-spacing:.12em}textarea,input,select{width:100%;background:#0a0b0d;color:var(--text);border:1px solid #3c3024;border-radius:12px;padding:12px;margin:8px 0}button{background:linear-gradient(135deg,var(--gold),var(--orange));color:#100b05;border:0;border-radius:12px;padding:12px 14px;font-weight:800;cursor:pointer}pre{white-space:pre-wrap;word-break:break-word;background:#070707;border:1px solid #2c241c;border-radius:12px;padding:12px;max-height:360px;overflow:auto}.flow{display:flex;gap:10px;justify-content:center;padding:16px 22px;color:var(--muted);font-weight:700}.flow span{color:var(--gold)}@media(max-width:1050px){.grid{grid-template-columns:1fr}.core{min-height:300px}}</style></head>
<body><header><h1><span class="accent">MEMBRA</span> ClosedAI PayRail</h1><div>Verified Human Ideas → Funded Payouts</div></header><div class="grid"><section class="card"><h2>Human Contributors</h2><div class="item">Ideas</div><div class="item">Prompts</div><div class="item">Memories</div><div class="item">Fieldwork</div><div class="item">Code</div><div class="item">Build Logs</div><h2 style="margin-top:24px">Core Doctrine</h2><div class="item">Humans contribute meaning</div><div class="item">LLMs structure value</div><div class="item">Notaries verify identity</div><div class="item">Hashes prove time</div><div class="item">Markets fund usefulness</div></section><main><div class="core"><div class="orb"><div><b>MEMBRA</b><br>Verified Human Ideas<br>Provenance • Monetization</div></div></div><div class="flow"><span>Human Idea</span> → <span>Verified Artifact</span> → <span>Provenance Proof</span> → <span>Appraised Claim</span> → <span>Funded Payout</span></div><section class="card"><h2>Create Idea Artifact</h2><input id="creator" placeholder="creator handle or public wallet" value="overandor"><textarea id="text" rows="7" placeholder="Paste idea, prompt, answer, memory, field note, protocol, or build log"></textarea><select id="claim"><option>proof_of_idea</option><option>proof_of_prompt</option><option>proof_of_memory</option><option>proof_of_build</option><option>proof_of_fieldwork</option></select><button onclick="createArtifact()">Structure + Hash + GitHub/IPFS Anchor</button><pre id="out"></pre></section><section class="card"><h2>Reproducidescribe Image / UI Input</h2><input type="file" id="img"><button onclick="describeImage()">Describe + Extract UI Delta + Anchor</button><pre id="imgout"></pre></section></main><aside class="card"><h2>Trust + Payment Rails</h2><div class="item">Notary / KYC bridge: <button onclick="kyc()">Create Stripe Identity Session</button></div><div class="item">GitHub Ledger: enabled via repo contents API</div><div class="item">IPFS Archive: Pinata JSON pinning</div><div class="item">Blockchain Timestamp: optional EVM hash tx</div><div class="item">Payment Rail: Stripe Checkout</div><h2>Artifact Actions</h2><input id="artifact_id" placeholder="artifact_id"><input id="amount" type="number" placeholder="USD amount"><button onclick="checkout()">Create Checkout</button><button onclick="anchor()">Onchain Hash Anchor</button><pre id="sideout"></pre></aside></div>
<script>async function api(path,opts={}){const r=await fetch(path,Object.assign({headers:{'Content-Type':'application/json'}},opts));const j=await r.json();if(!r.ok)throw j;return j}async function createArtifact(){try{const j=await api('/api/artifacts/from-chat',{method:'POST',body:JSON.stringify({creator:creator.value,text:text.value,claim_type:claim.value,public_wallet:creator.value.startsWith('0x')?creator.value:null})});out.textContent=JSON.stringify(j,null,2);artifact_id.value=j.id}catch(e){out.textContent=JSON.stringify(e,null,2)}}async function describeImage(){const f=img.files[0];if(!f)return;const fd=new FormData();fd.append('file',f);const r=await fetch('/api/visuals/reproducidescribe',{method:'POST',body:fd});const j=await r.json();imgout.textContent=JSON.stringify(j,null,2)}async function kyc(){const r=await api('/api/kyc/session',{method:'POST',body:JSON.stringify({})});sideout.textContent=JSON.stringify(r,null,2);if(r.url)location.href=r.url}async function checkout(){const j=await api('/api/payments/checkout',{method:'POST',body:JSON.stringify({artifact_id:artifact_id.value,amount_usd:parseFloat(amount.value||'0'),purpose:'support_or_license'})});sideout.textContent=JSON.stringify(j,null,2);if(j.url)location.href=j.url}async function anchor(){const j=await api('/api/onchain/anchor',{method:'POST',body:JSON.stringify({artifact_id:artifact_id.value})});sideout.textContent=JSON.stringify(j,null,2)}</script></body></html>""")

@app.post("/api/artifacts/from-chat")
def create_artifact(inp: ChatArtifactIn):
    raw_hash = sha256_bytes(inp.text.encode("utf-8"))
    structured = openai_structured_text(inp.text, inp.claim_type, inp.license_scope)
    artifact_id = "MEMBRA-" + uuid.uuid4().hex[:12].upper()
    title = inp.title or structured.get("title") or f"{inp.claim_type} {artifact_id}"
    base = (structured.get("appraisal") or {}).get("base_usd") or 0
    packet = {"id": artifact_id, "created_at": now_iso(), "creator": inp.creator, "artifact_type": inp.claim_type, "title": title, "raw_text_sha256": raw_hash, "summary": structured, "public_wallet": inp.public_wallet or settings.PUBLIC_RECEIVE_WALLET, "payment_status": "unfunded", "doctrine": "Idea becomes payable only when accepted by a funded market, buyer, sponsor, grant, bounty, license, or platform pool."}
    github_path = f"artifacts/{artifact_id}.json"
    commit = github_put(github_path, json.dumps(packet, indent=2).encode(), f"Add artifact {artifact_id}")
    packet["github_path"] = github_path; packet["github_commit"] = commit
    cid = pin_json(packet, f"{artifact_id}.json"); packet["ipfs_cid"] = cid
    with db() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO artifacts (id, created_at, creator, artifact_type, title, raw_hash, summary, github_path, github_commit, ipfs_cid, appraisal_usd, public_wallet) VALUES (%s, now(), %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s)""", (artifact_id, inp.creator, inp.claim_type, title, raw_hash, json.dumps(structured), github_path, commit, cid, base, inp.public_wallet or settings.PUBLIC_RECEIVE_WALLET))
    return {**packet, "github_url": f"https://github.com/{settings.GITHUB_OWNER}/{settings.GITHUB_REPO}/blob/{settings.GITHUB_BRANCH}/{github_path}", "ipfs_url": settings.PINATA_GATEWAY + cid}

@app.post("/api/visuals/reproducidescribe")
async def reproducidescribe(file: UploadFile = File(...)):
    b = await file.read()
    if not b: raise HTTPException(status_code=400, detail="empty file")
    mime = file.content_type or mimetypes.guess_type(file.filename or "")[0] or "application/octet-stream"
    img_hash = sha256_bytes(b)
    desc = openai_reproducidescribe_image(b, mime, file.filename or "upload")
    record_id = "VIS-" + img_hash[:16].upper()
    packet = {"id": record_id, "created_at": now_iso(), "filename": file.filename, "mime": mime, "image_sha256": img_hash, "description": desc, "ui_rule": "Every image gets reproducidescribed into style tokens, provenance, and MEMBRA UI deltas before being used as product design input."}
    github_path = f"visuals/{record_id}.json"
    commit = github_put(github_path, json.dumps(packet, indent=2).encode(), f"Add visual reproducidescription {record_id}")
    cid = pin_json(packet, f"{record_id}.json")
    with db() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO visuals (id, created_at, image_hash, filename, mime, description, github_path, github_commit, ipfs_cid) VALUES (%s, now(), %s, %s, %s, %s::jsonb, %s, %s, %s)""", (record_id, img_hash, file.filename, mime, json.dumps(desc), github_path, commit, cid))
    return {**packet, "github_path": github_path, "github_commit": commit, "ipfs_cid": cid, "github_url": f"https://github.com/{settings.GITHUB_OWNER}/{settings.GITHUB_REPO}/blob/{settings.GITHUB_BRANCH}/{github_path}", "ipfs_url": settings.PINATA_GATEWAY + cid}

@app.post("/api/kyc/session")
def stripe_kyc_session():
    require("STRIPE_SECRET_KEY", settings.STRIPE_SECRET_KEY)
    r = requests.post("https://api.stripe.com/v1/identity/verification_sessions", auth=(settings.STRIPE_SECRET_KEY, ""), data={"type": "document", "metadata[system]": "membra-closedai"}, timeout=30)
    if r.status_code >= 400: raise HTTPException(status_code=502, detail={"stripe_error": r.text})
    return r.json()

@app.post("/api/payments/checkout")
def stripe_checkout(inp: CheckoutIn):
    require("STRIPE_SECRET_KEY", settings.STRIPE_SECRET_KEY)
    amount_cents = int(round(inp.amount_usd * 100))
    if amount_cents <= 0: raise HTTPException(status_code=400, detail="amount_usd must be > 0")
    data = {"mode": "payment", "success_url": settings.STRIPE_SUCCESS_URL + "?artifact_id=" + inp.artifact_id, "cancel_url": settings.STRIPE_CANCEL_URL + "?artifact_id=" + inp.artifact_id, "line_items[0][price_data][currency]": "usd", "line_items[0][price_data][product_data][name]": f"MEMBRA idea artifact support/license: {inp.artifact_id}", "line_items[0][price_data][product_data][description]": "Payment supports or licenses a provenance-backed idea artifact. Not an investment or guaranteed return.", "line_items[0][price_data][unit_amount]": str(amount_cents), "line_items[0][quantity]": "1", "metadata[artifact_id]": inp.artifact_id, "metadata[purpose]": inp.purpose, "metadata[note]": inp.payer_note[:400]}
    r = requests.post("https://api.stripe.com/v1/checkout/sessions", auth=(settings.STRIPE_SECRET_KEY, ""), data=data, timeout=30)
    if r.status_code >= 400: raise HTTPException(status_code=502, detail={"stripe_error": r.text})
    sess = r.json(); lid = "LEDGER-" + uuid.uuid4().hex[:12].upper()
    with db() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO ledger (id, created_at, artifact_id, rail, amount, currency, tx_hash, status, note, raw) VALUES (%s, now(), %s, 'stripe_checkout', %s, 'USD', %s, %s, %s, %s::jsonb)""", (lid, inp.artifact_id, inp.amount_usd, sess.get("id"), "checkout_created", inp.payer_note, json.dumps(sess)))
    return {"ledger_id": lid, "stripe_session_id": sess.get("id"), "url": sess.get("url"), "status": sess.get("status")}

@app.post("/api/onchain/anchor")
def onchain_anchor(inp: AnchorIn):
    with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM artifacts WHERE id=%s", (inp.artifact_id,)); row = cur.fetchone()
    if not row: raise HTTPException(status_code=404, detail="artifact not found")
    txh = evm_anchor_hash(row["raw_hash"])
    with db() as conn, conn.cursor() as cur:
        cur.execute("UPDATE artifacts SET evm_tx_hash=%s WHERE id=%s", (txh, inp.artifact_id)); lid = "LEDGER-" + uuid.uuid4().hex[:12].upper()
        cur.execute("""INSERT INTO ledger (id, created_at, artifact_id, rail, tx_hash, status, note, raw) VALUES (%s, now(), %s, 'evm_anchor', %s, 'submitted', 'Onchain hash anchor transaction', %s::jsonb)""", (lid, inp.artifact_id, txh, json.dumps({"chain_id": settings.EVM_CHAIN_ID, "rpc": settings.EVM_RPC_URL})))
    return {"artifact_id": inp.artifact_id, "tx_hash": txh, "ledger_id": lid}

@app.get("/api/ledger/public")
def public_ledger():
    with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM ledger ORDER BY created_at DESC LIMIT 100"); rows = cur.fetchall()
    return {"rows": [dict(r) for r in rows]}

@app.get("/api/artifacts/{artifact_id}")
def get_artifact(artifact_id: str):
    with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM artifacts WHERE id=%s", (artifact_id,)); row = cur.fetchone()
    if not row: raise HTTPException(status_code=404, detail="artifact not found")
    return dict(row)

@app.get("/healthz")
def healthz():
    return {"ok": True, "app": settings.APP_NAME, "time": now_iso(), "mode": "production-no-mock"}
PYEOF

cat > "${APP_DIR}/contracts/ProofOfIdeaSBT.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "@openzeppelin/contracts/token/ERC721/ERC721.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

interface IERC5192 {
    event Locked(uint256 tokenId);
    event Unlocked(uint256 tokenId);
    function locked(uint256 tokenId) external view returns (bool);
}

contract ProofOfIdeaSBT is ERC721, Ownable, IERC5192 {
    uint256 public nextTokenId = 1;
    struct ProofRecord { bytes32 artifactHash; string metadataURI; string githubCommit; uint256 issuedAt; }
    mapping(uint256 => ProofRecord) public proofRecords;
    constructor(address initialOwner) ERC721("STRNEDUCIFIED Proof of Idea", "STRN-POL") Ownable(initialOwner) {}
    function mint(address to, bytes32 artifactHash, string calldata metadataURI, string calldata githubCommit) external onlyOwner returns (uint256) {
        uint256 tokenId = nextTokenId++;
        _safeMint(to, tokenId);
        proofRecords[tokenId] = ProofRecord({artifactHash: artifactHash, metadataURI: metadataURI, githubCommit: githubCommit, issuedAt: block.timestamp});
        emit Locked(tokenId);
        return tokenId;
    }
    function locked(uint256 tokenId) external view returns (bool) { require(_ownerOf(tokenId) != address(0), "nonexistent token"); return true; }
    function tokenURI(uint256 tokenId) public view override returns (string memory) { require(_ownerOf(tokenId) != address(0), "nonexistent token"); return proofRecords[tokenId].metadataURI; }
    function _update(address to, uint256 tokenId, address auth) internal override returns (address) {
        address from = _ownerOf(tokenId);
        if (from != address(0) && to != address(0)) revert("STRN-POL: non-transferable proof credential");
        return super._update(to, tokenId, auth);
    }
}
EOF

cat > "${APP_DIR}/package.json" <<'EOF'
{"name":"membra-closedai-contracts","private":true,"version":"1.0.0","scripts":{"compile":"hardhat compile","deploy:amoy":"hardhat run scripts/deploy.js --network amoy"},"dependencies":{"@nomicfoundation/hardhat-toolbox":"^5.0.0","@openzeppelin/contracts":"^5.1.0","dotenv":"^16.4.7","hardhat":"^2.22.17"}}
EOF

cat > "${APP_DIR}/hardhat.config.js" <<'EOF'
require("@nomicfoundation/hardhat-toolbox");
require("dotenv").config();
const privateKey = process.env.EVM_PRIVATE_KEY || "";
module.exports = { solidity: "0.8.24", networks: { amoy: { url: process.env.EVM_RPC_URL || "https://polygon-amoy.drpc.org", accounts: privateKey ? [privateKey] : [], chainId: Number(process.env.EVM_CHAIN_ID || 80002) } } };
EOF

cat > "${APP_DIR}/scripts/deploy.js" <<'EOF'
const hre = require("hardhat");
async function main() {
  const [deployer] = await hre.ethers.getSigners();
  if (!deployer) throw new Error("No deployer. Set EVM_PRIVATE_KEY in .env locally.");
  console.log("Deploying with:", deployer.address);
  const Proof = await hre.ethers.getContractFactory("ProofOfIdeaSBT");
  const proof = await Proof.deploy(deployer.address);
  await proof.waitForDeployment();
  console.log("ProofOfIdeaSBT deployed:", await proof.getAddress());
}
main().catch((e) => { console.error(e); process.exit(1); });
EOF

cat > "${APP_DIR}/README.md" <<'EOF'
# MEMBRA ClosedAI Production Scaffold

This repository was generated from one `.sh` file.

## What it implements

- Chat / prompt / answer / memory / fieldwork / code -> structured Idea Artifact
- SHA-256 hashing
- ChatGPT/OpenAI structured appraisal
- GitHub provenance commits
- Pinata/IPFS metadata pinning
- Stripe Identity KYC session creation
- Stripe Checkout support/license payment creation
- Optional EVM onchain hash anchoring by signed transaction
- Public ledger table
- Image reproducidescribe endpoint that converts every image into visible text, style tokens, reproducible description, MEMBRA UI delta, and provenance record

## No mock policy

The app does not fake KYC, payment, GitHub, IPFS, OpenAI, or blockchain success. Missing credentials produce hard errors.

## Start

```bash
cp .env.example .env
# Fill real production credentials.
docker compose up --build
```

Open:

```text
http://localhost:8080
```

## Optional contract deploy

```bash
npm install
source .env
npm run compile
npm run deploy:amoy
```

Never commit `.env`, private keys, seed phrases, identity documents, or raw KYC data.
EOF

cat > "${APP_DIR}/deploy/systemd.service" <<'EOF'
[Unit]
Description=MEMBRA ClosedAI Production API
After=network.target

[Service]
WorkingDirectory=/opt/membra-closedai-production
ExecStart=/usr/bin/docker compose up
ExecStop=/usr/bin/docker compose down
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

cat > "${APP_DIR}/accounting/public-ledger.schema.json" <<'EOF'
{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"MEMBRA Public Payment Ledger Row","type":"object","required":["id","created_at","artifact_id","rail","status"],"properties":{"id":{"type":"string"},"created_at":{"type":"string"},"artifact_id":{"type":"string"},"rail":{"type":"string"},"amount":{"type":["number","null"]},"currency":{"type":["string","null"]},"tx_hash":{"type":["string","null"]},"status":{"type":"string"},"note":{"type":["string","null"]}}}
EOF

cat > "${APP_DIR}/provenance/DOCTRINE.md" <<'EOF'
# MEMBRA ClosedAI Doctrine

Human idea -> verified artifact -> provenance proof -> appraised claim -> funded payout.

The system does not pay for every thought.
The system pays only when an approved, structured, verified artifact is accepted by a funded buyer, sponsor, donor, bounty, grant, license, or platform pool.

Raw private chats do not go onchain.
Raw KYC documents do not go into GitHub.
Private keys and seed phrases are never collected.

Every picture must be reproducidescribed:
image -> hash -> visible text -> style tokens -> reproducible description -> UI delta -> provenance anchor.
EOF

echo ""
echo "Created ${APP_DIR}"
echo ""
echo "Next steps:"
echo "  cd ${APP_DIR}"
echo "  cp .env.example .env"
echo "  # Fill real credentials. No mocks. No private keys in GitHub."
echo "  docker compose up --build"
echo ""
echo "Optional contract:"
echo "  npm install"
echo "  npm run compile"
echo "  source .env && npm run deploy:amoy"
echo ""
echo "Done."
