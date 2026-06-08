"""Wiz Poster — publique e agende seus vídeos no TikTok.

Produto web da Wiz Mídia: criadores conectam a própria conta TikTok (OAuth oficial)
e enviam/agendam vídeos via Content Posting API.

Rotas:
  GET  /            landing page
  GET  /login       inicia OAuth (Login Kit)
  GET  /callback    troca code por token, salva usuário
  GET  /dashboard   conta conectada + form de envio + fila
  POST /post        envia vídeo (rascunho ou publicação direta, agora ou agendado)
  POST /logout      desconecta
Env: TIKTOK_CLIENT_KEY, TIKTOK_CLIENT_SECRET, BASE_URL, SECRET_KEY
"""
from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests
from fastapi import BackgroundTasks, Cookie, FastAPI, File, Form, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from itsdangerous import BadSignature, URLSafeSerializer

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")
CLIENT_KEY = os.environ.get("TIKTOK_CLIENT_KEY", "")
CLIENT_SECRET = os.environ.get("TIKTOK_CLIENT_SECRET", "")
SECRET_KEY = os.environ.get("SECRET_KEY", secrets.token_hex(16))
SCOPES = "user.info.basic,video.upload,video.publish"
OPEN_API = "https://open.tiktokapis.com/v2"
DB_PATH = os.environ.get("DB_PATH", str(Path(__file__).parent / "wizposter.db"))
UPLOADS = Path(os.environ.get("UPLOAD_DIR", Path(__file__).parent / "uploads"))
UPLOADS.mkdir(exist_ok=True)

app = FastAPI(title="Wiz Poster")
signer = URLSafeSerializer(SECRET_KEY, salt="wizposter")


# ---------------------------------------------------------------- DB
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS users(
            open_id TEXT PRIMARY KEY, display_name TEXT, avatar_url TEXT,
            access_token TEXT, refresh_token TEXT, expires_at REAL)""")
        c.execute("""CREATE TABLE IF NOT EXISTS posts(
            id INTEGER PRIMARY KEY AUTOINCREMENT, open_id TEXT, title TEXT,
            mode TEXT, file TEXT, scheduled_at TEXT, status TEXT DEFAULT 'agendado',
            publish_id TEXT, created_at TEXT)""")


init_db()


# ---------------------------------------------------------------- TikTok API
def refresh_token_if_needed(u: sqlite3.Row) -> dict:
    u = dict(u)
    if u["expires_at"] - 120 > time.time():
        return u
    r = requests.post(f"{OPEN_API}/oauth/token/", data={
        "client_key": CLIENT_KEY, "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token", "refresh_token": u["refresh_token"]},
        headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=60)
    d = r.json()
    if "access_token" in d:
        u["access_token"] = d["access_token"]
        u["refresh_token"] = d.get("refresh_token", u["refresh_token"])
        u["expires_at"] = time.time() + d.get("expires_in", 86400)
        with db() as c:
            c.execute("UPDATE users SET access_token=?, refresh_token=?, expires_at=? "
                      "WHERE open_id=?",
                      (u["access_token"], u["refresh_token"], u["expires_at"], u["open_id"]))
    return u


def tiktok_post_video(u: dict, path: str, title: str, mode: str) -> str:
    """mode: 'draft' (caixa de rascunhos) | 'direct' (publica no perfil).
    Vídeos >64MB são enviados em pedaços (regra da API)."""
    headers = {"Authorization": f"Bearer {u['access_token']}",
               "Content-Type": "application/json; charset=UTF-8"}
    size = Path(path).stat().st_size
    max_single = 64 * 1024 * 1024
    if size <= max_single:
        chunk_size, total_chunks = size, 1
    else:
        chunk_size = 10 * 1024 * 1024           # 10MB por pedaço
        total_chunks = size // chunk_size       # último pedaço absorve a sobra
    src = {"source": "FILE_UPLOAD", "video_size": size,
           "chunk_size": chunk_size, "total_chunk_count": total_chunks}
    if mode == "direct":
        endpoint, body = "post/publish/video/init/", {
            "post_info": {"title": title[:150], "privacy_level": "SELF_ONLY",
                          "disable_comment": False}, "source_info": src}
    else:
        endpoint, body = "post/publish/inbox/video/init/", {"source_info": src}
    r = requests.post(f"{OPEN_API}/{endpoint}", headers=headers, json=body, timeout=60)
    data = r.json().get("data", {})
    if r.status_code != 200 or not data.get("upload_url"):
        raise RuntimeError(f"init {r.status_code}: {r.text[:200]}")

    with open(path, "rb") as fh:
        for i in range(total_chunks):
            start = i * chunk_size
            end = size - 1 if i == total_chunks - 1 else start + chunk_size - 1
            fh.seek(start)
            blob = fh.read(end - start + 1)
            put = requests.put(data["upload_url"], data=blob, timeout=600, headers={
                "Content-Type": "video/mp4",
                "Content-Range": f"bytes {start}-{end}/{size}"})
            if put.status_code not in (200, 201, 206):
                raise RuntimeError(f"upload pedaço {i+1}/{total_chunks}: {put.status_code}")
    return data.get("publish_id", "")


# ---------------------------------------------------------------- agendador
def _process_due() -> int:
    """Envia todos os posts agendados cujo horário já chegou. Retorna quantos enviou.
    Chamado por: thread de fundo, /cron (pinger externo) e ao abrir o painel."""
    now = datetime.now().strftime("%Y-%m-%dT%H:%M")
    with db() as c:
        due = c.execute("SELECT * FROM posts WHERE status='agendado' AND "
                        "scheduled_at<=?", (now,)).fetchall()
    sent = 0
    for p in due:
        with db() as c:
            u = c.execute("SELECT * FROM users WHERE open_id=?", (p["open_id"],)).fetchone()
        status, pid = "erro", ""
        if u:
            try:
                pid = tiktok_post_video(refresh_token_if_needed(u),
                                        p["file"], p["title"], p["mode"])
                status = "enviado"
                sent += 1
            except Exception as e:  # noqa: BLE001
                status, pid = "erro", str(e)[:180]
                print(f"[wizposter] erro no agendado {p['id']}: {e}", flush=True)
        with db() as c:
            c.execute("UPDATE posts SET status=?, publish_id=? WHERE id=?",
                      (status, pid, p["id"]))
    return sent


def _scheduler_loop():
    while True:
        try:
            _process_due()
        except Exception:  # noqa: BLE001
            pass
        time.sleep(30)


threading.Thread(target=_scheduler_loop, daemon=True).start()


# ---------------------------------------------------------------- helpers
def current_user(session: str | None):
    if not session:
        return None
    try:
        open_id = signer.loads(session)
    except BadSignature:
        return None
    with db() as c:
        return c.execute("SELECT * FROM users WHERE open_id=?", (open_id,)).fetchone()


PAGE = """<!doctype html><html lang="pt-BR"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Wiz Poster — agende seus vídeos no TikTok</title><style>
*{{box-sizing:border-box;margin:0}}body{{font-family:-apple-system,Segoe UI,Roboto,sans-serif;
background:#0d0d12;color:#eee;min-height:100vh}}
.wrap{{max-width:860px;margin:0 auto;padding:32px 20px}}
.nav{{display:flex;justify-content:space-between;align-items:center;margin-bottom:40px}}
.logo{{font-size:22px;font-weight:800;letter-spacing:1px}}.logo span{{color:#fe2c55}}
.btn{{background:#fe2c55;color:#fff;border:0;border-radius:8px;padding:12px 22px;
font-size:15px;font-weight:700;cursor:pointer;text-decoration:none;display:inline-block}}
.btn.sec{{background:#222;border:1px solid #444}}
.card{{background:#16161d;border:1px solid #26262f;border-radius:14px;padding:24px;margin:14px 0}}
h1{{font-size:34px;line-height:1.2;margin:18px 0}}h1 b{{color:#fe2c55}}
p.sub{{color:#aaa;font-size:17px;margin-bottom:26px}}
input,select{{width:100%;background:#0d0d12;color:#eee;border:1px solid #333;
border-radius:8px;padding:11px;margin:6px 0 14px;font-size:15px}}
label{{font-size:13px;color:#999;text-transform:uppercase;letter-spacing:.5px}}
table{{width:100%;border-collapse:collapse;font-size:14px}}
td,th{{padding:9px 6px;border-bottom:1px solid #26262f;text-align:left}}
.tag{{padding:3px 10px;border-radius:20px;font-size:12px;font-weight:700}}
.tag.agendado{{background:#2a2a14;color:#e6c94d}}.tag.enviado{{background:#10301c;color:#3fd47e}}
.tag.erro{{background:#33141a;color:#ff6b81}}
.user{{display:flex;align-items:center;gap:12px}}.user img{{width:44px;height:44px;border-radius:50%}}
.feat{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px}}
</style></head><body><div class="wrap">
<div class="nav"><div class="logo">WIZ<span>POSTER</span></div>{nav}</div>{body}
<p style="color:#555;font-size:12px;margin-top:40px">© Wiz Mídia — Patrocínio-MG ·
<a href="https://www.wizmidia.com.br/termostk" style="color:#777">Termos</a> ·
<a href="https://www.wizmidia.com.br/policytk" style="color:#777">Privacidade</a></p>
</div></body></html>"""


def render(body: str, nav: str = "") -> HTMLResponse:
    return HTMLResponse(PAGE.format(body=body, nav=nav))


# ---------------------------------------------------------------- rotas
@app.get("/", response_class=HTMLResponse)
def index(session: str | None = Cookie(default=None)):
    if current_user(session):
        return RedirectResponse("/dashboard")
    body = """
    <h1>Publique e <b>agende</b> seus vídeos<br>no TikTok num só lugar.</h1>
    <p class="sub">O Wiz Poster conecta na sua conta do TikTok com o login oficial e
    envia seus vídeos na hora ou no horário que você escolher.</p>
    <a class="btn" href="/login">Conectar minha conta TikTok</a>
    <div class="feat" style="margin-top:36px">
      <div class="card"><b>🔐 Login oficial</b><p style="color:#999;margin-top:8px">
        Você autoriza pelo próprio TikTok. Sem senha, revogável a qualquer momento.</p></div>
      <div class="card"><b>📤 Envio direto</b><p style="color:#999;margin-top:8px">
        Publique no perfil ou mande pros rascunhos pra finalizar no app.</p></div>
      <div class="card"><b>⏰ Agendamento</b><p style="color:#999;margin-top:8px">
        Escolha dia e hora — o Wiz Poster publica sozinho.</p></div>
    </div>"""
    return render(body)


@app.get("/login")
def login():
    url = "https://www.tiktok.com/v2/auth/authorize/?" + urllib.parse.urlencode({
        "client_key": CLIENT_KEY, "scope": SCOPES, "response_type": "code",
        "redirect_uri": f"{BASE_URL}/callback", "state": secrets.token_hex(8)})
    return RedirectResponse(url)


@app.get("/callback")
def callback(code: str = "", error: str = ""):
    if error or not code:
        return render(f"<div class='card'>Autorização cancelada ({error}). "
                      f"<a class='btn sec' href='/'>Voltar</a></div>")
    r = requests.post(f"{OPEN_API}/oauth/token/", data={
        "client_key": CLIENT_KEY, "client_secret": CLIENT_SECRET, "code": code,
        "grant_type": "authorization_code", "redirect_uri": f"{BASE_URL}/callback"},
        headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=60)
    d = r.json()
    if "access_token" not in d:
        return render(f"<div class='card'>Erro do TikTok: {json.dumps(d)[:300]}</div>")
    info = requests.get(f"{OPEN_API}/user/info/?fields=open_id,display_name,avatar_url",
                        headers={"Authorization": f"Bearer {d['access_token']}"},
                        timeout=30).json().get("data", {}).get("user", {})
    open_id = d.get("open_id") or info.get("open_id", "")
    with db() as c:
        c.execute("INSERT OR REPLACE INTO users VALUES(?,?,?,?,?,?)",
                  (open_id, info.get("display_name", "criador"),
                   info.get("avatar_url", ""), d["access_token"],
                   d["refresh_token"], time.time() + d.get("expires_in", 86400)))
    resp = RedirectResponse("/dashboard")
    resp.set_cookie("session", signer.dumps(open_id), httponly=True, max_age=86400 * 30)
    return resp


@app.get("/cron")
def cron():
    """Pinger externo chama isso a cada 1 min p/ disparar os agendados
    (mantém o app acordado no plano grátis do Render)."""
    try:
        n = _process_due()
        return {"ok": True, "enviados": n}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "erro": str(e)[:200]}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(session: str | None = Cookie(default=None)):
    u = current_user(session)
    if not u:
        return RedirectResponse("/")
    try:
        _process_due()   # dispara agendados vencidos ao abrir o painel
    except Exception:  # noqa: BLE001
        pass
    with db() as c:
        posts = c.execute("SELECT * FROM posts WHERE open_id=? ORDER BY id DESC LIMIT 20",
                          (u["open_id"],)).fetchall()
    rows = "".join(
        f"<tr><td>{p['title'][:38]}</td><td>{'Publicação' if p['mode']=='direct' else 'Rascunho'}</td>"
        f"<td>{p['scheduled_at'] or 'imediato'}</td>"
        f"<td><span class='tag {p['status']}' title=\"{(p['publish_id'] or '')[:160]}\">{p['status']}</span>"
        + (f"<div style='color:#ff6b81;font-size:11px;max-width:300px'>{p['publish_id'][:120]}</div>"
           if p['status'] == 'erro' and p['publish_id'] else "") + "</td></tr>"
        for p in posts) or "<tr><td colspan=4 style='color:#777'>Nenhum envio ainda.</td></tr>"
    avatar = u["avatar_url"] or "https://placehold.co/44"
    nav = (f"<div class='user'><img src='{avatar}'><b>@{u['display_name']}</b>"
           f"<form method='post' action='/logout' style='margin-left:8px'>"
           f"<button class='btn sec'>Sair</button></form></div>")
    body = f"""
    <div class="card"><h2 style="margin-bottom:14px">📤 Enviar vídeo</h2>
    <form method="post" action="/post" enctype="multipart/form-data">
      <label>Vídeo (mp4)</label><input type="file" name="video" accept="video/mp4" required>
      <label>Legenda / título</label><input name="title" maxlength="150"
        placeholder="Escreva a legenda do post" required>
      <label>Modo</label><select name="mode">
        <option value="direct">Publicar no perfil (agendável)</option>
        <option value="draft">Enviar pros rascunhos do TikTok</option></select>
      <label>Agendar para (opcional — vazio = enviar agora)</label>
      <input type="datetime-local" name="scheduled_at">
      <button class="btn">Enviar</button></form></div>
    <div class="card"><h2 style="margin-bottom:14px">🗓 Últimos envios</h2>
    <table><tr><th>Título</th><th>Modo</th><th>Quando</th><th>Status</th></tr>{rows}</table></div>"""
    return render(body, nav)


@app.post("/post")
async def post_video(background_tasks: BackgroundTasks,
                     session: str | None = Cookie(default=None),
                     video: UploadFile = File(...), title: str = Form(...),
                     mode: str = Form("draft"), scheduled_at: str = Form("")):
    u = current_user(session)
    if not u:
        return RedirectResponse("/", status_code=303)
    dest = UPLOADS / f"{int(time.time())}_{secrets.token_hex(4)}.mp4"
    dest.write_bytes(await video.read())
    with db() as c:
        cur = c.execute(
            "INSERT INTO posts(open_id,title,mode,file,scheduled_at,status,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (u["open_id"], title, mode, str(dest), scheduled_at or None,
             "agendado" if scheduled_at else "enviando",
             datetime.now().isoformat(timespec="seconds")))
        post_id = cur.lastrowid

    if not scheduled_at:  # envio imediato em background
        def _send():
            status, pid = "erro", ""
            try:
                pid = tiktok_post_video(refresh_token_if_needed(u), str(dest), title, mode)
                status = "enviado"
            except Exception as e:  # noqa: BLE001
                pid = str(e)[:180]
                print(f"[wizposter] erro no envio do post {post_id}: {e}", flush=True)
            with db() as c:
                c.execute("UPDATE posts SET status=?, publish_id=? WHERE id=?",
                          (status, pid, post_id))
        background_tasks.add_task(_send)
    return RedirectResponse("/dashboard", status_code=303)


@app.post("/logout")
def logout():
    resp = RedirectResponse("/", status_code=303)
    resp.delete_cookie("session")
    return resp
