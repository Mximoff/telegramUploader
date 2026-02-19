"""
Koyeb FastAPI Service
- دانلود مستقیم تا 2GB
- یوتیوب و پورن‌هاب با yt-dlp
- آپلود با Telethon (bypasses Bot API 50MB limit)
- کش file_id از طریق Bot API به Worker برمیگرده

متغیرهای محیطی مورد نیاز:
  BOT_TOKEN
  TELEGRAM_API_ID
  TELEGRAM_API_HASH
  KOYEB_API_KEY
  COOKIES_FILE  (اختیاری، پیشفرض: /app/cookies.txt)
"""

import os
import asyncio
import hashlib
import re
import tempfile
import time
import logging
from pathlib import Path
from typing import Optional

import httpx
import yt_dlp
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks, Request
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("koyeb-bot")

app = FastAPI(title="TG DL Bot — Koyeb")

# ─── Config (همه از env var) ──────────────────────────────────────────────────
BOT_TOKEN   = os.environ["BOT_TOKEN"]
API_ID      = int(os.environ["TELEGRAM_API_ID"])
API_HASH    = os.environ["TELEGRAM_API_HASH"]
API_KEY     = os.environ["KOYEB_API_KEY"]
COOKIES_FILE = os.environ.get("COOKIES_FILE", "/app/cookies.txt")
DEFAULT_QUALITY = "720"

# ─── In-memory task store ─────────────────────────────────────────────────────
# key: task_id, value: dict با status, url, startedAt, userId
active_tasks: dict[str, dict] = {}

# ─── Models ──────────────────────────────────────────────────────────────────
class ProcessRequest(BaseModel):
    url: str
    platform: Optional[str] = None   # "youtube" | "pornhub" | None
    newName: Optional[str] = None
    chatId: int
    userId: int
    messageId: int
    cacheKey: Optional[str] = None
    quality: Optional[str] = DEFAULT_QUALITY

# ─── Telethon (singleton) ─────────────────────────────────────────────────────
_telethon: Optional[TelegramClient] = None

async def get_telethon() -> TelegramClient:
    global _telethon
    if _telethon and _telethon.is_connected():
        return _telethon
    # اگه SESSION_STRING نداشتیم با bot token وصل میشیم
    session_str = os.environ.get("TELETHON_SESSION", "")
    session = StringSession(session_str) if session_str else StringSession()
    _telethon = TelegramClient(session, API_ID, API_HASH)
    await _telethon.start(bot_token=BOT_TOKEN)
    log.info("✅ Telethon connected")
    return _telethon

# ─── Telegram Bot API helpers ─────────────────────────────────────────────────
async def tg_edit(chat_id: int, message_id: int, text: str):
    """یه پیام ساده وضعیت — بدون progress bar، بدون spam"""
    async with httpx.AsyncClient() as c:
        await c.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
            json={"chat_id": chat_id, "message_id": message_id, "text": text},
            timeout=10,
        )

async def tg_cache_notify(chat_id: int, cache_key: str, file_id: str, file_type: str):
    """به Worker میگه file_id رو کش کنه — از طریق یه endpoint ساده"""
    # Worker یه /cache endpoint داره که این رو ذخیره می‌کنه
    # اگه Worker URL نداشتیم، خودمون مستقیم به KV نمیرسیم
    # پس این اطلاعات رو در پیام caption می‌ذاریم (در آینده میشه REST زد به Worker)
    pass

# ─── Download: direct URL ─────────────────────────────────────────────────────
async def download_direct(url: str, dest: str, filename: Optional[str]) -> str:
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(3600.0),  # 1 ساعت برای فایل‌های بزرگ
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0"},
    ) as client:
        # اسم فایل از header یا URL
        if not filename:
            head = await client.head(url)
            cd = head.headers.get("content-disposition", "")
            m = re.findall(r'filename[^;=\n]*=["\']?([^"\';\n]+)', cd)
            filename = m[0].strip() if m else url.split("/")[-1].split("?")[0] or "file"

        filepath = os.path.join(dest, filename)
        async with client.stream("GET", url) as res:
            res.raise_for_status()
            with open(filepath, "wb") as f:
                async for chunk in res.aiter_bytes(1024 * 1024):  # 1MB chunks
                    f.write(chunk)

    return filepath

# ─── Download: yt-dlp ─────────────────────────────────────────────────────────
def download_ytdlp(url: str, dest: str, quality: str, filename: Optional[str], platform: str) -> str:
    """Sync — در executor اجرا میشه"""
    
    is_audio = quality == "audio"
    fmt = (
        "bestaudio/best" if is_audio
        else f"bestvideo[height<={quality}][ext=mp4]+bestaudio[ext=m4a]/best[height<={quality}]/best"
    )

    tmpl = os.path.join(dest, "%(title).80s.%(ext)s")
    if filename:
        base = os.path.splitext(filename)[0]
        tmpl = os.path.join(dest, f"{base}.%(ext)s")

    opts: dict = {
        "format": fmt,
        "outtmpl": tmpl,
        "merge_output_format": "mp4",
        "quiet": True,
        "no_warnings": True,
        # سرعت بالا
        "concurrent_fragment_downloads": 16,
        "http_chunk_size": 20 * 1024 * 1024,  # 20MB
        "retries": 10,
        "fragment_retries": 10,
        "file_access_retries": 5,
        # بدون progress callback — فقط وضعیت کلی
        "noprogress": True,
    }

    if os.path.exists(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
        log.info(f"Using cookies from {COOKIES_FILE}")

    if platform == "pornhub":
        opts["http_headers"] = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
            "Referer": "https://www.pornhub.com/",
        }

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        if "entries" in info:
            info = info["entries"][0]
        path = ydl.prepare_filename(info)
        # اگه merge شد، ext تغییر می‌کنه
        if not os.path.exists(path):
            path = os.path.splitext(path)[0] + ".mp4"
        return path

# ─── Upload via Telethon ──────────────────────────────────────────────────────
async def upload_telethon(chat_id: int, filepath: str, caption: str):
    """آپلود با Telethon — هیچ محدودیت سایزی نداره تا 2GB"""
    client = await get_telethon()
    size_mb = os.path.getsize(filepath) / 1024 / 1024
    is_video = filepath.lower().endswith((".mp4", ".mkv", ".webm", ".avi", ".mov"))
    
    log.info(f"Uploading {size_mb:.1f}MB via Telethon...")

    await client.send_file(
        chat_id,
        filepath,
        caption=caption,
        supports_streaming=is_video,
        part_size_kb=512,   # 512KB per part
        workers=4,          # 4 parallel upload streams
    )
    log.info(f"Upload done: {filepath}")

# ─── Main background task ─────────────────────────────────────────────────────
async def run_task(req: ProcessRequest, task_id: str):
    chat_id = req.chatId
    msg_id  = req.messageId

    active_tasks[task_id] = {
        "status": "queued",
        "url": req.url,
        "startedAt": int(time.time() * 1000),
        "userId": req.userId,
    }

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = None

            # ── دانلود ──────────────────────────────────────────────────────
            active_tasks[task_id]["status"] = "downloading"
            await tg_edit(chat_id, msg_id, "⬇️ در حال دانلود...")

            if req.platform in ("youtube", "pornhub"):
                loop = asyncio.get_event_loop()
                filepath = await loop.run_in_executor(
                    None,
                    lambda: download_ytdlp(
                        req.url, tmpdir,
                        req.quality or DEFAULT_QUALITY,
                        req.newName, req.platform or ""
                    )
                )
            else:
                filepath = await download_direct(req.url, tmpdir, req.newName)

            if not filepath or not os.path.exists(filepath):
                raise FileNotFoundError("فایل دانلود نشد")

            size_mb = os.path.getsize(filepath) / 1024 / 1024
            log.info(f"Downloaded: {filepath} ({size_mb:.1f}MB)")

            # ── آپلود ──────────────────────────────────────────────────────
            active_tasks[task_id]["status"] = "uploading"
            await tg_edit(chat_id, msg_id, f"⬆️ در حال آپلود ({size_mb:.1f}MB)...")

            fname = req.newName or Path(filepath).name
            caption = f"📄 {fname}"
            if req.platform == "youtube": caption = f"🎬 {fname}"
            elif req.platform == "pornhub": caption = f"🔞 {fname}"

            # همیشه از Telethon استفاده می‌کنیم — هم برای بزرگ هم کوچیک
            # چون Telethon سریع‌تره و محدودیت نداره
            await upload_telethon(chat_id, filepath, caption)

            # ── اتمام ──────────────────────────────────────────────────────
            await tg_edit(chat_id, msg_id, f"✅ تموم شد! ({size_mb:.1f}MB)")
            active_tasks[task_id]["status"] = "done"

    except Exception as e:
        log.exception(f"Task {task_id} failed")
        err = str(e)[:200]
        await tg_edit(chat_id, msg_id, f"❌ خطا: {err}")
        active_tasks[task_id]["status"] = "error"

    finally:
        # بعد از ۱۰ دقیقه از حافظه حذف میشه
        await asyncio.sleep(600)
        active_tasks.pop(task_id, None)

# ─── Auth helper ─────────────────────────────────────────────────────────────
def check_key(x_api_key: str):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# ─── Routes ──────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"ok": True, "active": len(active_tasks)}

@app.post("/process")
async def process(
    req: ProcessRequest,
    background_tasks: BackgroundTasks,
    x_api_key: str = Header(...),
):
    check_key(x_api_key)
    task_id = hashlib.md5(f"{req.userId}:{req.url}:{time.time()}".encode()).hexdigest()[:16]
    background_tasks.add_task(run_task, req, task_id)
    log.info(f"Task {task_id} queued: {req.url}")
    return {"ok": True, "task_id": task_id}

@app.get("/tasks/{user_id}")
async def get_tasks(user_id: int, x_api_key: str = Header(...)):
    check_key(x_api_key)
    tasks = [
        {**t, "task_id": tid}
        for tid, t in active_tasks.items()
        if t.get("userId") == user_id
    ]
    return {"tasks": tasks}

# ─── Startup ─────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    log.info("🚀 Service starting...")
    try:
        await get_telethon()
    except Exception as e:
        log.warning(f"Telethon startup failed (will retry on first request): {e}")
