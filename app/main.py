from fastapi import FastAPI, Request, Query, Form, HTTPException, APIRouter, status
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from typing import Optional
import requests
import os

from . import queries
from .sitemap import router as sitemap_router
from .robots import router as robots_router


# =========================
# App setup
# =========================

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(
    title="Dude Perfect Music DB",
    version="0.1.0",
)

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


# =========================
# Routers
# =========================

pages = APIRouter(include_in_schema=False)
api = APIRouter(prefix="/api")


# =========================
# Config
# =========================

N8N_WEBHOOK_URL = "https://n8n.khomeserver.com/webhook/dp-contact-7b4f92"
TURNSTILE_SECRET = os.getenv("TURNSTILE_SECRET", "")
TURNSTILE_VERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"
TURNSTILE_SITE_KEY = os.getenv("TURNSTILE_SITE_KEY", "")


# =========================
# Helpers
# =========================

def render(request: Request, template: str, context: dict = {}, status_code=200):
    return templates.TemplateResponse(
        template,
        {"request": request, **context},
        status_code=status_code
    )


def verify_turnstile(token: str, remote_ip: Optional[str] = None) -> bool:
    if not TURNSTILE_SECRET:
        return False

    data = {"secret": TURNSTILE_SECRET, "response": token}
    if remote_ip:
        data["remoteip"] = remote_ip

    try:
        r = requests.post(TURNSTILE_VERIFY_URL, data=data, timeout=3)
        r.raise_for_status()
        return bool(r.json().get("success"))
    except requests.RequestException:
        return False


# =========================
# Static / misc
# =========================

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return FileResponse(str(BASE_DIR / "static" / "favicon.ico"))


# =========================
# Pages
# =========================

@pages.get("/", response_class=HTMLResponse)
def home(request: Request):
    return render(request, "index.html")


@pages.get("/search", response_class=HTMLResponse)
def search_home(request: Request):
    return render(request, "search/index.html")


@pages.get("/contact", response_class=HTMLResponse)
def contact_page(request: Request):
    return render(request, "contact.html", {
        "turnstile_site_key": TURNSTILE_SITE_KEY
    })


@pages.post("/contact/submit", response_class=HTMLResponse)
def contact_submit(
    request: Request,
    name: str = Form(...),
    email: str = Form(...),
    message: str = Form(...),
    website: str = Form(""),
    cf_turnstile_response: str = Form("", alias="cf-turnstile-response"),
):
    if website.strip():
        return render(request, "contact_success.html")

    if not name.strip() or not email.strip() or not message.strip():
        return render(request, "contact.html", {
            "error": "Please fill out all fields.",
            "turnstile_site_key": TURNSTILE_SITE_KEY
        }, status.HTTP_400_BAD_REQUEST)

    if not verify_turnstile(cf_turnstile_response, request.client.host):
        return render(request, "contact.html", {
            "error": "Verification failed.",
            "turnstile_site_key": TURNSTILE_SITE_KEY
        }, status.HTTP_400_BAD_REQUEST)

    try:
        requests.post(N8N_WEBHOOK_URL, json={
            "name": name,
            "email": email,
            "message": message,
        }, timeout=2)
    except:
        pass

    return render(request, "contact_success.html")


# =========================
# Songs
# =========================

@pages.get("/songs", response_class=HTMLResponse)
def songs_page(request: Request, q: Optional[str] = None):
    results = queries.search_songs(q) if q else None
    return render(request, "songs/songs.html", {"results": results, "query": q})


@pages.get("/songs/{song_id}", response_class=HTMLResponse)
def song_detail(request: Request, song_id: int):
    song = queries.get_song_detail(song_id)
    if not song:
        raise HTTPException(404)
    return render(request, "songs/song_detail.html", {"song": song})


# =========================
# Artists
# =========================

@pages.get("/artists", response_class=HTMLResponse)
def artists_page(request: Request, q: Optional[str] = None):
    results = queries.search_artists(q) if q else None
    return render(request, "artists/artists.html", {"results": results, "query": q})


@pages.get("/artists/{artist_id}", response_class=HTMLResponse)
def artist_detail(request: Request, artist_id: int):
    artist = queries.get_artist_detail(artist_id)
    if not artist:
        raise HTTPException(404)
    return render(request, "artists/artist_detail.html", {"artist": artist})


# =========================
# Videos
# =========================

@pages.get("/videos", response_class=HTMLResponse)
def videos_page(request: Request, q: Optional[str] = None):
    results = queries.search_videos(q) if q else None
    return render(request, "videos/videos.html", {"results": results, "query": q})


@pages.get("/videos/{video_id}", response_class=HTMLResponse)
def video_detail(request: Request, video_id: int):
    video = queries.get_video_detail_page(video_id)
    if not video:
        raise HTTPException(404)

    return render(request, "videos/video_detail.html", {
        "video": video,
        "battle": queries.get_battle_view(video_id),
        "overtime": queries.get_overtime_view(video_id),
        "bucket_list": queries.get_bucket_list_view(video_id),
        "stereotypes": queries.get_stereotypes_view(video_id),
    })


# =========================
# Categories
# =========================

@pages.get("/videos/categories", response_class=HTMLResponse)
def categories_page(request: Request):
    return render(request, "videos/categories/index.html", {
        "categories": queries.list_video_categories()
    })


@pages.get("/videos/categories/{slug}", response_class=HTMLResponse)
def category_detail(request: Request, slug: str, q: Optional[str] = None):
    category = queries.get_video_category_by_slug(slug)
    if not category:
        raise HTTPException(404)

    videos = queries.list_videos_for_category(category["id"], q=q)
    return render(request, "videos/categories/category_detail.html", {
        "category": category,
        "videos": videos,
        "query": q
    })


# =========================
# API
# =========================

@api.get("/search")
def api_search(q: str):
    return queries.search_songs(q)


@api.get("/songs/{song_id}")
def api_song(song_id: int):
    song = queries.get_song_detail(song_id)
    if not song:
        raise HTTPException(404)
    return song


@api.get("/artists/{artist_id}")
def api_artist(artist_id: int):
    artist = queries.get_artist_detail(artist_id)
    if not artist:
        raise HTTPException(404)
    return artist


@api.get("/videos/{video_id}")
def api_video(video_id: int):
    video = queries.get_video_detail_page(video_id)
    if not video:
        raise HTTPException(404)
    return video


# =========================
# Register routers
# =========================

app.include_router(pages)
app.include_router(api)
app.include_router(sitemap_router)
app.include_router(robots_router)