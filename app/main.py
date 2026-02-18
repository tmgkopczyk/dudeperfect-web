from fastapi import FastAPI, Request, Query, Form
import requests
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import HTTPException
from fastapi import APIRouter
from .queries import *
from .sitemap import router as sitemap_router
from .robots import router as robots_router


N8N_WEBHOOK_URL = "https://n8n.khomeserver.com/webhook/dp-contact-7b4f92"

app = FastAPI(
    title="Dude Perfect Music DB",
    version="0.1.0",
    openapi_tags=[
        {"name": "Songs", "description": "Song-related operations"},
        {"name": "Artists", "description": "Artist-related operations"},
        {"name": "Videos", "description": "Video-related operations"},
        {"name": "Search", "description": "Search endpoints"}
    ],
)

pages_router = APIRouter(include_in_schema=False)
api_router   = APIRouter(prefix="/api")

templates = Jinja2Templates(directory="app/templates")

from fastapi.responses import JSONResponse
#from app.queries import get_battle_view

@app.get("/debug/battles/{video_id}")
def debug_battle_view(video_id: int):
    data = get_battle_view(video_id)
    if not data:
        return JSONResponse(
            status_code=404,
            content={"error": "Not a battle"}
        )
    return data


@pages_router.post("/contact/submit",response_class=HTMLResponse)
def contact_submit(
    request: Request,
    name: str = Form(...),
    email: str = Form(...),
    message: str = Form(...),
    token: str = Form(...),
    website: str = Form("")
):
    payload = {
        "name":name,
        "email":email,
        "message":message,
        "token":token,
        "website":website,
        "page":str(request.url)
    }
    try:
    	requests.post(
        	N8N_WEBHOOK_URL,
        	json=payload,
        	timeout=2   # short timeout now safe
    	)
    except requests.exceptions.RequestException:
        pass  # never fail the form

    #requests.post(N8N_WEBHOOK_URL,json=payload, timeout=5)

    return templates.TemplateResponse(
        "contact_success.html",
        {"request":request}
    )

@pages_router.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request}
    )

@pages_router.get("/search", response_class=HTMLResponse)
def search_home(request: Request):
    return templates.TemplateResponse(
        "search/index.html",
        {"request": request}
    )

@pages_router.get("/contact", response_class=HTMLResponse)
def contact_page(request: Request):
    return templates.TemplateResponse(
        "contact.html",
        {"request": request}
    )



@pages_router.get("/songs", response_class=HTMLResponse)
def songs_page(
    request: Request,
    q: str | None = Query(default=None, max_length=100)
):
    results = search_songs(q) if q else None
    return templates.TemplateResponse(
        "songs/songs.html",
        {
            "request": request,
            "results": results,
            "query": q
        }
    )


@pages_router.get("/songs/{song_id}", response_class=HTMLResponse)
def song_detail_page(request: Request, song_id: int):
    song = get_song_detail(song_id)
    if not song:
        raise HTTPException(status_code=404)

    return templates.TemplateResponse(
        "songs/song_detail.html",
        {"request": request, "song": song}
    )

@pages_router.get("/videos/categories", response_class=HTMLResponse)
def video_categories_page(request: Request):
    categories = list_video_categories()
    return templates.TemplateResponse(
        "videos/categories/index.html",
        {"request": request, "categories": categories}
    )


@pages_router.get("/videos/categories/{slug}", response_class=HTMLResponse)
def video_category_detail_page(
    request: Request,
    slug: str,
    q: str | None = Query(default=None, max_length=100)
):
    category = get_video_category_by_slug(slug)
    if not category:
        raise HTTPException(status_code=404)

    q = q.strip() if q else None
    videos = list_videos_for_category(category["id"],q=q)

    return templates.TemplateResponse(
        "videos/categories/category_detail.html",
        {
            "request": request,
            "category": category,
            "videos": videos,
            "query": q
        }
    )


@api_router.get("/search", tags=["Search"])
def api_search(q: str, limit: int = 50):
    if not q or len(q.strip()) < 1:
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")

    results = search_songs(q.strip(), limit=limit)
    return results

@api_router.get("/song/{spotify_track_id}",tags=["Songs"])
def api_song(spotify_track_id: str):
    song = get_song_by_track_id(spotify_track_id)

    if not song:
        raise HTTPException(status_code=404, detail="Song not found")

    return song

@api_router.get("/songs/{song_id}",tags=["Songs"])
def api_song_detail(song_id: int):
    song = get_song_detail(song_id)
    if not song:
        raise HTTPException(status_code=404)
    return song


@api_router.get("/search/artists",tags=["Search","Artists"])
def api_search_artists(q: str, limit: int = 50):
    if not q or len(q.strip()) < 1:
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")

    return search_artists(q.strip(), limit=limit)

@api_router.get("/artists/{artist_id}",tags=["Artists"])
def api_artist_detail(artist_id: int):
    artist = get_artist_detail(artist_id)

    if not artist:
        raise HTTPException(status_code=404, detail="Artist not found")

    return artist


@pages_router.get("/artists/{artist_id}", response_class=HTMLResponse)
def artist_detail(request: Request, artist_id: int):
    artist = get_artist_detail(artist_id)
    if not artist:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse(
        "artists/artist_detail.html",
        {"request": request, "artist": artist}
    )

@pages_router.get("/artists", response_class=HTMLResponse)
def artists_page(request: Request, q: str | None = None):
    results = search_artists(q) if q else None
    return templates.TemplateResponse(
        "artists/artists.html",
        {
            "request": request,
            "results": results,
            "query": q
        }
    )



@api_router.get("/videos/{video_id}",tags=["Videos"])
def api_video_detail(video_id: int):
    video = get_video_detail_page(video_id)

    if not video:
        raise HTTPException(status_code=404, detail="Video not found")

    return video

@pages_router.get("/videos", response_class=HTMLResponse)
def videos_page(request: Request, q: str | None = None):
    q = q.strip() if q else None
    results = search_videos(q) if q else None
    return templates.TemplateResponse(
        "videos/videos.html",
        {
            "request": request,
            "results": results,
            "query": q
        }
    )

@app.get("/debug/overtime/{video_id}")
def debug_overtime(video_id: int):
    return get_overtime_view(video_id)


@pages_router.get("/videos/{video_id}", response_class=HTMLResponse)
def video_detail_page(request: Request, video_id: int):
    video = get_video_detail_page(video_id)
    battle = get_battle_view(video_id)
    overtime = get_overtime_view(video_id)

    return templates.TemplateResponse(
        "videos/video_detail.html",
        {
            "request": request,
            "video": video,
            "battle": battle,
            "overtime": overtime,
        },
    )




app.include_router(pages_router)
app.include_router(api_router)
app.include_router(sitemap_router)
app.include_router(robots_router)
