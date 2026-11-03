from fastapi import FastAPI, Request, APIRouter, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from app.robots import router as robots_router
from app.sitemap import router as sitemap_router
from app.api import api as api_router
from app.overtime.routes import router as overtime_router
import math
import os
import requests
from app import queries


# =========================
# App setup
# =========================

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(
    title="Dude Perfect Fan Archive API",
    version="1.0.0",
)

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.globals["environment"] = os.getenv(
    "CURRENT_ENVIRONMENT",
    "development"
)
app.mount(
    "/static",
    StaticFiles(directory=str(BASE_DIR / "static")),
    name="static",
)

# ==========================================
# Static Asset Cache Busting
# ==========================================

STATIC_DIR = BASE_DIR / "static"


def static_version(filename: str) -> int:
    """
    Return the file's modification timestamp.

    When a static file changes, its version changes,
    forcing browsers to request the updated asset.
    """
    path = STATIC_DIR / filename

    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return 0


templates.env.globals["static_version"] = static_version

# =========================
# Routers
# =========================

pages = APIRouter(include_in_schema=False)

SEARCH_RESULTS_PER_CATEGORY = 10
PER_PAGE = 50

@app.middleware("http")
async def log_requests(request: Request, call_next):
    cf_ip = request.headers.get("cf-connecting-ip")
    forwarded_for = request.headers.get("x-forwarded-for")
    user_agent = request.headers.get("user-agent")
    referer = request.headers.get("referer")

    path = request.url.path
    if request.url.query:
        path += f"?{request.url.query}"

    print(
        f"REQUEST "
        f"method={request.method} "
        f"path={path!r} "
        f"cf_ip={cf_ip!r} "
        f"x_forwarded_for={forwarded_for!r} "
        f"user_agent={user_agent!r} "
        f"referer={referer!r}",
        flush=True,
    )

    response = await call_next(request)
    return response
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

def render(request:Request,template:str,context:dict | None = None,status_code: int = 200):
    return templates.TemplateResponse(template,{"request":request,**(context or {})},status_code=status_code)

def verify_turnstile(token: str, remote_ip: str | None = None) -> bool:
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
    return FileResponse(BASE_DIR / "static" / "favicon.ico")

# =========================
# Pages
# =========================

@pages.get("/", response_class=HTMLResponse)
def home(request: Request):
    return render(request, "index.html")


@pages.get("/search", response_class=HTMLResponse)
def search_home(request: Request, q: str = ""):
    q = q.strip()

    results = None

    if q:
        results = {
            "videos": queries.search_videos(q, limit=SEARCH_RESULTS_PER_CATEGORY),
            "battles": queries.search_battles(q, limit=SEARCH_RESULTS_PER_CATEGORY),
            "songs": queries.search_songs(q, limit=SEARCH_RESULTS_PER_CATEGORY),
            "artists": queries.search_artists(q, limit=SEARCH_RESULTS_PER_CATEGORY),
            "players": queries.search_players(q, limit=SEARCH_RESULTS_PER_CATEGORY),
            "stereotypes": queries.search_stereotype_segments(
                q,
                limit=SEARCH_RESULTS_PER_CATEGORY,
            ),
            "recurring_stereotypes": queries.search_recurring_stereotypes(
                q,
                limit=SEARCH_RESULTS_PER_CATEGORY,
            ),
        }

    return render(
        request,
        "search/index.html",
        {
            "query": q,
            "results": results,
        },
    )


# =========================
# Stereotypes
# =========================

@pages.get("/stereotypes", response_class=HTMLResponse)
def stereotypes_landing_page(request: Request):
    episodes = queries.get_stereotypes_episodes()

    return render(
        request,
        "stereotypes/stereotypes_landing.html",
        {
            "episodes": episodes,
        },
    )


@pages.get("/stereotypes/recurring", response_class=HTMLResponse)
def recurring_stereotypes_page(request: Request):
    recurring = queries.get_recurring_stereotypes()

    return render(
        request,
        "stereotypes/recurring.html",
        {
            "recurring": recurring,
        },
    )


@pages.get(
    "/stereotypes/recurring/{recurring_id}",
    response_class=HTMLResponse,
)
def recurring_stereotype_detail(
    request: Request,
    recurring_id: int,
):
    recurring = queries.get_recurring_stereotype(recurring_id)

    if not recurring:
        raise HTTPException(status_code=404)

    return render(
        request,
        "stereotypes/recurring_detail.html",
        {
            "recurring": recurring,
        },
    )


@pages.get("/stereotypes/performers", response_class=HTMLResponse)
def stereotype_performers_page(request: Request):
    performers = queries.get_stereotype_performers()

    return render(
        request,
        "stereotypes/performers.html",
        {
            "performers": performers,
        },
    )


@pages.get(
    "/stereotypes/performers/{player_id}",
    response_class=HTMLResponse,
)
def stereotype_performer_detail(
    request: Request,
    player_id: int,
):
    performer = queries.get_stereotype_performer_view(player_id)

    if not performer:
        raise HTTPException(status_code=404)

    return render(
        request,
        "stereotypes/performer_detail.html",
        {
            "performer": performer,
        },
    )


# =========================
# Contact
# =========================

@pages.get("/contact", response_class=HTMLResponse)
def contact_page(request: Request):
    return render(
        request,
        "contact.html",
        {
            "turnstile_site_key": TURNSTILE_SITE_KEY,
        },
    )


@pages.post("/contact/submit", response_class=HTMLResponse)
def contact_submit(
    request: Request,
    name: str = Form(""),
    email: str = Form(""),
    message: str = Form(""),
    website: str = Form(""),
    cf_turnstile_response: str = Form("", alias="cf-turnstile-response"),
):
    # Honeypot
    if website.strip():
        return render(request, "contact_success.html")

    if not name.strip() or not email.strip() or not message.strip():
        return render(
            request,
            "contact.html",
            {
                "error": "Please fill out all fields.",
                "turnstile_site_key": TURNSTILE_SITE_KEY,
            },
            status_code=400,
        )

    remote_addr = request.client.host if request.client else None

    if not verify_turnstile(
        cf_turnstile_response,
        remote_addr,
    ):
        return render(
            request,
            "contact.html",
            {
                "error": "Verification failed.",
                "turnstile_site_key": TURNSTILE_SITE_KEY,
            },
            status_code=400,
        )

    try:
        requests.post(
            N8N_WEBHOOK_URL,
            json={
                "name": name,
                "email": email,
                "message": message,
            },
            timeout=2,
        )
    except requests.RequestException:
        pass

    return render(request, "contact_success.html")
# =========================
# Songs
# =========================

@pages.get("/songs", response_class=HTMLResponse)
def songs_page(
    request: Request,
    q: str | None = None,
):
    if q:
        results = queries.search_songs(q)
        songs = None
        letters = None
    else:
        results = None
        songs = queries.get_all_songs()
        letters = queries.get_song_letters()

    return render(
        request,
        "songs/songs.html",
        {
            "results": results,
            "songs": songs,
            "letters": letters,
            "query": q,
        },
    )


@pages.get("/songs/{song_id}", response_class=HTMLResponse)
def song_detail(
    request: Request,
    song_id: int,
):
    song = queries.get_song_detail(song_id)

    if not song:
        raise HTTPException(status_code=404)

    return render(
        request,
        "songs/song_detail.html",
        {
            "song": song,
        },
    )


# =========================
# Artists
# =========================

@pages.get("/artists", response_class=HTMLResponse)
def artists_page(
    request: Request,
    q: str | None = None,
):
    if q:
        results = queries.search_artists(q)
        artists = None
        letters = None
    else:
        results = None
        artists = queries.get_all_artists()
        letters = queries.get_artist_letters()

    return render(
        request,
        "artists/artists.html",
        {
            "results": results,
            "artists": artists,
            "letters": letters,
            "query": q,
        },
    )


@pages.get("/artists/{artist_id}", response_class=HTMLResponse)
def artist_detail(
    request: Request,
    artist_id: int,
):
    artist = queries.get_artist_detail(artist_id)

    if not artist:
        raise HTTPException(status_code=404)

    return render(
        request,
        "artists/artist_detail.html",
        {
            "artist": artist,
        },
    )

# =========================
# Players
# =========================

@pages.get("/player/{slug}", response_class=HTMLResponse)
def player_page(
    request: Request,
    slug: str,
):
    player = queries.get_player_by_slug(slug)

    if not player:
        raise HTTPException(status_code=404)

    recent_battles = queries.get_recent_battles_for_player(
        player["id"]
    )
    stereotype_appearances = queries.get_stereotype_appearances_for_player(
        player["id"]
    )

    return render(
        request,
        "players/player_detail.html",
        {
            "player": player,
            "recent_battles": recent_battles,
            "recent_stereotype_appearances": stereotype_appearances,
        },
    )

@pages.get("/player/{slug}/battles", response_class=HTMLResponse)
def player_battles_page(
    request: Request,
    slug: str,
):
    player = queries.get_player_by_slug(slug)

    if not player:
        raise HTTPException(status_code=404)

    battles = queries.get_recent_battles_for_player(
        player["id"],
        limit=None,
    )

    return render(
        request,
        "players/player_battles.html",
        {
            "player": player,
            "battles": battles,
        },
    )

@pages.get("/player/{slug}/stereotypes", response_class=HTMLResponse)
def player_stereotypes_page(
    request: Request,
    slug: str,
):
    player = queries.get_player_by_slug(slug)

    if not player:
        raise HTTPException(status_code=404)

    appearances = queries.get_stereotype_appearances_for_player(
        player["id"],
        limit=None,
    )

    return render(
        request,
        "players/player_stereotypes.html",
        {
            "player": player,
            "appearances": appearances,
        },
    )

@pages.get("/head-to-head", response_class=HTMLResponse)
def head_to_head_page(request: Request, players: str = ""):
    all_players = queries.get_head_to_head_battle_stats()

    by_slug = {p["slug"]: p for p in all_players}

    requested = [
        slug.strip()
        for slug in players.split(",")
        if slug.strip()
    ]

    selected_slugs = list(
        dict.fromkeys(
            slug for slug in requested
            if slug in by_slug
        )
    )

    if not players:
        selected_slugs = ["tyler-toney", "coby-cotton"]

    selected = (
        [by_slug[slug] for slug in selected_slugs]
        if 2 <= len(selected_slugs) <= 5
        else []
    )

    # ------------------------------------------
    # Battle Statistics
    # ------------------------------------------

    battle_metrics = [
        ("appearances", "Battle appearances", "count"),
        ("individual_wins", "Individual wins", "count"),
        ("team_wins", "Team wins", "count"),
        ("total_wins", "Unique battles won", "count"),
        ("win_percentage", "Win rate", "percent"),
    ]

    # ------------------------------------------
    # Wheel Hosting Statistics
    # ------------------------------------------

    wheel_metrics = [
        ("wheel_times_selected", "Times selected by the wheel", "count"),
        ("wheel_events_hosted", "Wheel events hosted", "count"),
        ("wheel_episodes_hosted", "Wheel episodes hosted", "count"),
        ("wheel_unfortunate_hosted", "Wheel Unfortunate events hosted", "count"),
        ("wheel_variants_hosted", "Wheel Fortunate events hosted", "count"),
    ]

    cool_not_cool_metrics = [
        ("cnc_items_presented", "Items presented", "count"),
        ("cnc_super_cool_items", "Super Cool items", "count"),
        ("cnc_super_not_cool_items", "Super Not Cool items", "count"),
        ("cnc_cool_majority_items", "Cool-majority items", "count"),
        ("cnc_not_cool_majority_items", "Not Cool-majority items", "count"),
        ("cnc_cool_votes_received", "Cool votes received", "count"),
        ("cnc_not_cool_votes_received", "Not Cool votes received", "count"),
        ("cnc_cool_votes_cast", "Cool votes cast", "count"),
        ("cnc_not_cool_votes_cast", "Not Cool votes cast", "count"),
    ]

    # ------------------------------------------
    # Betcha Statistics
    # ------------------------------------------

    betcha_metrics = [
        ("betcha_challenges_presented", "Challenges presented", "count"),
        ("betcha_successful_challenges", "Successful challenges", "count"),
        ("betcha_failed_challenges", "Failed challenges", "count"),
        ("betcha_success_rate", "Challenge success rate", "percent"),
        ("betcha_predictions_made", "Predictions made", "count"),
        ("betcha_correct_predictions", "Correct predictions", "count"),
        ("betcha_incorrect_predictions", "Incorrect predictions", "count"),
        ("betcha_prediction_accuracy", "Prediction accuracy", "percent"),
        ("betcha_agree_predictions", "Agree predictions", "count"),
        ("betcha_disagree_predictions", "Disagree predictions", "count"),
    ]

    get_crafty_metrics = [
        ("get_crafty_segment_appearances", "Segment appearances", "count"),
        ("get_crafty_challenges_participated", "Challenges participated", "count"),
        ("get_crafty_completed_challenges", "Completed challenges", "count"),
        ("get_crafty_challenges_won", "Challenges won", "count"),
        ("get_crafty_challenges_lost", "Challenges lost", "count"),
        ("get_crafty_win_rate", "Win rate", "percent"),
        ("get_crafty_second_place", "Second-place finishes", "count"),
        ("get_crafty_third_place", "Third-place finishes", "count"),
        ("get_crafty_podium_finishes", "Podium finishes", "count"),
        ("get_crafty_average_placement", "Average placement", "decimal"),
    ]

    # ------------------------------------------
    # Statistical Leaders
    # ------------------------------------------

    all_metrics = (
        battle_metrics
        + wheel_metrics
        + cool_not_cool_metrics
        + betcha_metrics
        + get_crafty_metrics
    )

    leaders = {
        key: max(
            (p[key] for p in selected),
            default=None
        )
        for key, _, _ in all_metrics
    }

    return render(request, "players/head_to_head.html", {
        "all_players": all_players,
        "cool_not_cool_metrics": cool_not_cool_metrics,
        "selected": selected,
        "selected_slugs": selected_slugs,
        "battle_metrics": battle_metrics,
        "wheel_metrics": wheel_metrics,
        "betcha_metrics": betcha_metrics,
        "get_crafty_metrics": get_crafty_metrics,
        "leaders": leaders,
        "selection_error": bool(players) and not selected,
    })

@pages.get("/players", response_class=HTMLResponse)
def players_index(request: Request):
    players = queries.list_players()

    return render(
        request,
        "players/index.html",
        {
            "players": players,
        },
    )


# =========================
# Videos
# =========================

@pages.get("/videos", response_class=HTMLResponse)
def videos_page(
    request: Request,
    q: str | None = None,
    page: int = 1,
):
    if page < 1:
        page = 1

    if q:
        results = queries.search_videos(q)
        videos = None
        total_pages = None
    else:
        results = None

        total = queries.get_video_count()
        total_pages = math.ceil(total / PER_PAGE)

        videos = queries.get_videos(
            limit=PER_PAGE,
            offset=(page - 1) * PER_PAGE,
        )

    return render(
        request,
        "videos/videos.html",
        {
            "query": q,
            "results": results,
            "videos": videos,
            "page": page,
            "total_pages": total_pages,
        },
    )


@pages.get("/videos/youtube/{youtube_video_id}")
def video_by_youtube_id(youtube_video_id: str):
    video_id = queries.get_video_id_by_youtube_id(
        youtube_video_id
    )

    if video_id is None:
        raise HTTPException(status_code=404)

    return RedirectResponse(
        url=f"/videos/{video_id}",
        status_code=302,
    )


# =========================
# Video Categories
# =========================

@pages.get("/videos/categories", response_class=HTMLResponse)
def categories_page(request: Request):
    return render(
        request,
        "videos/categories/index.html",
        {
            "categories": queries.list_video_categories(),
        },
    )


@pages.get(
    "/videos/categories/{slug}",
    response_class=HTMLResponse,
)
def category_detail(
    request: Request,
    slug: str,
    q: str | None = None,
):
    category = queries.get_video_category_by_slug(slug)

    if not category:
        raise HTTPException(status_code=404)

    videos = queries.list_videos_for_category(
        category["id"],
        q=q,
    )

    return render(
        request,
        "videos/categories/category_detail.html",
        {
            "category": category,
            "videos": videos,
            "query": q,
        },
    )


@pages.get("/videos/{video_id}", response_class=HTMLResponse)
def video_detail(
    request: Request,
    video_id: int,
):
    video = queries.get_video_detail_page(video_id)

    if not video:
        raise HTTPException(status_code=404)

    return render(
        request,
        "videos/video_detail.html",
        {
            "video": video,
            "battle": queries.get_battle_view(video_id),
            "overtime": queries.get_overtime_view(video_id),
            "bucket_list": queries.get_bucket_list_view(video_id),
            "stereotypes": queries.get_stereotypes_view(video_id),
        },
    )

# =========================
# Battles
# =========================

@pages.get("/battles", response_class=HTMLResponse)
def battles_page(request: Request):
    battles = queries.get_battles()

    return render(
        request,
        "battles/index.html",
        {
            "battles": battles,
        },
    )


@pages.get("/battles/{battle_id}", response_class=HTMLResponse)
def battle_detail(
    request: Request,
    battle_id: int,
):
    video_id = queries.get_battle_video_id(battle_id)

    if video_id is None:
        raise HTTPException(status_code=404)

    video = queries.get_video_detail_page(video_id)
    battle = queries.get_battle_view(video_id)

    if not video or not battle:
        raise HTTPException(status_code=404)

    return render(
        request,
        "battles/detail.html",
        {
            "video": video,
            "battle": battle,
        },
    )


app.include_router(pages)
app.include_router(overtime_router)
app.include_router(api_router)
app.include_router(robots_router)
app.include_router(sitemap_router)
