from flask import (
    Flask,
    render_template,
    request,
    abort,
    jsonify,
    redirect,
    url_for
)
import math
import os
import requests
import queries
from robots import robots
from sitemap import sitemap


# =========================
# App setup
# =========================

app = Flask(__name__)
app.add_url_rule(
    "/robots.txt",
    endpoint="robots",
    view_func=robots,
)

app.add_url_rule(
    "/sitemap.xml",
    endpoint="sitemap",
    view_func=sitemap,
)

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

@app.route("/favicon.ico")
def favicon():
    return app.send_static_file("favicon.ico")


# =========================
# Pages
# =========================

@app.route("/")
def home():
    return render_template("index.html")


@app.route("/search")
def search_home():
    return render_template("search/index.html")


# =========================
# Stereotypes
# =========================

@app.route("/stereotypes")
def stereotypes_landing_page():
    episodes = queries.get_stereotypes_episodes()

    return render_template(
        "stereotypes/stereotypes_landing.html",
        episodes=episodes,
    )


@app.route("/stereotypes/recurring")
def recurring_stereotypes_page():
    recurring = queries.get_recurring_stereotypes()

    return render_template(
        "stereotypes/recurring.html",
        recurring=recurring,
    )


@app.route("/stereotypes/recurring/<int:recurring_id>")
def recurring_stereotype_detail(recurring_id):
    recurring = queries.get_recurring_stereotype(recurring_id)

    if not recurring:
        abort(404)

    return render_template(
        "stereotypes/recurring_detail.html",
        recurring=recurring,
    )


@app.route("/stereotypes/performers")
def stereotype_performers_page():
    performers = queries.get_stereotype_performers()

    return render_template(
        "stereotypes/performers.html",
        performers=performers,
    )


@app.route("/stereotypes/performers/<int:player_id>")
def stereotype_performer_detail(player_id):
    performer = queries.get_stereotype_performer_view(player_id)

    if not performer:
        abort(404)

    return render_template(
        "stereotypes/performer_detail.html",
        performer=performer,
    )


# =========================
# Contact
# =========================

@app.route("/contact")
def contact_page():
    return render_template(
        "contact.html",
        turnstile_site_key=TURNSTILE_SITE_KEY,
    )


@app.route("/contact/submit", methods=["POST"])
def contact_submit():
    name = request.form.get("name", "")
    email = request.form.get("email", "")
    message = request.form.get("message", "")
    website = request.form.get("website", "")
    cf_turnstile_response = request.form.get(
        "cf-turnstile-response",
        "",
    )

    # Honeypot
    if website.strip():
        return render_template("contact_success.html")

    if not name.strip() or not email.strip() or not message.strip():
        return (
            render_template(
                "contact.html",
                error="Please fill out all fields.",
                turnstile_site_key=TURNSTILE_SITE_KEY,
            ),
            400,
        )

    remote_addr = request.remote_addr

    if not verify_turnstile(
        cf_turnstile_response,
        remote_addr,
    ):
        return (
            render_template(
                "contact.html",
                error="Verification failed.",
                turnstile_site_key=TURNSTILE_SITE_KEY,
            ),
            400,
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

    return render_template("contact_success.html")

# =========================
# Songs
# =========================

@app.route("/songs")
def songs_page():
    q = request.args.get("q")

    if q:
        results = queries.search_songs(q)
        songs = None
        letters = None
    else:
        results = None
        songs = queries.get_all_songs()
        letters = queries.get_song_letters()

    return render_template(
        "songs/songs.html",
        results=results,
        songs=songs,
        letters=letters,
        query=q,
    )


@app.route("/songs/<int:song_id>")
def song_detail(song_id):
    song = queries.get_song_detail(song_id)

    if not song:
        abort(404)

    return render_template(
        "songs/song_detail.html",
        song=song,
    )


# =========================
# Artists
# =========================

@app.route("/artists")
def artists_page():
    q = request.args.get("q")

    if q:
        results = queries.search_artists(q)
        artists = None
        letters = None
    else:
        results = None
        artists = queries.get_all_artists()
        letters = queries.get_artist_letters()

    return render_template(
        "artists/artists.html",
        results=results,
        artists=artists,
        letters=letters,
        query=q,
    )


@app.route("/artists/<int:artist_id>")
def artist_detail(artist_id):
    artist = queries.get_artist_detail(artist_id)

    if not artist:
        abort(404)

    return render_template(
        "artists/artist_detail.html",
        artist=artist,
    )


# =========================
# Players
# =========================

@app.route("/player/<slug>")
def player_page(slug):
    player = queries.get_player_by_slug(slug)

    if not player:
        abort(404)

    return render_template(
        "players/player_detail.html",
        player=player,
        recent_battles=[],
    )


@app.route("/players")
def players_index():
    players = queries.list_players()

    return render_template(
        "players/index.html",
        players=players,
    )


# =========================
# Videos
# =========================

@app.route("/videos")
def videos_page():
    q = request.args.get("q")
    page = request.args.get("page", 1, type=int)

    PER_PAGE = 50

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

    return render_template(
        "videos/videos.html",
        query=q,
        results=results,
        videos=videos,
        page=page,
        total_pages=total_pages,
    )


@app.route("/videos/youtube/<youtube_video_id>")
def video_by_youtube_id(youtube_video_id):
    video_id = queries.get_video_id_by_youtube_id(
        youtube_video_id
    )

    if video_id is None:
        abort(404)

    return redirect(
        url_for(
            "video_detail",
            video_id=video_id,
        )
    )


# =========================
# Video Categories
# =========================

@app.route("/videos/categories")
def categories_page():
    return render_template(
        "videos/categories/index.html",
        categories=queries.list_video_categories(),
    )


@app.route("/videos/categories/<slug>")
def category_detail(slug):
    q = request.args.get("q")

    category = queries.get_video_category_by_slug(slug)

    if not category:
        abort(404)

    videos = queries.list_videos_for_category(
        category["id"],
        q=q,
    )

    return render_template(
        "videos/categories/category_detail.html",
        category=category,
        videos=videos,
        query=q,
    )


@app.route("/videos/<int:video_id>")
def video_detail(video_id):
    video = queries.get_video_detail_page(video_id)

    if not video:
        abort(404)

    return render_template(
        "videos/video_detail.html",
        video=video,
        battle=queries.get_battle_view(video_id),
        overtime=queries.get_overtime_view(video_id),
        bucket_list=queries.get_bucket_list_view(video_id),
        stereotypes=queries.get_stereotypes_view(video_id),
    )


# =========================
# API
# =========================

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "")
    return jsonify(queries.search_songs(q))


@app.route("/api/songs/<int:song_id>")
def api_song(song_id):
    song = queries.get_song_detail(song_id)

    if not song:
        abort(404)

    return jsonify(song)


@app.route("/api/artists/<int:artist_id>")
def api_artist(artist_id):
    artist = queries.get_artist_detail(artist_id)

    if not artist:
        abort(404)

    return jsonify(artist)


@app.route("/api/videos/<int:video_id>")
def api_video(video_id):
    video = queries.get_video_detail_page(video_id)

    if not video:
        abort(404)

    return jsonify(video)

if __name__ == "__main__":
    app.run(
        host="192.168.68.68",
        port=8081,
        debug=True,
    )