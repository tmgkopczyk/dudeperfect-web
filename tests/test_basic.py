from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_homepage():
    response = client.get("/")
    assert response.status_code == 200


def test_songs():
    response = client.get("/songs")
    assert response.status_code == 200


def test_artists():
    response = client.get("/artists")
    assert response.status_code == 200


def test_players():
    response = client.get("/players")
    assert response.status_code == 200


def test_overtime():
    response = client.get("/overtime")
    assert response.status_code == 200


def test_search():
    response = client.get("/search")
    assert response.status_code == 200


def test_robots_txt():
    response = client.get("/robots.txt")
    assert response.status_code == 200


def test_sitemap_xml():
    response = client.get("/sitemap.xml")
    assert response.status_code == 200

def test_song_detail():
    response = client.get("/songs/1")
    assert response.status_code == 200
    assert "Running from the Scene" in response.text


def test_artist_detail():
    response = client.get("/artists/1")
    assert response.status_code == 200
    assert "Manic Bloom" in response.text


def test_video_detail():
    response = client.get("/videos/1")
    assert response.status_code == 200
    assert "Aggie Skates Off Roof" in response.text


def test_song_not_found():
    response = client.get("/songs/999999999")
    assert response.status_code == 404


def test_artist_not_found():
    response = client.get("/artists/999999999")
    assert response.status_code == 404


def test_video_not_found():
    response = client.get("/videos/999999999")
    assert response.status_code == 404

def test_search_artist():
    response = client.get("/search", params={"q": "Manic Bloom"})
    assert response.status_code == 200
    assert "Manic Bloom" in response.text


def test_search_song():
    response = client.get("/search", params={"q": "Running from the Scene"})
    assert response.status_code == 200
    assert "Running from the Scene" in response.text

def test_player_detail():
    response = client.get("/player/tyler-toney")
    assert response.status_code == 200
    assert "Tyler" in response.text
    assert "/player/tyler-toney/battles" in response.text
    assert "/player/tyler-toney/stereotypes" in response.text

def test_player_not_found():
    response = client.get("/player/definitely-not-a-real-player")
    assert response.status_code == 404

def test_api_songs():
    response = client.get("/api/songs")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")

    data = response.json()
    assert "items" in data
    assert isinstance(data["items"], list)
    assert len(data["items"]) > 0

    song = data["items"][0]
    assert {"id", "title", "spotify_track_id", "letter_group", "artists"} <= song.keys()


def test_api_artists():
    response = client.get("/api/artists")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")

    data = response.json()
    assert "items" in data
    assert isinstance(data["items"], list)
    assert len(data["items"]) > 0

    artist = data["items"][0]
    assert {"id", "name", "letter_group", "song_count"} <= artist.keys()


def test_api_videos():
    response = client.get("/api/videos")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")

    data = response.json()
    assert "items" in data
    assert isinstance(data["items"], list)
    assert len(data["items"]) > 0

    video = data["items"][0]
    assert {"id", "title", "published_at", "song_count"} <= video.keys()


def test_api_players():
    response = client.get("/api/players")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")

    data = response.json()
    assert "items" in data
    assert isinstance(data["items"], list)
    assert len(data["items"]) > 0

    player = data["items"][0]
    assert {"id", "name", "slug"} <= player.keys()


def test_api_song_detail():
    response = client.get("/api/songs/1")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == 1
    assert data["title"] == "Running from the Scene"


def test_api_artist_detail():
    response = client.get("/api/artists/1")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == 1
    assert data["name"] == "Manic Bloom"


def test_api_video_detail():
    response = client.get("/api/videos/1")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == 1
    assert "Aggie Skates Off Roof" in data["title"]


def test_api_player_detail():
    response = client.get("/api/players/3")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == 3
    assert data["name"] == "Coby"
    assert data["slug"] == "coby-cotton"


def test_api_song_not_found():
    response = client.get("/api/songs/999999999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Song not found"


def test_api_artist_not_found():
    response = client.get("/api/artists/999999999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Artist not found"


def test_api_video_not_found():
    response = client.get("/api/videos/999999999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Video not found"


def test_api_player_not_found():
    response = client.get("/api/players/999999999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Player not found"

def test_api_search_all():
    response = client.get(
        "/api/search",
        params={"q": "Manic Bloom"},
    )
    assert response.status_code == 200

    data = response.json()
    assert "artists" in data
    assert any(
        artist["name"] == "Manic Bloom"
        for artist in data["artists"]
    )


def test_api_search_artists():
    response = client.get(
        "/api/search",
        params={
            "q": "Manic Bloom",
            "type": "artists",
        },
    )
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    assert any(
        artist["name"] == "Manic Bloom"
        for artist in data
    )


def test_api_search_songs():
    response = client.get(
        "/api/search",
        params={
            "q": "Running from the Scene",
            "type": "songs",
        },
    )
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    assert any(
        song["title"] == "Running from the Scene"
        for song in data
    )

def test_api_search_invalid_type():
    response = client.get(
        "/api/search",
        params={
            "q": "Dude Perfect",
            "type": "not-a-real-type",
        },
    )
    assert response.status_code == 400

    data = response.json()
    assert data["detail"]["error"] == "Invalid search type"
    assert "valid_types" in data["detail"]


def test_api_search_limit_too_high():
    response = client.get(
        "/api/search",
        params={
            "q": "Dude Perfect",
            "limit": 101,
        },
    )
    assert response.status_code == 422


def test_api_search_limit_too_low():
    response = client.get(
        "/api/search",
        params={
            "q": "Dude Perfect",
            "limit": 0,
        },
    )
    assert response.status_code == 422

def test_api_battles():
    response = client.get("/api/battles", params={"limit": 3})
    assert response.status_code == 200

    data = response.json()
    assert "items" in data
    assert "count" in data
    assert isinstance(data["items"], list)
    assert len(data["items"]) == 3

    battle = data["items"][0]
    assert {
        "battle_id",
        "video_id",
        "winner",
        "description",
        "title",
        "published_at",
        "youtube_video_id",
    } <= battle.keys()


def test_api_battle_detail():
    response = client.get("/api/battles/110")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == 110
    assert data["title"] == "All Sports Golf Battle 7"
    assert data["winner"] == "Cody / Connor (Cody, Connor Harris)"

    assert isinstance(data["teams"], list)
    assert len(data["teams"]) == 5

    assert isinstance(data["timeline"], list)
    assert len(data["timeline"]) == 4

    assert isinstance(data["final_standings"], list)
    assert data["final_standings"][0]["name"] == "Cody / Connor"
    assert data["final_standings"][0]["status"] == "winner"

def test_api_battle_not_found():
    response = client.get("/api/battles/999999999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Battle not found"

def test_api_stereotypes():
    response = client.get("/api/stereotypes", params={"limit": 3})
    assert response.status_code == 200

    data = response.json()
    assert "items" in data
    assert "count" in data
    assert isinstance(data["items"], list)
    assert len(data["items"]) == 3

    episode = data["items"][0]
    assert {
        "id",
        "video_id",
        "episode_number",
        "theme",
        "title",
        "youtube_video_id",
        "published_at",
        "segment_count",
    } <= episode.keys()


def test_api_stereotype_detail():
    response = client.get("/api/stereotypes/35")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == 35
    assert data["episode_number"] == 35
    assert data["theme"] == "Birthday"

    assert isinstance(data["segments"], list)
    assert len(data["segments"]) == 18

    assert data["segments"][0]["segment_order"] == 1
    assert data["segments"][0]["name"] == "Not So Sweet Sixteen"

    prank_bros = data["segments"][3]
    assert prank_bros["name"] == "The Prank Bros"
    assert prank_bros["recurring_name"] == "The Prank Brothers"


def test_api_stereotype_not_found():
    response = client.get("/api/stereotypes/999999999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Stereotype episode not found"

def test_api_recurring_stereotypes():
    response = client.get("/api/stereotypes/recurring")
    assert response.status_code == 200

    data = response.json()
    assert "items" in data
    assert "count" in data
    assert isinstance(data["items"], list)
    assert data["count"] == 7

    recurring = data["items"][0]
    assert recurring["id"] == 1
    assert recurring["name"] == "The Rage Monster"
    assert recurring["appearance_count"] == 34
    assert recurring["episode_count"] == 34
    assert 1 in recurring["episode_numbers"]


def test_api_recurring_stereotype_detail():
    response = client.get("/api/stereotypes/recurring/1")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == 1
    assert data["name"] == "The Rage Monster"
    assert data["appearance_count"] == 34

    assert isinstance(data["appearances"], list)
    assert len(data["appearances"]) == 34

    first = data["appearances"][0]
    assert first["episode_number"] == 1
    assert first["theme"] == "Pickup Basketball"
    assert first["segment_name"] == "The Rage Monster"
    assert first["main_performer"] == "Tyler"


def test_api_recurring_stereotype_not_found():
    response = client.get("/api/stereotypes/recurring/999999999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Recurring stereotype not found"

def test_api_stereotype_performers():
    response = client.get("/api/stereotypes/performers")
    assert response.status_code == 200

    data = response.json()
    assert "items" in data
    assert "count" in data
    assert isinstance(data["items"], list)
    assert data["count"] == 35

    performer = data["items"][0]
    assert performer["id"] == 1
    assert performer["name"] == "Tyler"
    assert performer["slug"] == "tyler-toney"
    assert performer["stereotype_count"] == 206
    assert performer["episode_count"] == 36


def test_api_stereotype_performer_detail():
    response = client.get("/api/stereotypes/performers/1")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == 1
    assert data["name"] == "Tyler"
    assert data["slug"] == "tyler-toney"
    assert data["appearance_count"] == 206
    assert data["episode_count"] == 36

    assert isinstance(data["appearances"], list)
    assert len(data["appearances"]) == 206

    first = data["appearances"][0]
    assert first["segment_name"] == "The Forever Nickname"
    assert first["episode_number"] == 36
    assert first["theme"] == "School"
    assert first["is_main_performer"] is True


def test_api_stereotype_performer_not_found():
    response = client.get("/api/stereotypes/performers/999999999")
    assert response.status_code == 404
    assert response.json()["detail"] == "Stereotype performer not found"

def test_api_player_battles():
    response = client.get("/api/players/3/battles")
    assert response.status_code == 200

    data = response.json()
    assert "items" in data
    assert "count" in data
    assert isinstance(data["items"], list)
    assert data["count"] == len(data["items"])
    assert data["count"] > 0

    battle = data["items"][0]
    assert {
        "battle_id",
        "video_id",
        "title",
        "published_at",
        "accent_color",
        "won",
    } <= battle.keys()

    assert isinstance(battle["battle_id"], int)
    assert isinstance(battle["won"], bool)


def test_api_player_battles_not_found():
    response = client.get("/api/players/999999999/battles")
    assert response.status_code == 404

def test_api_player_stereotypes():
    response = client.get("/api/players/3/stereotypes")
    assert response.status_code == 200

    data = response.json()
    assert "items" in data
    assert "count" in data
    assert isinstance(data["items"], list)
    assert data["count"] == len(data["items"])
    assert data["count"] > 0

    stereotype = data["items"][0]
    assert {
        "segment_id",
        "segment_order",
        "name",
        "episode_number",
        "theme",
        "video_id",
        "title",
        "published_at",
    } <= stereotype.keys()

    assert isinstance(stereotype["segment_id"], int)
    assert isinstance(stereotype["episode_number"], int)


def test_api_player_stereotypes_not_found():
    response = client.get("/api/players/999999999/stereotypes")
    assert response.status_code == 404


def test_api_video_categories():
    response = client.get("/api/videos/categories")
    assert response.status_code == 200

    data = response.json()
    assert "items" in data
    assert "count" in data
    assert isinstance(data["items"], list)
    assert data["count"] == 6

    category = data["items"][0]
    assert {
        "slug",
        "title",
        "description",
    } <= category.keys()

    assert category["slug"] == "battles"
    assert category["title"] == "Battles"


def test_api_video_category_detail():
    response = client.get("/api/videos/categories/battles")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == 1
    assert data["slug"] == "battles"
    assert data["title"] == "Battles"

    assert isinstance(data["videos"], list)
    assert data["video_count"] == len(data["videos"])
    assert data["video_count"] > 0

    video = data["videos"][0]
    assert {
        "id",
        "title",
        "published_at",
        "song_count",
    } <= video.keys()


def test_api_video_category_not_found():
    response = client.get("/api/videos/categories/definitely-not-a-real-category")
    assert response.status_code == 404

def test_api_video_by_youtube_id():
    response = client.get("/api/videos/youtube/mWGA_yTpwQ0")
    assert response.status_code == 200

    data = response.json()
    assert data["id"] == 1
    assert data["youtube_video_id"] == "mWGA_yTpwQ0"
    assert "Aggie Skates Off Roof" in data["title"]

    assert "songs" in data
    assert isinstance(data["songs"], list)


def test_api_video_by_youtube_id_not_found():
    response = client.get("/api/videos/youtube/definitely-not-a-real-youtube-id")
    assert response.status_code == 404

def test_video_detail_with_battle_stats():
    response = client.get("/videos/390")
    assert response.status_code == 200

    assert "We Played Every Sport On Go-Karts" in response.text
    assert "Battle Rounds" in response.text
    assert "Sub-rounds" in response.text

def test_player_battle_history():
    response = client.get("/player/tyler-toney/battles")

    assert response.status_code == 200
    assert "Tyler Toney Battles" in response.text
    assert "We Played Every Sport On Go-Karts" in response.text
    assert "Paper Airplane Battle" in response.text

def test_player_battle_history_not_found():
    response = client.get("/player/definitely-not-a-real-player/battles")

    assert response.status_code == 404

def test_player_stereotype_history():
    response = client.get("/player/tyler-toney/stereotypes")

    assert response.status_code == 200
    assert "Tyler Toney Stereotype Appearances" in response.text
    assert "206 appearances" in response.text
    assert "The Forever Nickname" in response.text
    assert "Golf Stereotypes" in response.text


def test_player_stereotype_history_not_found():
    response = client.get(
        "/player/definitely-not-a-real-player/stereotypes"
    )

    assert response.status_code == 404
