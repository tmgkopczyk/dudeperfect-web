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
