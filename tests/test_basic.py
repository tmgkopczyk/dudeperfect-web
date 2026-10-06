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
