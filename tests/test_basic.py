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
