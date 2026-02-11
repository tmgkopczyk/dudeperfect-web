from fastapi import APIRouter, Response
from sqlalchemy import text
from .db import engine
from .queries import list_video_categories

router = APIRouter(include_in_schema=False)

BASE_URL = "https://dudeperfectfanarchive.com"  # <-- change this


@router.get("/sitemap.xml")
def sitemap():
    urls: list[str] = []

    # --- Static pages ---
    urls.extend([
        f"{BASE_URL}/",
        f"{BASE_URL}/videos",
        f"{BASE_URL}/songs",
        f"{BASE_URL}/artists",
        f"{BASE_URL}/videos/categories",
    ])

    # --- Category pages (DB-backed slugs) ---
    categories = list_video_categories()
    for cat in categories:
        urls.append(f"{BASE_URL}/videos/categories/{cat['slug']}")

    with engine.connect() as conn:
        # --- Videos ---
        for row in conn.execute(text("SELECT id FROM videos")):
            urls.append(f"{BASE_URL}/videos/{row.id}")

        # --- Songs ---
        for row in conn.execute(text("SELECT id FROM songs")):
            urls.append(f"{BASE_URL}/songs/{row.id}")

        # --- Artists ---
        for row in conn.execute(text("SELECT id FROM artists")):
            urls.append(f"{BASE_URL}/artists/{row.id}")

    return Response(
        content=render_sitemap(urls),
        media_type="application/xml"
    )


def render_sitemap(urls: list[str]) -> str:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]

    for url in urls:
        lines.append("  <url>")
        lines.append(f"    <loc>{url}</loc>")
        lines.append("  </url>")

    lines.append("</urlset>")
    return "\n".join(lines)
