from fastapi import APIRouter, Response


router = APIRouter(include_in_schema=False)


ROBOTS_TXT = """User-agent: *

# Internal/application endpoints
Disallow: /api/
Disallow: /contact/submit

# Avoid crawling search-result combinations
Disallow: /search?

Sitemap: https://dudeperfectfanarchive.com/sitemap.xml
"""


@router.get("/robots.txt")
def robots():
    return Response(
        content=ROBOTS_TXT,
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )