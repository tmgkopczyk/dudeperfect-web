from fastapi import APIRouter, Response

router = APIRouter(include_in_schema=False)

ROBOTS_TXT = """User-agent: *
Disallow: /api/
Disallow: /docs
Disallow: /openapi.json
Disallow: /debug/
Disallow: /contact/submit

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
        }
    )
