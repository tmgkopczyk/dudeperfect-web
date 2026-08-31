from flask import Response


ROBOTS_TXT = """User-agent: *
Disallow: /api/
Disallow: /docs
Disallow: /openapi.json
Disallow: /debug/
Disallow: /contact/submit

Sitemap: https://dudeperfectfanarchive.com/sitemap.xml
"""


def robots():
    return Response(
        ROBOTS_TXT,
        mimetype="text/plain",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )