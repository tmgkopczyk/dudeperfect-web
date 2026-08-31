from flask import Response


ROBOTS_TXT = """User-agent: *

# Internal/application endpoints
Disallow: /api/
Disallow: /contact/submit

# Avoid crawling search-result combinations
Disallow: /search?

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