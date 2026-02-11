from collections import defaultdict
from sqlalchemy import text
from .db import engine

def get_battle_view(video_id: int):
    with engine.connect() as conn:
        battle_row = conn.execute(
            text("""
                SELECT
                b.id AS battle_id,
                b.notes,
                b.winner,
                d.name AS definition_name,
                d.description,
                v.id AS video_id,
                v.title
                FROM battles b
                JOIN battle_definitions d ON d.id = b.definition_id
                JOIN videos v ON v.id = b.video_id
                WHERE v.id = :video_id
                LIMIT 1
            """),
            {"video_id": video_id}
        ).mappings().fetchone()


        if not battle_row:
            return None

        players = conn.execute(
            text("""
                SELECT
                  name,
                  is_guest,
                  notes
                FROM battle_players
                WHERE battle_id = :battle_id
                ORDER BY is_guest, name
            """),
            {"battle_id": battle_row["battle_id"]}
        ).mappings().all()
        rounds = conn.execute(
        text("""
            SELECT
            br.id,
            br.round_order,
            br.name
            FROM battle_rounds br
            WHERE br.battle_id = :battle_id
            ORDER BY br.round_order
        """),
        {"battle_id": battle_row["battle_id"]}
        ).mappings().all()

        timeline = []

        for r in rounds:
            results = conn.execute(
                text("""
                    SELECT
                    name,
                    status,
                    placement,
                    notes
                    FROM battle_round_participants
                    WHERE battle_round_id = :round_id
                    ORDER BY placement NULLS LAST, name
                """),
                {"round_id": r["id"]}
            ).mappings().all()

            timeline.append({
                "name": r["name"],
                "results": [dict(x) for x in results]
            })



    # 👇 SHAPE DATA FOR THE TEMPLATE
    return {
        "id": battle_row["battle_id"],
        "winner": battle_row["winner"],   # 👈 THIS is the key line
        "format": "standard",
        "description": battle_row["description"],
        "notes": battle_row["notes"],
        "teams": [
            {
                "name": "Players",
                "players": [dict(p) for p in players]
            }
        ],
        "timeline": timeline,
        "final_standings": []
    }






def get_song_detail(song_id: int):
    sql = text("""SELECT
  s.id               AS song_id,
  s.title            AS song_title,
  s.spotify_track_id AS spotify_track_id,

  a.name             AS artist_name,
  sa.artist_order    AS artist_order,

  v.id               AS video_id,
  v.title            AS video_title,
  v.youtube_video_id AS youtube_video_id

FROM songs s
LEFT JOIN song_artists sa ON sa.song_id = s.id
LEFT JOIN artists a       ON a.id = sa.artist_id
LEFT JOIN video_songs vs  ON vs.song_id = s.id
LEFT JOIN videos v        ON v.id = vs.video_id

WHERE s.id = :song_id
ORDER BY sa.artist_order, v.published_at;
""")

    with engine.connect() as conn:
        rows = conn.execute(sql, {"song_id": song_id}).mappings().all()

    if not rows:
        return None

    song = {
        "id": rows[0]["song_id"],
        "title": rows[0]["song_title"],
        "spotify_track_id": rows[0]["spotify_track_id"],
        "artists": [],
        "videos": []
    }

    seen_artists = set()
    seen_videos = set()

    for row in rows:
        if row["artist_name"] and row["artist_name"] not in seen_artists:
            song["artists"].append(row["artist_name"])
            seen_artists.add(row["artist_name"])

        if row["video_id"] and row["video_id"] not in seen_videos:
            song["videos"].append({
                "id": row["video_id"],
                "title": row["video_title"],
                "youtube_video_id": row["youtube_video_id"]
            })
            seen_videos.add(row["video_id"])

    return song


def search_songs(query: str, limit: int = 50):
    sql = text("""
        SELECT
     s.id,
       s.title,
       s.spotify_track_id,
       array_agg(a.name ORDER BY sa.artist_order) AS artists
    FROM songs s
    JOIN song_artists sa ON sa.song_id = s.id
    JOIN artists a ON a.id = sa.artist_id
    WHERE unaccent(lower(s.title))
                LIKE unaccent(lower(:q))
    GROUP BY s.id
    ORDER BY s.title
    LIMIT :limit
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "q": f"%{query}%",
                "limit": limit
            }
        ).mappings().all()

    # Convert RowMapping → dict
    return [
    {
        "id": row["id"],
        "title": row["title"],
        "spotify_track_id": row["spotify_track_id"],
        "artists": row["artists"],
    }
    for row in rows
    ]

def get_song_by_track_id(track_id: str):
    sql = text("""
        SELECT
          s.title,
          s.spotify_track_id,
          array_agg(a.name ORDER BY sa.artist_order) AS artists
        FROM songs s
        JOIN song_artists sa ON sa.song_id = s.id
        JOIN artists a ON a.id = sa.artist_id
        WHERE s.spotify_track_id = :track_id
        GROUP BY s.id
        LIMIT 1
    """)

    with engine.connect() as conn:
        row = conn.execute(
            sql,
            {"track_id": track_id}
        ).mappings().first()

    if not row:
        return None

    return {
        "title": row["title"],
        "spotify_track_id": row["spotify_track_id"],
        "artists": row["artists"],
    }

def search_artists(query: str, limit: int = 50):
    sql = text("""
        SELECT
          a.id,
          a.name,
          a.spotify_artist_id,
          COUNT(DISTINCT sa.song_id) AS song_count
        FROM artists a
        LEFT JOIN song_artists sa ON sa.artist_id = a.id
        WHERE unaccent(lower(a.name))
              LIKE unaccent(lower(:q))
        GROUP BY a.id
        ORDER BY a.name
        LIMIT :limit
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "q": f"%{query}%",
                "limit": limit
            }
        ).mappings().all()

    return [
        {
            "id": row["id"],
            "name": row["name"],
            "spotify_artist_id": row["spotify_artist_id"],
            "song_count": row["song_count"],
        }
        for row in rows
    ]


def search_videos(query: str, limit: int = 50):
    sql = text("""
        SELECT
          v.id,
          v.title,
          v.youtube_video_id,
          v.published_at,
          COUNT(DISTINCT vs.song_id) AS song_count
        FROM videos v
        LEFT JOIN video_songs vs ON vs.video_id = v.id
        WHERE unaccent(lower(v.title))
              LIKE unaccent(lower(:q))
        GROUP BY v.id
        ORDER BY v.published_at DESC
        LIMIT :limit
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "q": f"%{query}%",
                "limit": limit
            }
        ).mappings().all()

    return [
        {
            "id": row["id"],
            "title": row["title"],
            "youtube_video_id": row["youtube_video_id"],
            "published_at": row["published_at"],
            "song_count": row["song_count"],
        }
        for row in rows
    ]

def get_video_detail(video_id: int):
    sql = text("""
        SELECT
          s.title,
          s.spotify_track_id,
          array_agg(a.name ORDER BY sa.artist_order) AS artists
        FROM video_songs vs
        JOIN songs s ON s.id = vs.song_id
        JOIN song_artists sa ON sa.song_id = s.id
        JOIN artists a ON a.id = sa.artist_id
        WHERE vs.video_id = :video_id
        GROUP BY s.id
        ORDER BY s.title
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {"video_id": video_id}
        ).mappings().all()

    return [
        {
            "title": row["title"],
            "spotify_track_id": row["spotify_track_id"],
            "artists": row["artists"],
        }
        for row in rows
    ]

def get_video_detail_page(video_id: int):
    sql = text("""
        SELECT
          v.id                AS video_id,
          v.title             AS video_title,
          v.youtube_video_id  AS youtube_video_id,
          v.published_at      AS published_at,

          s.id                AS song_id,
          s.title             AS song_title,
          s.spotify_track_id  AS spotify_track_id,

          a.name              AS artist_name
        FROM videos v
        LEFT JOIN video_songs vs  ON vs.video_id = v.id
        LEFT JOIN songs s         ON s.id = vs.song_id
        LEFT JOIN song_artists sa ON sa.song_id = s.id
        LEFT JOIN artists a       ON a.id = sa.artist_id
        WHERE v.id = :video_id
        ORDER BY s.title, sa.artist_order
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {"video_id": video_id}
        ).mappings().all()

    if not rows:
        return None

    video = {
        "id": rows[0]["video_id"],
        "title": rows[0]["video_title"],
        "youtube_video_id": rows[0]["youtube_video_id"],
        "published_at": rows[0]["published_at"],
        "songs": {}
    }

    for row in rows:
        if row["song_id"] is None:
            continue

        song = video["songs"].setdefault(
            row["song_id"],
            {
                "id": row["song_id"],
                "title": row["song_title"],
                "spotify_track_id": row["spotify_track_id"],
                "artists": []
            }
        )

        if row["artist_name"]:
            song["artists"].append(row["artist_name"])

    video["songs"] = list(video["songs"].values())
    return video



def get_artist_detail(artist_id: int):
    sql = text("""
        SELECT
          a.id                AS artist_id,
          a.name              AS artist_name,
          a.spotify_artist_id AS spotify_artist_id,

          s.id                AS song_id,
          s.title             AS song_title,
          s.spotify_track_id  AS spotify_track_id,

          v.id                AS video_id,
          v.title             AS video_title,
          v.youtube_video_id  AS youtube_video_id

        FROM artists a
        LEFT JOIN song_artists sa ON sa.artist_id = a.id
        LEFT JOIN songs s         ON s.id = sa.song_id
        LEFT JOIN video_songs vs  ON vs.song_id = s.id
        LEFT JOIN videos v        ON v.id = vs.video_id

        WHERE a.id = :artist_id
        ORDER BY s.title, v.title
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {"artist_id": artist_id}
        ).mappings().all()

    if not rows:
        return None

    artist = {
        "id": rows[0]["artist_id"],
        "name": rows[0]["artist_name"],
        "spotify_artist_id": rows[0]["spotify_artist_id"],
        "songs": {}
    }

    for row in rows:
        if row["song_id"] is None:
            continue

        song = artist["songs"].setdefault(
            row["song_id"],
            {
                "id": row["song_id"],
                "title": row["song_title"],
                "spotify_track_id": row["spotify_track_id"],
                "videos": []
            }
        )

        if row["video_id"]:
            song["videos"].append({
                "title": row["video_title"],
                "youtube_video_id": row["youtube_video_id"]
            })

    # Convert songs dict → list
    artist["songs"] = list(artist["songs"].values())

    return artist

def list_video_categories():
    sql = text("""
      SELECT slug, title, description
      FROM video_categories
      WHERE is_active = true
      ORDER BY sort_order, title
    """)
    with engine.connect() as conn:
        return conn.execute(sql).mappings().all()


def get_video_category_by_slug(slug: str):
    sql = text("""
      SELECT id, slug, title, description
      FROM video_categories
      WHERE slug = :slug AND is_active = true
      LIMIT 1
    """)
    with engine.connect() as conn:
        return conn.execute(sql, {"slug": slug}).mappings().first()

def list_videos_for_category(category_id: int, q: str | None = None):
    sql = text("""
        SELECT
          v.id,
          v.title,
          v.published_at,
          COUNT(DISTINCT vs.song_id) AS song_count
        FROM video_category_videos vcv
        JOIN videos v ON v.id = vcv.video_id
        LEFT JOIN video_songs vs ON vs.video_id = v.id
        WHERE vcv.category_id = :category_id
          AND (
            CAST(:q AS text) IS NULL
            OR v.title ILIKE '%' || :q || '%'
          )
        GROUP BY v.id, vcv.rank
        ORDER BY
          vcv.rank NULLS LAST,
          v.published_at DESC NULLS LAST,
          v.id DESC
    """)

    params = {
        "category_id": category_id,
        "q": q.strip() if q and q.strip() else None
    }

    with engine.connect() as conn:
        return conn.execute(sql, params).mappings().all()
