from collections import defaultdict, Counter
from sqlalchemy import text
from .db import engine

def get_battle_view(video_id: int):
    with engine.connect() as conn:

        # =========================
        # 1️⃣ Load battle + video
        # =========================
        battle_row = conn.execute(
            text("""
                SELECT
                    b.id AS battle_id,
                    b.description,
                    b.rules,
                    b.notes,
                    b.winner,
                    v.id AS video_id,
                    v.title
                FROM battles b
                JOIN videos v ON v.id = b.video_id
                WHERE v.id = :video_id
                LIMIT 1
            """),
            {"video_id": video_id}
        ).mappings().fetchone()

        if not battle_row:
            return None

        # =========================
        # 2️⃣ Players
        # =========================
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

        # =========================
        # 3️⃣ Rounds
        # =========================
        rounds = conn.execute(
            text("""
                SELECT
                    br.id,
                    br.round_order,
                    br.name,
                    br.score_label
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
                            COALESCE(bp.name, bt.name) AS name,
                            brp.status,
                            brp.placement,
                            brp.score,
                            brp.notes
                        FROM battle_round_participants brp
                        LEFT JOIN battle_players bp
                            ON brp.battle_player_id = bp.id
                        LEFT JOIN battle_teams bt
                            ON brp.battle_team_id = bt.id
                        WHERE brp.battle_round_id = :round_id
                        ORDER BY brp.placement NULLS LAST,
                                COALESCE(bp.name, bt.name)
                    """),
                    {"round_id": r["id"]}
                ).mappings().all()


            timeline.append({
                "name": r["name"],
                "score_label": r["score_label"],
                "results": [dict(x) for x in results]
            })

    # =========================
    # 4️⃣ Shape data for template
    # =========================
    return {
        "id": battle_row["battle_id"],
        "title": battle_row["title"],
        "winner": battle_row["winner"],
        "format": "standard",
        "description": battle_row["description"],
        "rules": battle_row["rules"],
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


def get_overtime_view(video_id: int):
    with engine.connect() as conn:

        # 1️⃣ Find episode
        episode = conn.execute(
            text("""
                SELECT id
                FROM overtime_episodes
                WHERE video_id = :video_id
                LIMIT 1
            """),
            {"video_id": video_id}
        ).mappings().first()

        if not episode:
            return None

        # 2️⃣ Get segments for this episode
        segments = conn.execute(
            text("""
                SELECT
                    os.id,
                    st.name,
                    st.canonical_name
                FROM overtime_segments os
                JOIN overtime_segment_types st
                ON st.id = os.segment_type_id
                WHERE os.episode_id = :episode_id
                ORDER BY os.segment_order NULLS LAST, os.id
            """),
            {"episode_id": episode["id"]}
        ).mappings().all()


        if not segments:
            return None

        formatted_segments = []

        for segment in segments:

            segment_id = segment["id"]
            raw_type = segment["name"]
            canonical_type = segment["canonical_name"] or raw_type

            # =========================
            # 🎬 COOL NOT COOL
            # =========================
            if canonical_type in ("Cool Not Cool", "Not Cool Cool"):

                items = conn.execute(
                    text("""
                        SELECT
                            i.id,
                            i.item_name,
                            p.name AS presenter_name
                        FROM overtime_segment_items i
                        LEFT JOIN players p
                          ON p.id = i.presenter_id
                        WHERE i.segment_id = :segment_id
                        ORDER BY i.id
                    """),
                    {"segment_id": segment_id}
                ).mappings().all()

                formatted_items = []

                for item in items:
                    votes = conn.execute(
                        text("""
                            SELECT
                                pl.name AS voter_name,
                                v.vote
                            FROM overtime_segment_item_votes v
                            JOIN players pl
                              ON pl.id = v.voter_id
                            WHERE v.item_id = :item_id
                            ORDER BY pl.name
                        """),
                        {"item_id": item["id"]}
                    ).mappings().all()

                    vote_values = [v["vote"] for v in votes]
                    cool_count = vote_values.count("cool")
                    not_cool_count = vote_values.count("not_cool")

                    overall = None
                    if cool_count > not_cool_count:
                        overall = "cool"
                    elif not_cool_count > cool_count:
                        overall = "not_cool"
                    elif cool_count == not_cool_count and cool_count > 0:
                        overall = "both"

                    formatted_items.append({
                        "item_name": item["item_name"],
                        "presenter_name": item["presenter_name"],
                        "votes": [dict(v) for v in votes],
                        "overall": overall
                    })

                formatted_segments.append({
                    "segment_type": canonical_type,
                    "display_name": raw_type,
                    "items": formatted_items
                })


            # =========================
            # 🎡 WHEEL SEGMENT
            # =========================
            elif canonical_type in ("Wheel Unfortunate", "Wheel Fortunate"):

                event = conn.execute(
                    text("""
                        SELECT
                            sp.name AS selected_player,
                            hp.name AS host_name,
                            w.mechanism,
                            w.outcome_type,
                            w.outcome_text
                        FROM overtime_wheel_events w
                        LEFT JOIN players sp
                          ON sp.id = w.selected_player_id
                        LEFT JOIN players hp
                          ON hp.id = w.host_id
                        WHERE w.segment_id = :segment_id
                    """),
                    {"segment_id": segment_id}
                ).mappings().first()

                formatted_segments.append({
                    "segment_type": raw_type,
                    "event": dict(event) if event else None
                })

            # =========================
            # 🎯 BETCHA
            # =========================
            elif canonical_type == "Betcha":

                event = conn.execute(
                    text("""
                        SELECT
                            p.name AS presenter_name,
                            b.bet_description
                        FROM overtime_betcha_events b
                        JOIN players p
                          ON p.id = b.presenter_id
                        WHERE b.segment_id = :segment_id
                    """),
                    {"segment_id": segment_id}
                ).mappings().first()

                votes = conn.execute(
                    text("""
                        SELECT
                            pl.name AS voter_name,
                            v.vote
                        FROM overtime_betcha_votes v
                        JOIN players pl
                          ON pl.id = v.voter_id
                        WHERE v.segment_id = :segment_id
                        ORDER BY pl.name
                    """),
                    {"segment_id": segment_id}
                ).mappings().all()

                formatted_segments.append({
                    "segment_type": raw_type,
                    "event": dict(event) if event else None,
                    "votes": [dict(v) for v in votes]
                })

            # =========================
            # 🎨 GET CRAFTY
            # =========================
            elif canonical_type == "Get Crafty":

                entries = conn.execute(
                    text("""
                        SELECT
                            p.name,
                            e.entry_description,
                            e.placement,
                            e.is_winner
                        FROM overtime_get_crafty_entries e
                        JOIN players p
                          ON p.id = e.player_id
                        WHERE e.segment_id = :segment_id
                        ORDER BY e.placement NULLS LAST
                    """),
                    {"segment_id": segment_id}
                ).mappings().all()

                formatted_segments.append({
                    "segment_type": raw_type,
                    "entries": [dict(e) for e in entries]
                })
            
            # =========================
            # 🎮 DEFAULT / PARTICIPANT SEGMENTS
            # =========================
            elif canonical_type == "Game Time":
                event = conn.execute(
                    text("""
                        SELECT
                            e.id,
                            e.game_description,
                            e.score_label,
                            e.win_condition,
                            p.name AS winner_name
                        FROM overtime_game_time_events e
                        LEFT JOIN players p
                        ON p.id = e.winner_player_id
                        WHERE e.segment_id = :segment_id
                        LIMIT 1
                    """),
                    {"segment_id": segment_id}
                ).mappings().first()


                results = []

                if event is not None:
                    results = conn.execute(
                        text("""
                            SELECT
                                p.name,
                                r.score_display,
                                r.is_winner
                            FROM overtime_game_time_results r
                            JOIN players p ON p.id = r.player_id
                            WHERE r.event_id = :event_id
                            ORDER BY r.score_numeric DESC NULLS LAST
                        """),
                        {"event_id": event["id"]}
                    ).mappings().all()

                formatted_segments.append({
                    "segment_type": raw_type,
                    "event": dict(event) if event else None,
                    "results": [dict(r) for r in results]
                })
            elif canonical_type == "Absurd Recurds":
                record = conn.execute(
                    text("""
                        SELECT
                            ar.record_description,
                            p.name AS player_name,
                            ar.outcome,
                            ar.notes
                        FROM overtime_absurd_recurds ar
                        LEFT JOIN players p
                        ON p.id = ar.player_id
                        WHERE ar.segment_id = :segment_id
                        LIMIT 1
                    """),
                    {"segment_id": segment_id}
                ).mappings().first()

                formatted_segments.append({
                "segment_type": raw_type,
                "record": dict(record) if record else None
                })
                            
            elif canonical_type == "Judge Dudy":

                case = conn.execute(
                    text("""
                        SELECT
                            c.id,
                            c.case_title,
                            c.case_description,
                            c.verdict
                        FROM overtime_judge_dudy_cases c
                        WHERE c.segment_id = :segment_id
                        LIMIT 1
                    """),
                    {"segment_id": segment_id}
                ).mappings().first()

                participants = []

                if case:
                    participants = conn.execute(
                        text("""
                            SELECT
                                p.name,
                                j.role
                            FROM overtime_judge_dudy_participants j
                            JOIN players p
                              ON p.id = j.player_id
                            WHERE j.case_id = :case_id
                        """),
                        {"case_id": case["id"]}
                    ).mappings().all()

                # Convert participants into role dictionary
                role_map = {}
                for p in participants:
                    role_map[p["role"]] = p["name"]

                formatted_segments.append({
                    "segment_type": raw_type,
                    "case": {
                        "title": case["case_title"] if case else None,
                        "description": case["case_description"] if case else None,
                        "verdict": case["verdict"] if case else None,
                        "participants": role_map
                    } if case else None
                })
            elif canonical_type in ("Top 10", "Not Top 10", "Top 15"):
                entries = conn.execute(
                    text("""
                        SELECT rank, selection, notes
                        FROM overtime_ranked_list_entries
                        WHERE segment_id = :segment_id
                        ORDER BY rank ASC
                    """),
                    {"segment_id": segment_id}
                ).mappings().all()

                formatted_segments.append({
                    "segment_type": raw_type,
                    "entries": [dict(e) for e in entries]
                })

            else:
                formatted_segments.append({
                    "segment_type": raw_type,
                    "data": None
                })


        return {
            "segments": formatted_segments
        }

def get_bucket_list_view(video_id: int):
    with engine.connect() as conn:

        # 1️⃣ Find episode
        episode = conn.execute(
            text("""
                SELECT id, episode_number
                FROM bucket_list_episodes
                WHERE video_id = :video_id
                LIMIT 1
            """),
            {"video_id": video_id}
        ).mappings().first()

        if not episode:
            return None

        # 2️⃣ Get tasks
        tasks = conn.execute(
            text("""
                SELECT
                    task_order,
                    task_text,
                    completed,
                    completion_note
                FROM bucket_list_tasks
                WHERE episode_id = :episode_id
                ORDER BY task_order
            """),
            {"episode_id": episode["id"]}
        ).mappings().all()

        if not tasks:
            return None

        return {
            "episode_number": episode["episode_number"],
            "tasks": [dict(t) for t in tasks]
        }

def get_stereotypes_view(video_id: int):
    with engine.connect() as conn:

        # 1️⃣ Find episode
        episode = conn.execute(
            text("""
                SELECT id, episode_number, theme
                FROM stereotypes_episodes
                WHERE video_id = :video_id
                LIMIT 1
            """),
            {"video_id": video_id}
        ).mappings().first()

        if not episode:
            return None

        # 2️⃣ Get segments (no performer join here anymore)
        segments = conn.execute(
            text("""
                SELECT
                    s.id,
                    s.segment_order,
                    s.name,
                    s.timestamp_seconds,
                    s.notes,
                    r.name AS recurring_name
                FROM stereotype_segments s
                LEFT JOIN recurring_stereotypes r
                    ON r.id = s.recurring_id
                WHERE s.episode_id = :episode_id
                ORDER BY s.segment_order
            """),
            {"episode_id": episode["id"]}
        ).mappings().all()

        formatted_segments = []

        for seg in segments:

            performers = conn.execute(
                text("""
                    SELECT p.name
                    FROM stereotype_segment_performers ssp
                    JOIN players p
                      ON p.id = ssp.player_id
                    WHERE ssp.segment_id = :segment_id
                    ORDER BY p.name
                """),
                {"segment_id": seg["id"]}
            ).mappings().all()

            formatted_segments.append({
                "segment_order": seg["segment_order"],
                "name": seg["name"],
                "timestamp_seconds": seg["timestamp_seconds"],
                "notes": seg["notes"],
                "recurring_name": seg["recurring_name"],
                "performers": [p["name"] for p in performers]
            })

        return {
            "episode_number": episode["episode_number"],
            "theme": episode["theme"],
            "segments": formatted_segments
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
