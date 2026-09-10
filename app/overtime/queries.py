from sqlalchemy import text
from app.db import engine

def get_overtime_stats():
    sql = text("""
        SELECT
            (SELECT COUNT(*) FROM overtime_episodes) AS episode_count,
            (SELECT COUNT(*) FROM overtime_segments) AS segment_count,
            (SELECT COUNT(*) FROM overtime_cool_not_cool_items) AS item_count,
            (SELECT COUNT(*) FROM overtime_segment_types) AS segment_type_count;
    """)
    with engine.connect() as conn:
        return conn.execute(sql).mappings().one()


def search_overtime_items(q: str):
    sql = text("""
        SELECT
            osi.id AS item_id,
            osi.item_name,

            p.id AS presenter_id,
            p.name AS presenter_name,

            os.id AS segment_id,
            os.segment_order,
            os.title AS segment_title,

            ost.id AS segment_type_id,
            ost.name AS segment_type_name,
            ost.canonical_name,

            oe.id AS episode_id,
            oe.episode_number,

            v.id AS video_id,
            v.title AS video_title,
            v.youtube_video_id
        FROM overtime_cool_not_cool_items osi
        JOIN overtime_segments os
            ON os.id = osi.segment_id
        JOIN overtime_segment_types ost
            ON ost.id = os.segment_type_id
        JOIN overtime_episodes oe
            ON oe.id = os.episode_id
        JOIN videos v
            ON v.id = oe.video_id
        LEFT JOIN players p
            ON p.id = osi.presenter_id
        WHERE osi.item_name ILIKE :q
        ORDER BY
            oe.episode_number,
            os.segment_order,
            osi.id
    """)
    with engine.connect() as conn:
        return conn.execute(
            sql,
            {"q": f"%{q}%"}
        ).mappings().all()

def get_overtime_episodes():
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    oe.id,
                    oe.video_id,
                    oe.episode_number,
                    oe.notes,
                    v.title,
                    v.youtube_video_id,
                    v.published_at,
                    COUNT(os.id) AS segment_count
                FROM overtime_episodes oe
                JOIN videos v
                    ON v.id = oe.video_id
                LEFT JOIN overtime_segments os
                    ON os.episode_id = oe.id
                GROUP BY
                    oe.id,
                    oe.video_id,
                    oe.episode_number,
                    oe.notes,
                    v.title,
                    v.youtube_video_id,
                    v.published_at
                ORDER BY oe.episode_number DESC;
            """)
        ).mappings().all()

        return [dict(row) for row in rows]

def get_overtime_episode(episode_id: int):
    with engine.connect() as conn:
        episode = conn.execute(
            text("""
                SELECT
                    oe.id,
                    oe.episode_number,
                    oe.notes,
                    oe.video_id,
                    v.title AS video_title,
                    v.youtube_video_id,
                    v.published_at
                FROM overtime_episodes oe
                JOIN videos v
                    ON v.id = oe.video_id
                WHERE oe.id = :episode_id
                LIMIT 1
            """),
            {"episode_id": episode_id},
        ).mappings().first()

        if not episode:
            return None

        segments = conn.execute(
            text("""
                SELECT
                    os.id,
                    os.segment_order,
                    os.title,
                    os.notes,
                    ost.id AS segment_type_id,
                    ost.name AS segment_type_name,
                    ost.canonical_name
                FROM overtime_segments os
                JOIN overtime_segment_types ost
                    ON ost.id = os.segment_type_id
                WHERE os.episode_id = :episode_id
                ORDER BY os.segment_order
            """),
            {"episode_id": episode_id},
        ).mappings().all()

        result = dict(episode)
        result["segments"] = [dict(row) for row in segments]

        return result

def get_overtime_segment_types():
    sql = text("""
        SELECT
            canonical.id,
            canonical.name,
            canonical.slug,
            COUNT(os.id) AS segment_count,
            media.media_url AS image_url,
            media.alt_text AS image_alt
        FROM overtime_segment_types canonical
        LEFT JOIN overtime_segment_types actual
            ON actual.id = canonical.id
            OR actual.variant_of = canonical.id
        LEFT JOIN overtime_segments os
            ON os.segment_type_id = actual.id
        LEFT JOIN overtime_segment_type_media media
            ON media.segment_type_id = canonical.id
            AND media.media_type = 'image'
        WHERE canonical.variant_of IS NULL
        GROUP BY
            canonical.id,
            canonical.name,
            canonical.slug,
            media.media_url,
            media.alt_text
        ORDER BY canonical.name
    """)

    with engine.connect() as conn:
        return conn.execute(sql).mappings().all()

def get_overtime_segment_type(slug: str):
    sql = text("""
        SELECT
            id,
            name,
            slug,
            mode,
            canonical_name
        FROM overtime_segment_types
        WHERE slug = :slug
          AND variant_of IS NULL
    """)

    with engine.connect() as conn:
        return conn.execute(
            sql,
            {"slug": slug},
        ).mappings().first()

def get_overtime_segment_type_appearances(segment_type_id: int):
    sql = text("""
        SELECT
            os.id AS segment_id,
            os.segment_order,
            os.title,
            os.notes,

            actual.id AS segment_type_id,
            actual.name AS segment_type_name,
            actual.slug AS segment_type_slug,

            oe.id AS episode_id,
            oe.episode_number,

            v.id AS video_id,
            v.title AS video_title,
            v.youtube_video_id,
            v.published_at

        FROM overtime_segments os

        JOIN overtime_segment_types actual
            ON actual.id = os.segment_type_id

        JOIN overtime_episodes oe
            ON oe.id = os.episode_id

        JOIN videos v
            ON v.id = oe.video_id

        WHERE
            actual.id = :segment_type_id
            OR actual.variant_of = :segment_type_id

        ORDER BY
            oe.episode_number,
            os.segment_order
    """)

    with engine.connect() as conn:
        return conn.execute(
            sql,
            {"segment_type_id": segment_type_id},
        ).mappings().all()

