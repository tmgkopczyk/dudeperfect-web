from sqlalchemy import text
from app.db import engine

def get_battle_stats():
    sql = text("""
        SELECT
            (SELECT COUNT(*) FROM overtime_episodes) AS episode_count,
            (SELECT COUNT(*) FROM overtime_segments) AS segment_count,
            (SELECT COUNT(*) FROM overtime_segment_items) AS item_count,
            (SELECT COUNT(*) FROM overtime_segment_types) AS segment_type_count;
    """)
    with engine.connect() as conn:
        return conn.execute(sql).mappings().one()