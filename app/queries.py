from collections import defaultdict, Counter
from sqlalchemy import text
from db import engine

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
                    v.id AS video_id,
                    v.title
                FROM battles b
                JOIN videos v
                    ON v.id = b.video_id
                WHERE v.id = :video_id
                LIMIT 1
            """),
            {"video_id": video_id}
        ).mappings().fetchone()

        if not battle_row:
            return None

        # =========================
        # 2️⃣ Teams / Players
        # =========================

        team_rows = conn.execute(
            text("""
                SELECT
                    id,
                    name,
                    accent_color
                FROM battle_teams
                WHERE battle_id = :battle_id
                ORDER BY id;
            """),
            {"battle_id": battle_row["battle_id"]}
        ).mappings().all()

        teams = []

        # -------------------------
        # Team battle
        # -------------------------
        if team_rows:

            for team in team_rows:

                members = conn.execute(
                    text("""
                        SELECT
                            p.id AS player_id,
                            p.name,
                            p.slug,
                            bp.is_guest,
                            bp.notes
                        FROM battle_team_members btm
                        JOIN battle_players bp
                            ON bp.id = btm.battle_player_id
                        JOIN players p
                            ON p.id = bp.player_id
                        WHERE btm.team_id = :team_id
                        ORDER BY p.name
                    """),
                    {"team_id": team["id"]}
                ).mappings().all()

                teams.append({
                    "name": team["name"],
                    "accent_color": team["accent_color"],
                    "players": [dict(x) for x in members]
                })

        # -------------------------
        # Individual battle
        # -------------------------
        else:

            players = conn.execute(
                text("""
                    SELECT
                        p.id AS player_id,
                        p.name,
                        p.slug,
                        bp.is_guest,
                        bp.accent_color,
                        bp.notes
                    FROM battle_players bp
                    JOIN players p
                        ON p.id = bp.player_id
                    WHERE bp.battle_id = :battle_id
                    ORDER BY p.name
                """),
                {"battle_id": battle_row["battle_id"]}
            ).mappings().all()

            teams = [{
                "name": "Players",
                "players": [dict(x) for x in players]
            }]

        # =========================
        # 3️⃣ Rounds
        # =========================

        rounds = conn.execute(
            text("""
                SELECT
                    br.id,
                    br.round_order,
                    br.name,
                    br.score_label,
                    br.round_type
                FROM battle_rounds br
                WHERE br.battle_id = :battle_id
                ORDER BY br.round_order
            """),
            {"battle_id": battle_row["battle_id"]}
        ).mappings().all()

        timeline = []

        for r in rounds:

            # =====================
            # Overall round results
            # =====================

            results = conn.execute(
                text("""
                    SELECT
                        p.id AS player_id,
                        p.slug AS player_slug,
                        bt.id AS team_id,
                        COALESCE(p.name, bt.name) AS name,
                        brp.status,
                        brp.placement,
                        brp.score,
                        brp.notes
                    FROM battle_round_participants brp
                    LEFT JOIN battle_players bp
                        ON brp.battle_player_id = bp.id
                    LEFT JOIN players p
                        ON p.id = bp.player_id
                    LEFT JOIN battle_teams bt
                        ON bt.id = brp.battle_team_id
                    WHERE brp.battle_round_id = :round_id
                    ORDER BY
                        brp.placement NULLS LAST,
                        COALESCE(p.name, bt.name)
                """),
                {"round_id": r["id"]}
            ).mappings().all()

            # =====================
            # Matches
            # =====================

            matches = []

            if r["round_type"] in ("round_robin", "elimination", "tournament"):

                match_rows = conn.execute(
                    text("""
                        SELECT
                            id,
                            match_order,
                            title
                        FROM battle_round_matches
                        WHERE battle_round_id = :round_id
                        ORDER BY match_order
                    """),
                    {"round_id": r["id"]}
                ).mappings().all()

                for match in match_rows:

                    participants = conn.execute(
                        text("""
                            SELECT
                                p.id AS player_id,
                                p.slug AS player_slug,
                                bt.id AS team_id,
                                COALESCE(p.name, bt.name) AS name,
                                brmp.placement,
                                brmp.score,
                                brmp.status,
                                brmp.notes
                            FROM battle_round_match_participants brmp

                            LEFT JOIN battle_players bp
                                ON bp.id = brmp.battle_player_id

                            LEFT JOIN players p
                                ON p.id = bp.player_id

                            LEFT JOIN battle_teams bt
                                ON bt.id = brmp.battle_team_id

                            WHERE brmp.battle_round_match_id = :match_id

                            ORDER BY
                                brmp.placement NULLS LAST,
                                COALESCE(p.name, bt.name)
                        """),
                        {"match_id": match["id"]}
                    ).mappings().all()

                    matches.append({
                        "id": match["id"],
                        "match_order": match["match_order"],
                        "title": match["title"],
                        "participants": [
                            dict(x) for x in participants
                        ]
                    })

            # =====================
            # Add round to timeline
            # =====================

            timeline.append({
                "id": r["id"],
                "round_order": r["round_order"],
                "name": r["name"],
                "round_type": r["round_type"],
                "score_label": r["score_label"],
                "results": [dict(x) for x in results],
                "matches": matches
            })

        # =========================
        # 4️⃣ Final standings
        # =========================

        final_standings = conn.execute(
            text("""
                SELECT
                    br.player_id,
                    br.battle_team_id,
                    COALESCE(p.name, bt.name) AS name,
                    br.status,
                    br.placement,
                    br.score,
                    br.notes
                FROM battle_results br
                LEFT JOIN players p
                    ON p.id = br.player_id
                LEFT JOIN battle_teams bt
                    ON bt.id = br.battle_team_id
                WHERE br.battle_id = :battle_id
                ORDER BY
                    br.placement NULLS LAST,
                    COALESCE(p.name, bt.name)
            """),
            {"battle_id": battle_row["battle_id"]}
        ).mappings().all()

        winner_parts = []

        for result in final_standings:

            if result["placement"] != 1:
                continue

            # Individual winner
            if result["player_id"] is not None:
                winner_parts.append(result["name"])

            # Team winner
            elif result["battle_team_id"] is not None:

                members = conn.execute(
                    text("""
                        SELECT p.name
                        FROM battle_team_members btm
                        JOIN battle_players bp
                            ON bp.id = btm.battle_player_id
                        JOIN players p
                            ON p.id = bp.player_id
                        WHERE btm.team_id = :team_id
                        ORDER BY btm.id
                    """),
                    {"team_id": result["battle_team_id"]}
                ).scalars().all()

                team_name = result["name"]

                if members:
                    team_name += f" ({', '.join(members)})"

                winner_parts.append(team_name)

        winner = ", ".join(winner_parts) if winner_parts else None

    # =========================
    # 5️⃣ Shape data for template
    # =========================

    return {
        "id": battle_row["battle_id"],
        "title": battle_row["title"],
        "winner": winner,
        "format": "standard",
        "description": battle_row["description"],
        "rules": battle_row["rules"],
        "notes": battle_row["notes"],
        "teams": teams,
        "timeline": timeline,
        "final_standings": [dict(x) for x in final_standings]
    }

def get_battles():
    sql = text("""
        SELECT
            b.id AS battle_id,
            b.video_id,
            b.winner,
            b.description,
            v.title,
            v.published_at,
            v.youtube_video_id
        FROM battles b
        JOIN videos v
          ON v.id = b.video_id
        ORDER BY v.published_at DESC, b.id DESC
    """)

    with engine.connect() as conn:
        return [
            dict(row)
            for row in conn.execute(sql).mappings()
        ]

def get_battle_video_id(battle_id: int):
    sql = text("""
        SELECT video_id
        FROM battles
        WHERE id = :battle_id
    """)

    with engine.connect() as conn:
        return conn.execute(
            sql,
            {"battle_id": battle_id},
        ).scalar_one_or_none()

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
                    os.title,
                    os.notes,
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
                    total_votes = len(vote_values)

                    overall = None

                    if total_votes > 0 and cool_count == total_votes:
                        overall = "super_cool"
                    elif total_votes > 0 and not_cool_count == total_votes:
                        overall = "super_not_cool"
                    elif cool_count > not_cool_count:
                        overall = "cool"
                    elif not_cool_count > cool_count:
                        overall = "not_cool"
                    elif cool_count == not_cool_count:
                        overall = "tie"


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

                events = conn.execute(
                text("""
                    SELECT
                        w.id,
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
                    ORDER BY w.id
                """),
                {"segment_id": segment_id}
                ).mappings().all()

                formatted_segments.append({
                    "segment_type": raw_type,
                    "events": [dict(e) for e in events]
                })

            # =========================
            # 🎯 BETCHA
            # =========================
            elif canonical_type == "Betcha":

                event = conn.execute(
                    text("""
                        SELECT
                            p.name AS presenter_name,
                            b.bet_description,
                            b.outcome
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

                event = conn.execute(
                    text("""
                        SELECT
                            challenge_name,
                            description,
                            winner_id,
                            notes
                        FROM overtime_get_crafty_events
                        WHERE segment_id = :segment_id
                    """),
                    {"segment_id": segment_id}
                ).mappings().first()

                participants = conn.execute(
                    text("""
                        SELECT
                            p.name AS player_name,
                            gp.placement,
                            gp.notes
                        FROM overtime_get_crafty_participants gp
                        JOIN players p
                            ON p.id = gp.player_id
                        JOIN overtime_get_crafty_events ge
                            ON ge.id = gp.event_id
                        WHERE ge.segment_id = :segment_id
                        ORDER BY
                            gp.placement NULLS LAST,
                            p.name
                    """),
                    {"segment_id": segment_id}
                ).mappings().all()

                formatted_segments.append({
                    "segment_type": raw_type,
                    "event": dict(event) if event else None,
                    "participants": [dict(p) for p in participants]
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
                
                role_map = defaultdict(list)

                for p in participants:
                    role_map[p["role"]].append(p["name"])

                role_map = dict(role_map)

                
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
                event = conn.execute(
                    text("""
                        SELECT
                            tle.id,
                            tle.title,
                            p.name AS presenter_name
                        FROM overtime_top_list_events tle
                        LEFT JOIN players p
                            ON p.id = tle.presenter_id
                        WHERE tle.segment_id = :segment_id
                    """),
                    {"segment_id": segment_id}
                ).mappings().first()

                entries = []

                if event:
                    entries = conn.execute(
                        text("""
                            SELECT
                                i.id,
                                i.rank,
                                i.rank_display,
                                i.item_text,
                                i.item_type,
                                i.reveal_order,
                                m.media_type,
                                m.media_url,
                                m.alt_text
                            FROM overtime_top_list_items i
                            LEFT JOIN overtime_top_list_item_media m
                                ON m.item_id = i.id
                            WHERE i.event_id = :event_id
                            ORDER BY
                                CASE
                                    WHEN i.item_type = 'ranked' THEN 0
                                    ELSE 1
                                END,
                                i.rank DESC,
                                i.reveal_order ASC NULLS LAST;
                        """),
                        {"event_id": event["id"]}
                    ).mappings().all()

                formatted_segments.append({
                    "segment_type": raw_type,
                    "event": dict(event) if event else None,
                    "entries": [
                        dict(e) for e in entries
                        if e["item_type"] == "ranked"
                    ],
                    "honorable_mentions": [
                        dict(e) for e in entries
                        if e["item_type"] == "honorable_mention"
                    ]
                })
            elif canonical_type == "Taste Test":

                event = conn.execute(
                    text("""
                        SELECT
                            e.id,
                            e.food_item,
                            p.name AS participant_name
                        FROM overtime_taste_test_events e
                        LEFT JOIN players p
                            ON p.id = e.participant_id
                        WHERE e.segment_id = :segment_id
                    """),
                    {"segment_id": segment_id}
                ).mappings().first()
                samples = []
                rankings = []

                if event:

                    samples = conn.execute(
                        text("""
                            SELECT
                                sample_label,
                                actual_item,
                                guessed_item,
                                LOWER(actual_item) = LOWER(guessed_item) AS guess_correct
                            FROM overtime_taste_test_samples
                            WHERE event_id = :event_id
                            ORDER BY sample_label
                        """),
                        {"event_id": event["id"]}
                    ).mappings().all()

                    rankings = conn.execute(
                        text("""
                            SELECT
                                r.placement,
                                s.sample_label,
                                s.actual_item,
                                s.guessed_item,
                                LOWER(s.actual_item) = LOWER(s.guessed_item) AS guess_correct
                            FROM overtime_taste_test_rankings r
                            JOIN overtime_taste_test_samples s
                                ON s.id = r.sample_id
                            WHERE s.event_id = :event_id
                            ORDER BY r.placement
                        """),
                        {"event_id": event["id"]}
                    ).mappings().all()
                formatted_segments.append({
                    "segment_type": raw_type,
                    "event": dict(event) if event else None,
                    "rankings": [dict(r) for r in rankings],
                    "samples": [dict(s) for s in samples]
                })
            elif canonical_type == "Wives vs Chad":
                event = conn.execute(
                    text("""
                        SELECT
                            id,
                            theme,
                            winner,
                            notes
                        FROM overtime_wives_vs_chad_events
                        WHERE segment_id = :segment_id
                        LIMIT 1
                    """),
                    {"segment_id": segment_id}
                ).mappings().first()

                questions = []

                if event:
                    questions = conn.execute(
                        text("""
                            SELECT
                                question_order,
                                round_name,
                                question_text,
                                wives_answer,
                                chad_answer,
                                correct_answer,
                                wives_correct,
                                chad_correct,
                                notes
                            FROM overtime_wives_vs_chad_questions
                            WHERE event_id = :event_id
                            ORDER BY question_order
                        """),
                        {"event_id": event["id"]}
                    ).mappings().all()

                formatted_segments.append({
                    "segment_type": raw_type,
                    "event": dict(event) if event else None,
                    "questions": [dict(q) for q in questions]
                })
            elif canonical_type == "Culture Clash":
                print(segment)
                event = conn.execute(
                    text("""
                        SELECT
                            e.id,

                            c1.name AS team_a_country,
                            c1.flag_emoji AS team_a_flag,

                            c2.name AS team_b_country,
                            c2.flag_emoji AS team_b_flag,

                            e.notes
                        FROM culture_clash_events e
                        LEFT JOIN countries c1
                            ON c1.id = e.team_a_country_id
                        LEFT JOIN countries c2
                            ON c2.id = e.team_b_country_id
                        WHERE e.segment_id = :segment_id
                        LIMIT 1
                    """),
                    {
                        "segment_id": segment["id"]
                    }
                ).mappings().first()
                print(event)
                if event:

                    item_rows = conn.execute(
                        text("""
                            SELECT
                                i.id,
                                i.item_order,
                                i.food_name,
                                i.correct_name,

                                c.name AS country_name,
                                c.flag_emoji,

                                i.points,
                                i.notes
                            FROM culture_clash_items i
                            JOIN countries c
                                ON c.id = i.country_id
                            WHERE i.event_id = :event_id
                            ORDER BY i.item_order
                        """),
                        {
                            "event_id": event["id"]
                        }
                    ).mappings().all()

                    culture_clash_items = []

                    for item in item_rows:

                        guess_rows = conn.execute(
                            text("""
                                SELECT
                                    p.name AS player_name,
                                    g.guess_text,
                                    g.is_correct,
                                    g.notes
                                FROM culture_clash_guesses g
                                JOIN players p
                                    ON p.id = g.player_id
                                WHERE g.item_id = :item_id
                                ORDER BY p.name
                            """),
                            {
                                "item_id": item["id"]
                            }
                        ).mappings().all()

                        culture_clash_items.append({
                            "item_order": item["item_order"],
                            "food_name": item["food_name"],
                            "correct_name": item["correct_name"],
                            "country_name": item["country_name"],
                            "flag_emoji": item["flag_emoji"],
                            "guesses": [
                                dict(g)
                                for g in guess_rows
                            ]
                        })

                    formatted_segments.append({
                        "segment_type": raw_type,
                        "event": {
                            "team_a_country": event["team_a_country"],
                            "team_a_flag": event["team_a_flag"],

                            "team_b_country": event["team_b_country"],
                            "team_b_flag": event["team_b_flag"],

                            "notes": event["notes"]
                        },
                        "items": culture_clash_items
                    })
            elif canonical_type == "Commercial Clash":
                event = conn.execute(
                    text("""
                        SELECT
                            id,
                            sponsor_name,
                            notes
                        FROM overtime_commercial_clash_events
                        WHERE segment_id = :segment_id
                        LIMIT 1
                    """),
                    {"segment_id": segment_id}
                ).mappings().first()

                teams = []
                requirements = []

                if event:

                    # Challenge requirements
                    requirements = conn.execute(
                        text("""
                            SELECT
                                requirement_order,
                                requirement_text
                            FROM overtime_commercial_clash_requirements
                            WHERE event_id = :event_id
                            ORDER BY requirement_order
                        """),
                        {"event_id": event["id"]}
                    ).mappings().all()

                    # Teams
                    team_rows = conn.execute(
                        text("""
                            SELECT
                                id,
                                team_number,
                                commercial_theme,
                                commercial_title,
                                commercial_summary,
                                is_winner,
                                notes
                            FROM overtime_commercial_clash_teams
                            WHERE event_id = :event_id
                            ORDER BY team_number
                        """),
                        {"event_id": event["id"]}
                    ).mappings().all()

                    for team in team_rows:

                        members = conn.execute(
                            text("""
                                SELECT
                                    p.name AS player_name
                                FROM overtime_commercial_clash_team_members tm
                                JOIN players p
                                    ON p.id = tm.player_id
                                WHERE tm.team_id = :team_id
                                ORDER BY p.name
                            """),
                            {"team_id": team["id"]}
                        ).mappings().all()

                        teams.append({
                            "team_number": team["team_number"],
                            "commercial_theme": team["commercial_theme"],
                            "commercial_title": team["commercial_title"],
                            "commercial_summary": team["commercial_summary"],
                            "is_winner": team["is_winner"],
                            "notes": team["notes"],
                            "members": [dict(m) for m in members]
                        })
                formatted_segments.append({
                    "segment_type": raw_type,
                    "event": {
                        "sponsor_name": event["sponsor_name"],
                        "notes": event["notes"]
                    } if event else None,
                    "requirements": [dict(r) for r in requirements],
                    "teams": teams
                })
            else:
                formatted_segments.append({
                    "segment_type": raw_type,
                    "title": segment["title"],
                    "notes": segment["notes"],
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

def get_stereotypes_episodes():
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    se.id,
                    se.video_id,
                    se.episode_number,
                    se.theme,
                    v.title,
                    v.youtube_video_id,
                    v.published_at,
                    COUNT(ss.id) AS segment_count
                FROM stereotypes_episodes se
                JOIN videos v
                    ON v.id = se.video_id
                LEFT JOIN stereotype_segments ss
                    ON ss.episode_id = se.id
                GROUP BY
                    se.id,
                    se.video_id,
                    se.episode_number,
                    se.theme,
                    v.title,
                    v.youtube_video_id,
                    v.published_at
                ORDER BY se.episode_number DESC
            """)
        ).mappings().all()

        return [dict(row) for row in rows]

def get_recurring_stereotype(recurring_id: int):
    with engine.connect() as conn:

        recurring = conn.execute(
            text("""
                SELECT
                    id,
                    name
                FROM recurring_stereotypes
                WHERE id = :recurring_id
                LIMIT 1
            """),
            {"recurring_id": recurring_id}
        ).mappings().first()

        if not recurring:
            return None

        rows = conn.execute(
            text("""
                SELECT
                    ss.id AS segment_id,
                    ss.segment_order,
                    ss.name AS segment_name,
                    ss.timestamp_seconds,
                    ss.notes,

                    se.id AS episode_id,
                    se.episode_number,
                    se.theme,

                    v.id AS video_id,
                    v.title AS video_title,
                    v.youtube_video_id,
                    v.published_at,

                    p.id AS main_performer_id,
                    p.name AS main_performer

                FROM stereotype_segments ss

                JOIN stereotypes_episodes se
                    ON se.id = ss.episode_id

                JOIN videos v
                    ON v.id = se.video_id

                LEFT JOIN players p
                    ON p.id = ss.performer_id

                WHERE ss.recurring_id = :recurring_id

                ORDER BY
                    se.episode_number,
                    ss.segment_order
            """),
            {"recurring_id": recurring_id}
        ).mappings().all()

        appearances = []

        for row in rows:
            other_performers = conn.execute(
                text("""
                    SELECT
                        p.id,
                        p.name
                    FROM stereotype_segment_performers ssp
                    JOIN players p
                        ON p.id = ssp.player_id
                    WHERE ssp.segment_id = :segment_id
                    ORDER BY p.name
                """),
                {"segment_id": row["segment_id"]}
            ).mappings().all()

            appearance = dict(row)

            appearance["other_performers"] = [
                {
                    "id": performer["id"],
                    "name": performer["name"]
                }
                for performer in other_performers
            ]

            appearances.append(appearance)

        return {
            "id": recurring["id"],
            "name": recurring["name"],
            "appearance_count": len(appearances),
            "appearances": appearances
        }

def get_stereotype_performers():
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                WITH performer_appearances AS (

                    -- Main performers
                    SELECT
                        ss.id AS segment_id,
                        ss.episode_id,
                        ss.performer_id AS player_id
                    FROM stereotype_segments ss
                    WHERE ss.performer_id IS NOT NULL

                    UNION

                    -- Secondary performers
                    SELECT
                        ss.id AS segment_id,
                        ss.episode_id,
                        ssp.player_id
                    FROM stereotype_segment_performers ssp
                    JOIN stereotype_segments ss
                        ON ss.id = ssp.segment_id
                )

                SELECT
                    p.id,
                    p.name,
                    p.slug,
                    COUNT(DISTINCT pa.segment_id) AS stereotype_count,
                    COUNT(DISTINCT pa.episode_id) AS episode_count
                FROM performer_appearances pa

                JOIN players p
                    ON p.id = pa.player_id

                GROUP BY
                    p.id,
                    p.name,
                    p.slug

                ORDER BY
                    COUNT(DISTINCT pa.segment_id) DESC,
                    p.name
            """)
        ).mappings().all()

        return [dict(row) for row in rows]

def get_stereotype_performer_view(player_id: int):
    with engine.connect() as conn:

        performer = conn.execute(
            text("""
                SELECT
                    p.id,
                    p.name,
                    p.slug
                FROM players p
                WHERE p.id = :player_id
            """),
            {"player_id": player_id}
        ).mappings().first()

        if not performer:
            return None

        appearances = conn.execute(
            text("""
                WITH performer_appearances AS (

                    -- Main performer
                    SELECT
                        ss.id AS segment_id,
                        TRUE AS is_main_performer
                    FROM stereotype_segments ss
                    WHERE ss.performer_id = :player_id

                    UNION

                    -- Secondary performer
                    SELECT
                        ssp.segment_id,
                        FALSE AS is_main_performer
                    FROM stereotype_segment_performers ssp
                    WHERE ssp.player_id = :player_id
                )

                SELECT
                    ss.id AS segment_id,
                    ss.segment_order,
                    ss.name AS segment_name,
                    ss.notes,
                    ss.timestamp_seconds,

                    pa.is_main_performer,

                    se.id AS episode_id,
                    se.episode_number,
                    se.theme,
                    se.video_id,

                    v.title AS video_title,
                    v.youtube_video_id,
                    v.published_at,

                    rs.id AS recurring_id,
                    rs.name AS recurring_name

                FROM performer_appearances pa

                JOIN stereotype_segments ss
                    ON ss.id = pa.segment_id

                JOIN stereotypes_episodes se
                    ON se.id = ss.episode_id

                JOIN videos v
                    ON v.id = se.video_id

                LEFT JOIN recurring_stereotypes rs
                    ON rs.id = ss.recurring_id

                ORDER BY
                    se.episode_number DESC,
                    ss.segment_order
            """),
            {"player_id": player_id}
        ).mappings().all()

        if not appearances:
            return None

        episode_count = len({
            appearance["episode_id"]
            for appearance in appearances
        })

        return {
            "id": performer["id"],
            "name": performer["name"],
            "slug": performer["slug"],
            "appearance_count": len(appearances),
            "episode_count": episode_count,
            "appearances": [dict(row) for row in appearances]
        }

def get_recurring_stereotypes():
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT
                    rs.id,
                    rs.name,

                    COUNT(ss.id) AS appearance_count,

                    COUNT(DISTINCT se.id) AS episode_count,

                    ARRAY_AGG(
                        DISTINCT se.episode_number
                        ORDER BY se.episode_number
                    ) FILTER (
                        WHERE se.episode_number IS NOT NULL
                    ) AS episode_numbers

                FROM recurring_stereotypes rs

                LEFT JOIN stereotype_segments ss
                    ON ss.recurring_id = rs.id

                LEFT JOIN stereotypes_episodes se
                    ON se.id = ss.episode_id

                GROUP BY
                    rs.id,
                    rs.name

                ORDER BY
                    COUNT(ss.id) DESC,
                    rs.name
            """)
        ).mappings().all()

        return [dict(row) for row in rows]



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

        # 2️⃣ Get segments
        segments = conn.execute(
            text("""
                SELECT
                    s.id,
                    s.segment_order,
                    s.name,
                    s.timestamp_seconds,
                    s.notes,

                    p.id AS main_performer_id,
                    p.name AS main_performer,

                    r.id AS recurring_id,
                    r.name AS recurring_name

                FROM stereotype_segments s

                LEFT JOIN players p
                    ON p.id = s.performer_id

                LEFT JOIN recurring_stereotypes r
                    ON r.id = s.recurring_id

                WHERE s.episode_id = :episode_id
                ORDER BY s.segment_order
            """),
            {"episode_id": episode["id"]}
        ).mappings().all()

        formatted_segments = []

        for seg in segments:

            other_performers = conn.execute(
                text("""
                    SELECT
                        p.id,
                        p.name
                    FROM stereotype_segment_performers ssp
                    JOIN players p
                        ON p.id = ssp.player_id
                    WHERE ssp.segment_id = :segment_id
                    ORDER BY p.name
                """),
                {"segment_id": seg["id"]}
            ).mappings().all()

            formatted_segments.append({
                "id": seg["id"],
                "segment_order": seg["segment_order"],
                "name": seg["name"],
                "timestamp_seconds": seg["timestamp_seconds"],
                "notes": seg["notes"],

                "main_performer_id": seg["main_performer_id"],
                "main_performer": seg["main_performer"],

                "other_performers": [
                    {
                        "id": p["id"],
                        "name": p["name"]
                    }
                    for p in other_performers
                ],

                "recurring_id": seg["recurring_id"],
                "recurring_name": seg["recurring_name"]
            })

        return {
            "id": episode["id"],
            "episode_number": episode["episode_number"],
            "theme": episode["theme"],
            "segments": formatted_segments
        }
        
def get_song_detail(song_id: int):
    sql = text("""
    SELECT
        s.id               AS song_id,
        s.title            AS song_title,
        s.spotify_track_id AS spotify_track_id,
        s.source_type      AS source_type,
        s.source_url       AS source_url,
        s.notes            AS notes,

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
    "source_type": rows[0]["source_type"],
    "source_url": rows[0]["source_url"],
    "notes": rows[0]["notes"],
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


def get_all_artists():
    sql = text("""
        SELECT
            a.id,
            a.name,

            CASE
                WHEN LEFT(a.name, 1) ~* '^[A-Z]$'
                    THEN UPPER(LEFT(a.name, 1))
                ELSE '#'
            END AS letter_group,

            COUNT(DISTINCT sa.song_id) AS song_count

        FROM artists a

        LEFT JOIN song_artists sa
            ON sa.artist_id = a.id

        GROUP BY a.id, a.name

        ORDER BY
            CASE
                WHEN LEFT(a.name, 1) ~* '^[A-Z]$' THEN 1
                ELSE 0
            END,
            UPPER(a.name)
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()

    return [dict(row) for row in rows]

def get_artist_letters():
    sql = text("""
        WITH available_groups AS (
            SELECT DISTINCT
                CASE
                    WHEN LEFT(name, 1) ~* '[A-Z]'
                        THEN UPPER(LEFT(name, 1))
                    ELSE '#'
                END AS letter
            FROM artists
            WHERE name IS NOT NULL
              AND name <> ''
        ),
        letters AS (
            SELECT
                '#' AS letter,
                'number' AS anchor,
                0 AS sort_order

            UNION ALL

            SELECT
                chr(n) AS letter,
                chr(n) AS anchor,
                n - 64 AS sort_order
            FROM generate_series(65, 90) AS n
        )
        SELECT
            l.letter,
            l.anchor,
            (ag.letter IS NOT NULL) AS available
        FROM letters l
        LEFT JOIN available_groups ag
            ON ag.letter = l.letter
        ORDER BY l.sort_order
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()

    return [dict(row) for row in rows]

def get_all_songs():
    sql = text("""
        SELECT
            s.id,
            s.title,
            s.spotify_track_id,

            CASE
                WHEN LEFT(s.title, 1) ~* '[A-Z]'
                    THEN UPPER(LEFT(s.title, 1))
                ELSE '#'
            END AS letter_group,

            array_agg(
                a.name
                ORDER BY sa.artist_order
            ) AS artists

        FROM songs s

        JOIN song_artists sa
            ON sa.song_id = s.id

        JOIN artists a
            ON a.id = sa.artist_id

        GROUP BY s.id

        ORDER BY
            CASE
                WHEN LEFT(s.title, 1) ~* '[A-Z]'
                    THEN ASCII(UPPER(LEFT(s.title, 1)))
                ELSE 0
            END,
            s.title
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()

    return [
        {
            "id": row["id"],
            "title": row["title"],
            "spotify_track_id": row["spotify_track_id"],
            "letter_group": row["letter_group"],
            "artists": row["artists"],
        }
        for row in rows
    ]

def get_song_letters():
    sql = text("""
        WITH available_groups AS (
            SELECT DISTINCT
                CASE
                    WHEN LEFT(title, 1) ~* '[A-Z]'
                        THEN UPPER(LEFT(title, 1))
                    ELSE '#'
                END AS letter
            FROM songs
            WHERE title IS NOT NULL
              AND title <> ''
        ),
        letters AS (
            SELECT
                '#' AS letter,
                'number' AS anchor,
                0 AS sort_order

            UNION ALL

            SELECT
                chr(n) AS letter,
                chr(n) AS anchor,
                n - 64 AS sort_order
            FROM generate_series(65, 90) AS n
        )
        SELECT
            l.letter,
            l.anchor,
            (ag.letter IS NOT NULL) AS available
        FROM letters l
        LEFT JOIN available_groups ag
            ON ag.letter = l.letter
        ORDER BY l.sort_order
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()

    return [
        {
            "letter": row["letter"],
            "anchor": row["anchor"],
            "available": row["available"],
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

            a.name              AS artist_name,
            a.id                AS artist_id
            FROM videos v
            LEFT JOIN video_songs vs  ON vs.video_id = v.id
            LEFT JOIN songs s         ON s.id = vs.song_id
            LEFT JOIN song_artists sa ON sa.song_id = s.id
            LEFT JOIN artists a       ON a.id = sa.artist_id
            WHERE v.id = :video_id
            ORDER BY COALESCE(vs.song_order, s.id), sa.artist_order
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
            song["artists"].append({
                "id": row["artist_id"],
                "name": row["artist_name"],
            })

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

def search_videos(query: str, limit: int = 50):
    sql = text("""
        SELECT
            v.id,
            v.title,
            v.published_at,
            COUNT(vs.song_id) AS song_count
        FROM videos v
        LEFT JOIN video_songs vs
            ON vs.video_id = v.id
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

    return [dict(row) for row in rows]

def get_videos(limit: int = 50, offset: int = 0):
    sql = text("""
        SELECT
            v.id,
            v.title,
            v.published_at,
            COUNT(vs.song_id) AS song_count
        FROM videos v
        LEFT JOIN video_songs vs
            ON vs.video_id = v.id
        GROUP BY v.id
        ORDER BY v.published_at DESC
        LIMIT :limit
        OFFSET :offset
    """)

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "limit": limit,
                "offset": offset
            }
        ).mappings().all()

    return [dict(row) for row in rows]


def get_video_count():
    sql = text("""
        SELECT COUNT(*)
        FROM videos
    """)

    with engine.connect() as conn:
        return conn.execute(sql).scalar_one()

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


def get_player_by_slug(slug: str):
    sql = text("""
        SELECT
            p.id,
            p.name,
            p.full_name,
            p.nickname,
            p.hometown,
            p.birthday,
            p.bio,
            p.image_url,
            p.accent_color,
            p.slug,

            (
                SELECT COUNT(DISTINCT bp.battle_id)
                FROM battle_players bp
                WHERE bp.player_id = p.id
            ) AS total_battles,
            
            (
                SELECT COUNT(DISTINCT br.battle_id)
                FROM battle_results br
                WHERE br.placement = 1
                  AND (
                      br.player_id = p.id
                      OR EXISTS (
                          SELECT 1
                          FROM battle_team_members btm
                          JOIN battle_players winner_bp
                              ON winner_bp.id = btm.battle_player_id
                          WHERE btm.team_id = br.battle_team_id
                            AND winner_bp.player_id = p.id
                      )
                  )
            ) AS total_wins,
            
            ROUND(
                (
                    (
                        SELECT COUNT(DISTINCT br.battle_id)
                        FROM battle_results br
                        WHERE br.placement = 1
                          AND (
                              br.player_id = p.id
                              OR EXISTS (
                                  SELECT 1
                                  FROM battle_team_members btm
                                  JOIN battle_players winner_bp
                                      ON winner_bp.id = btm.battle_player_id
                                  WHERE btm.team_id = br.battle_team_id
                                    AND winner_bp.player_id = p.id
                              )
                          )
                    ) * 100.0
                )
                /
                NULLIF(
                    (
                        SELECT COUNT(DISTINCT bp.battle_id)
                        FROM battle_players bp
                        WHERE bp.player_id = p.id
                    ),
                    0
                ),
                1
            ) AS win_rate

        FROM players p
        WHERE p.slug = :slug
        LIMIT 1
    """)

    with engine.connect() as conn:
        row = conn.execute(
            sql,
            {"slug": slug}
        ).mappings().first()

    if not row:
        return None

    return dict(row)

def list_players():
    sql = text("""
        SELECT
            name,
            full_name,
            nickname,
            slug,
            accent_color,
            image_url
        FROM players
        WHERE slug IS NOT NULL
        ORDER BY id
        LIMIT 5
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()

    return [dict(r) for r in rows]

def get_video_id_by_youtube_id(youtube_video_id: str):
    with engine.connect() as conn:
        return conn.execute(
            text("""
                SELECT id
                FROM videos
                WHERE youtube_video_id = :youtube_video_id
            """),
            {
                "youtube_video_id": youtube_video_id
            }
        ).scalar_one_or_none()
