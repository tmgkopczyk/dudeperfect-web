from apiflask import APIBlueprint, Schema
from flask import jsonify, request
import queries
from marshmallow import fields, validate

api = APIBlueprint("api", __name__, url_prefix="/api")

DEFAULT_LIMIT = 50
MAX_LIMIT = 100
class CollectionQuerySchema(Schema):
    q = fields.String(
        load_default="",
        metadata={
            "description": "Filter results by search text"
        }
    )

    limit = fields.Integer(
        required=False,
        validate=validate.Range(min=1),
        metadata={
            "description": "Maximum number of results to return"
        }
    )

SEARCH_TYPES = {
    "songs": queries.search_songs,
    "artists": queries.search_artists,
    "videos": queries.search_videos,
    "battles": queries.search_battles,
    "players": queries.search_players,
    "stereotypes": queries.search_stereotype_segments,
    "recurring_stereotypes": queries.search_recurring_stereotypes,
}

def parse_limit():
    limit_arg = request.args.get("limit")

    if limit_arg is None:
        return None, None

    try:
        limit = int(limit_arg)
    except ValueError:
        return None, (
            jsonify({"error": "limit must be a positive integer"}),
            400
        )

    if limit < 1:
        return None, (
            jsonify({"error": "limit must be a positive integer"}),
            400
        )

    return limit, None


@api.route("/search")
def api_search():
    q = request.args.get("q", "").strip()
    query_type = request.args.get("type", "all").strip().lower()

    limit_arg = request.args.get("limit")

    if limit_arg is None:
        limit = DEFAULT_LIMIT
    else:
        try:
            limit = int(limit_arg)
        except ValueError:
            return jsonify({
                "error": f"limit must be an integer between 1 and {MAX_LIMIT}"
            }), 400

    if limit < 1 or limit > MAX_LIMIT:
        return jsonify({
            "error": f"limit must be an integer between 1 and {MAX_LIMIT}"
        }), 400

    if query_type == "all":
        results = {
            name: search_function(q, limit=limit)
            for name, search_function in SEARCH_TYPES.items()
        }
        return jsonify(results)

    if query_type not in SEARCH_TYPES:
        return jsonify({
            "error": "Invalid search type",
            "valid_types": ["all", *SEARCH_TYPES.keys()]
        }), 400

    return jsonify(
        SEARCH_TYPES[query_type](q, limit=limit)
    )

@api.route("/songs/<int:song_id>")
def api_song(song_id):
    song = queries.get_song_detail(song_id)
    if not song:
        return jsonify({
            "error": "Song not found"
        }), 404
    return jsonify(song)

@api.route("/artists/<int:artist_id>")
def api_artist(artist_id):
    artist = queries.get_artist_detail(artist_id)

    if not artist:
        return jsonify({
            "error": "Artist not found"
        }), 404

    return jsonify(artist)

@api.route("/videos/<int:video_id>")
def api_video(video_id):
    video = queries.get_video_detail_page(video_id)

    if not video:
        return jsonify({
            "error": "Video not found"
        }), 404

    return jsonify(video)

@api.route("/videos")
@api.input(CollectionQuerySchema, location="query", arg_name="query")
def api_videos(query):
    q = query["q"].strip()
    limit = query.get("limit")

    if q:
        items = queries.search_videos(q, limit=limit)
    else:
        items = queries.get_videos(limit=limit)

    return jsonify({
        "items": items,
        "count": len(items)
    })
@api.route("/songs")
def api_songs():
    q = request.args.get("q", "").strip()

    limit, error = parse_limit()
    if error:
        return error

    if q:
        items = queries.search_songs(q, limit=limit)
    else:
        items = queries.get_all_songs()

        if limit is not None:
            items = items[:limit]

    return jsonify({
        "items": items,
        "count": len(items)
    })

@api.route("/artists")
def api_artists():
    q = request.args.get("q", "").strip()

    limit, error = parse_limit()
    if error:
        return error

    if q:
        items = queries.search_artists(q, limit=limit)
    else:
        items = queries.get_all_artists()

        if limit is not None:
            items = items[:limit]

    return jsonify({
        "items": items,
        "count": len(items)
    })

@api.route("/players")
def api_players():
    q = request.args.get("q", "").strip()

    limit, error = parse_limit()
    if error:
        return error

    items = queries.search_players(q, limit=limit)

    return jsonify({
        "items": items,
        "count": len(items)
    })

@api.route("/players/<int:player_id>")
def api_player(player_id):
    player = queries.get_player_by_id(player_id)

    if not player:
        return jsonify({
            "error": "Player not found"
        }), 404

    if player["birthday"] is not None:
        player["birthday"] = player["birthday"].isoformat()

    if player["win_rate"] is not None:
        player["win_rate"] = float(player["win_rate"])

    return jsonify(player)

@api.route("/battles")
def api_battles():
    q = request.args.get("q", "").strip()

    limit, error = parse_limit()
    if error:
        return error

    if q:
        items = queries.search_battles(q, limit=limit)
    else:
        items = queries.get_battles()

        if limit is not None:
            items = items[:limit]

    return jsonify({
        "items": items,
        "count": len(items)
    })

@api.route("/battles/<int:battle_id>")
def api_battle(battle_id):
    video_id = queries.get_battle_video_id(battle_id)

    if video_id is None:
        return jsonify({
            "error": "Battle not found"
        }), 404

    battle = queries.get_battle_view(video_id)

    if not battle:
        return jsonify({
            "error": "Battle not found"
        }), 404

    return jsonify(battle)

@api.route("/stereotypes")
def api_stereotypes():
    limit, error = parse_limit()
    if error:
        return error

    items = queries.get_stereotypes_episodes()

    if limit is not None:
        items = items[:limit]

    return jsonify({
        "items": items,
        "count": len(items)
    })

@api.route("/stereotypes/<int:episode_id>")
def api_stereotype(episode_id):
    video_id = queries.get_stereotype_video_id(episode_id)

    if video_id is None:
        return jsonify({
            "error": "Stereotype episode not found"
        }), 404

    episode = queries.get_stereotypes_view(video_id)

    if not episode:
        return jsonify({
            "error": "Stereotype episode not found"
        }), 404

    return jsonify(episode)

@api.route("/stereotypes/recurring")
def api_recurring_stereotypes():
    limit, error = parse_limit()
    if error:
        return error

    items = queries.get_recurring_stereotypes()

    if limit is not None:
        items = items[:limit]

    return jsonify({
        "items": items,
        "count": len(items)
    })


@api.route("/stereotypes/recurring/<int:recurring_id>")
def api_recurring_stereotype(recurring_id):
    recurring = queries.get_recurring_stereotype(recurring_id)

    if not recurring:
        return jsonify({
            "error": "Recurring stereotype not found"
        }), 404

    return jsonify(recurring)

@api.route("/stereotypes/performers")
def api_stereotype_performers():
    limit, error = parse_limit()
    if error:
        return error

    items = queries.get_stereotype_performers()

    if limit is not None:
        items = items[:limit]

    return jsonify({
        "items": items,
        "count": len(items)
    })


@api.route("/stereotypes/performers/<int:player_id>")
def api_stereotype_performer(player_id):
    performer = queries.get_stereotype_performer_view(player_id)

    if not performer:
        return jsonify({
            "error": "Stereotype performer not found"
        }), 404

    return jsonify(performer)

@api.route("/videos/categories")
def api_video_categories():
    limit, error = parse_limit()
    if error:
        return error

    items = [
        dict(row)
        for row in queries.list_video_categories()
    ]

    if limit is not None:
        items = items[:limit]

    return jsonify({
        "items": items,
        "count": len(items)
    })


@api.route("/videos/categories/<slug>")
def api_video_category(slug):
    category = queries.get_video_category_by_slug(slug)

    if not category:
        return jsonify({
            "error": "Video category not found"
        }), 404

    q = request.args.get("q", "").strip()

    videos = [
        dict(row)
        for row in queries.list_videos_for_category(
            category["id"],
            q=q
        )
    ]

    limit, error = parse_limit()
    if error:
        return error

    if limit is not None:
        videos = videos[:limit]

    return jsonify({
        "id": category["id"],
        "slug": category["slug"],
        "title": category["title"],
        "description": category["description"],
        "videos": videos,
        "video_count": len(videos)
    })

@api.route("/videos/youtube/<youtube_video_id>")
def api_video_by_youtube_id(youtube_video_id):
    video_id = queries.get_video_id_by_youtube_id(youtube_video_id)

    if video_id is None:
        return jsonify({
            "error": "Video not found"
        }), 404

    video = queries.get_video_detail_page(video_id)

    if not video:
        return jsonify({
            "error": "Video not found"
        }), 404

    return jsonify(video)

@api.route("/players/<int:player_id>/battles")
def api_player_battles(player_id):
    player = queries.get_player_by_id(player_id)

    if not player:
        return jsonify({
            "error": "Player not found"
        }), 404

    limit, error = parse_limit()
    if error:
        return error

    items = queries.get_recent_battles_for_player(
        player_id,
        limit=limit
    )

    return jsonify({
        "items": items,
        "count": len(items)
    })


@api.route("/players/<int:player_id>/stereotypes")
def api_player_stereotypes(player_id):
    player = queries.get_player_by_id(player_id)

    if not player:
        return jsonify({
            "error": "Player not found"
        }), 404

    limit, error = parse_limit()
    if error:
        return error

    items = queries.get_stereotype_appearances_for_player(
        player_id,
        limit=limit
    )

    return jsonify({
        "items": items,
        "count": len(items)
    })