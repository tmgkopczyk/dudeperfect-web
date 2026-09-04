from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

import queries


api = APIRouter(prefix="/api")

DEFAULT_LIMIT = 50
MAX_LIMIT = 100


SEARCH_TYPES = {
    "songs": queries.search_songs,
    "artists": queries.search_artists,
    "videos": queries.search_videos,
    "battles": queries.search_battles,
    "players": queries.search_players,
    "stereotypes": queries.search_stereotype_segments,
    "recurring_stereotypes": queries.search_recurring_stereotypes,
}


@api.get("/search")
def api_search(
    q: Annotated[
        str,
        Query(description="Search text"),
    ] = "",
    query_type: Annotated[
        str,
        Query(
            alias="type",
            description="Type of result to search",
        ),
    ] = "all",
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_LIMIT,
            description="Maximum number of results to return",
        ),
    ] = DEFAULT_LIMIT,
):
    q = q.strip()
    query_type = query_type.strip().lower()

    if query_type == "all":
        return {
            name: search_function(q, limit=limit)
            for name, search_function in SEARCH_TYPES.items()
        }

    if query_type not in SEARCH_TYPES:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Invalid search type",
                "valid_types": ["all", *SEARCH_TYPES.keys()],
            },
        )

    return SEARCH_TYPES[query_type](q, limit=limit)


@api.get("/songs/{song_id}")
def api_song(song_id: int):
    song = queries.get_song_detail(song_id)

    if not song:
        raise HTTPException(
            status_code=404,
            detail="Song not found",
        )

    return song


@api.get("/artists/{artist_id}")
def api_artist(artist_id: int):
    artist = queries.get_artist_detail(artist_id)

    if not artist:
        raise HTTPException(
            status_code=404,
            detail="Artist not found",
        )

    return artist

@api.get(
    "/videos",
    summary="List videos",
    tags=["Videos"],
)
def api_videos(
    q: Annotated[
        str,
        Query(description="Filter videos by search text"),
    ] = "",
    limit: Annotated[
        int | None,
        Query(
            description="Maximum number of videos to return",
            ge=1,
        ),
    ] = None,
):
    q = q.strip()

    if q:
        items = queries.search_videos(q, limit=limit)
    else:
        items = queries.get_videos(limit=limit)

    return {
        "items": items,
        "count": len(items),
    }


@api.get("/videos/categories")
def api_video_categories(
    limit: Annotated[
        int | None,
        Query(ge=1),
    ] = None,
):
    items = [
        dict(row)
        for row in queries.list_video_categories()
    ]

    if limit is not None:
        items = items[:limit]

    return {
        "items": items,
        "count": len(items),
    }


@api.get("/videos/categories/{slug}")
def api_video_category(
    slug: str,
    q: Annotated[
        str,
        Query(description="Filter videos by search text"),
    ] = "",
    limit: Annotated[
        int | None,
        Query(ge=1),
    ] = None,
):
    category = queries.get_video_category_by_slug(slug)

    if not category:
        raise HTTPException(
            status_code=404,
            detail="Video category not found",
        )

    q = q.strip()

    videos = [
        dict(row)
        for row in queries.list_videos_for_category(
            category["id"],
            q=q,
        )
    ]

    if limit is not None:
        videos = videos[:limit]

    return {
        "id": category["id"],
        "slug": category["slug"],
        "title": category["title"],
        "description": category["description"],
        "videos": videos,
        "video_count": len(videos),
    }


@api.get("/videos/youtube/{youtube_video_id}")
def api_video_by_youtube_id(youtube_video_id: str):
    video_id = queries.get_video_id_by_youtube_id(
        youtube_video_id
    )

    if video_id is None:
        raise HTTPException(
            status_code=404,
            detail="Video not found",
        )

    video = queries.get_video_detail_page(video_id)

    if not video:
        raise HTTPException(
            status_code=404,
            detail="Video not found",
        )

    return video

@api.get("/videos/{video_id}")
def api_video(video_id: int):
    video = queries.get_video_detail_page(video_id)

    if not video:
        raise HTTPException(
            status_code=404,
            detail="Video not found",
        )

    return video




@api.get("/songs")
def api_songs(
    q: Annotated[
        str,
        Query(description="Filter songs by search text"),
    ] = "",
    limit: Annotated[
        int | None,
        Query(ge=1),
    ] = None,
):
    q = q.strip()

    if q:
        items = queries.search_songs(q, limit=limit)
    else:
        items = queries.get_all_songs()

        if limit is not None:
            items = items[:limit]

    return {
        "items": items,
        "count": len(items),
    }


@api.get("/artists")
def api_artists(
    q: Annotated[
        str,
        Query(description="Filter artists by search text"),
    ] = "",
    limit: Annotated[
        int | None,
        Query(ge=1),
    ] = None,
):
    q = q.strip()

    if q:
        items = queries.search_artists(q, limit=limit)
    else:
        items = queries.get_all_artists()

        if limit is not None:
            items = items[:limit]

    return {
        "items": items,
        "count": len(items),
    }


@api.get("/players")
def api_players(
    q: Annotated[
        str,
        Query(description="Filter players by search text"),
    ] = "",
    limit: Annotated[
        int | None,
        Query(ge=1),
    ] = None,
):
    q = q.strip()

    items = queries.search_players(q, limit=limit)

    return {
        "items": items,
        "count": len(items),
    }


@api.get("/players/{player_id}")
def api_player(player_id: int):
    player = queries.get_player_by_id(player_id)

    if not player:
        raise HTTPException(
            status_code=404,
            detail="Player not found",
        )

    if player["birthday"] is not None:
        player["birthday"] = player["birthday"].isoformat()

    if player["win_rate"] is not None:
        player["win_rate"] = float(player["win_rate"])

    return player


@api.get("/battles")
def api_battles(
    q: Annotated[
        str,
        Query(description="Filter battles by search text"),
    ] = "",
    limit: Annotated[
        int | None,
        Query(ge=1),
    ] = None,
):
    q = q.strip()

    if q:
        items = queries.search_battles(q, limit=limit)
    else:
        items = queries.get_battles()

        if limit is not None:
            items = items[:limit]

    return {
        "items": items,
        "count": len(items),
    }


@api.get("/battles/{battle_id}")
def api_battle(battle_id: int):
    video_id = queries.get_battle_video_id(battle_id)

    if video_id is None:
        raise HTTPException(
            status_code=404,
            detail="Battle not found",
        )

    battle = queries.get_battle_view(video_id)

    if not battle:
        raise HTTPException(
            status_code=404,
            detail="Battle not found",
        )

    return battle


@api.get("/stereotypes")
def api_stereotypes(
    limit: Annotated[
        int | None,
        Query(ge=1),
    ] = None,
):
    items = queries.get_stereotypes_episodes()

    if limit is not None:
        items = items[:limit]

    return {
        "items": items,
        "count": len(items),
    }

@api.get("/stereotypes/recurring")
def api_recurring_stereotypes(
    limit: Annotated[
        int | None,
        Query(ge=1),
    ] = None,
):
    items = queries.get_recurring_stereotypes()

    if limit is not None:
        items = items[:limit]

    return {
        "items": items,
        "count": len(items),
    }


@api.get("/stereotypes/recurring/{recurring_id}")
def api_recurring_stereotype(recurring_id: int):
    recurring = queries.get_recurring_stereotype(recurring_id)

    if not recurring:
        raise HTTPException(
            status_code=404,
            detail="Recurring stereotype not found",
        )

    return recurring


@api.get("/stereotypes/performers")
def api_stereotype_performers(
    limit: Annotated[
        int | None,
        Query(ge=1),
    ] = None,
):
    items = queries.get_stereotype_performers()

    if limit is not None:
        items = items[:limit]

    return {
        "items": items,
        "count": len(items),
    }


@api.get("/stereotypes/performers/{player_id}")
def api_stereotype_performer(player_id: int):
    performer = queries.get_stereotype_performer_view(player_id)

    if not performer:
        raise HTTPException(
            status_code=404,
            detail="Stereotype performer not found",
        )

    return performer

@api.get("/stereotypes/{episode_id}")
def api_stereotype(episode_id: int):
    video_id = queries.get_stereotype_video_id(episode_id)

    if video_id is None:
        raise HTTPException(
            status_code=404,
            detail="Stereotype episode not found",
        )

    episode = queries.get_stereotypes_view(video_id)

    if not episode:
        raise HTTPException(
            status_code=404,
            detail="Stereotype episode not found",
        )

    return episode








@api.get("/players/{player_id}/battles")
def api_player_battles(
    player_id: int,
    limit: Annotated[
        int | None,
        Query(ge=1),
    ] = None,
):
    player = queries.get_player_by_id(player_id)

    if not player:
        raise HTTPException(
            status_code=404,
            detail="Player not found",
        )

    items = queries.get_recent_battles_for_player(
        player_id,
        limit=limit,
    )

    return {
        "items": items,
        "count": len(items),
    }


@api.get("/players/{player_id}/stereotypes")
def api_player_stereotypes(
    player_id: int,
    limit: Annotated[
        int | None,
        Query(ge=1),
    ] = None,
):
    player = queries.get_player_by_id(player_id)

    if not player:
        raise HTTPException(
            status_code=404,
            detail="Player not found",
        )

    items = queries.get_stereotype_appearances_for_player(
        player_id,
        limit=limit,
    )

    return {
        "items": items,
        "count": len(items),
    }