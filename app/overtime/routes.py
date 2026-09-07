from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse
from app import queries as app_queries
from app.overtime import queries
from app.web import render

router = APIRouter(
    prefix="/overtime",
    include_in_schema=False,
)


@router.get("", response_class=HTMLResponse)
def overtime_page(request: Request):
    return render(
        request,
        "overtime/index.html",
        {
            "stats": queries.get_overtime_stats(),
        },
    )


@router.get("/items", response_class=HTMLResponse)
def overtime_items_page(
    request: Request,
    q: str | None = None,
):
    q = q.strip() if q else None

    return render(
        request,
        "overtime/items/index.html",
        {
            "query": q,
            "items": queries.search_overtime_items(q) if q else [],
        },
    )

@router.get("/episodes", response_class=HTMLResponse)
def overtime_episodes_page(request: Request):
    episodes = queries.get_overtime_episodes()

    return render(
        request,
        "overtime/episodes/index.html",
        {
            "episodes": episodes,
        },
    )

@router.get("/episodes/{episode_id}", response_class=HTMLResponse)
def overtime_episode_detail(
    request: Request,
    episode_id: int,
):
    episode = queries.get_overtime_episode(episode_id)

    if not episode:
        raise HTTPException(status_code=404)

    overtime = app_queries.get_overtime_view(episode["video_id"])

    return render(
        request,
        "overtime/episodes/detail.html",
        {
            "episode": episode,
            "overtime": overtime,
        },
    )