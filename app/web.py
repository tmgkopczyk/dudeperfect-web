from pathlib import Path

from fastapi import Request
from fastapi.templating import Jinja2Templates
import os

BASE_DIR = Path(__file__).resolve().parent

templates = Jinja2Templates(
    directory=str(BASE_DIR / "templates")
)
templates.env.globals["environment"] = os.getenv(
    "CURRENT_ENVIRONMENT",
    "development"
)
def render(
    request: Request,
    template: str,
    context: dict | None = None,
    status_code: int = 200,
):
    return templates.TemplateResponse(
        template,
        {
            "request": request,
            **(context or {}),
        },
        status_code=status_code,
    )