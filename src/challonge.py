# src/challonge.py
from __future__ import annotations
import os
import httpx

BASE = "https://api.challonge.com/v1"


class ChallongeError(RuntimeError):
    pass


def _api_key() -> str:
    key = os.getenv("CHALLONGE_API_KEY")
    if not key:
        raise ChallongeError("CHALLONGE_API_KEY is not set.")
    return key


async def _get(path: str, params: dict | None = None) -> any:
    key = _api_key()
    q = dict(params or {})
    q["api_key"] = key
    url = f"{BASE}{path}.json"

    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(url, params=q)
    if r.status_code == 401:
        raise ChallongeError("unauthorized: invalid API key or insufficient permissions")
    if r.status_code == 404:
        raise ChallongeError("not found: tournament/record doesn’t exist or you lack access")
    if r.status_code == 406:
        raise ChallongeError("unsupported format (request JSON or XML)")
    if r.status_code == 422:
        try:
            msg = "; ".join(r.json().get("errors", []))
        except Exception:
            msg = r.text
        raise ChallongeError(f"Validation error: {msg}")
    if r.status_code >= 400:
        raise ChallongeError(f"Challonge error ({r.status_code}): {r.text[:200]}")
    return r.json()


async def get_tournament(slug: str) -> dict:
    data = await _get(f"/tournaments/{slug}")
    return data["tournament"]


async def get_participants(slug: str) -> list[dict]:
    data = await _get(f"/tournaments/{slug}/participants")
    return [p["participant"] for p in data]


async def get_matches(slug: str) -> list[dict]:
    data = await _get(f"/tournaments/{slug}/matches")
    return [m["match"] for m in data]
