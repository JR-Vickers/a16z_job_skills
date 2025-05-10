"""
collectors/lever.py
Minimal collector for lever boards.
"""

from __future__ import annotations
import urllib.parse
import requests
import pandas as pd


def _extract_board_token(board_url: str) -> str:
    path = urllib.parse.urlsplit(board_url).path
    token = path.strip("/").split("/")[0]          # remove leading/trailing slashes
    if not token:
        raise ValueError(f"Could not parse lever token from URL: {board_url}")
    return token


def get_postings(board_url: str) -> pd.DataFrame:
    """
    Fetch every open job on a lever board and return metadata + HTML description.

    Parameters
    ----------
    board_url : str
        The public board URL, e.g. 'https://jobs.lever.co/kong'

    Returns
    -------
    pandas.DataFrame
        Columns: posting_id, title, location, team, description_html
    """
    token = _extract_board_token(board_url)
    api_url = f"https://api.lever.co/v0/postings/{token}?mode=json"


    resp = requests.get(api_url, timeout=30)
    resp.raise_for_status()                       # raises HTTPError on 4xx/5xx

    jobs = resp.json()
    rows: list[dict] = []

    for job in jobs:
        rows.append(
            {
                "posting_id": job["id"],
                "title": job["text"],
                "location": job["categories"].get("location"),
                "team": job["categories"].get("team"),
                "description_html": job["description"],
            }
        )

    return pd.DataFrame(rows)