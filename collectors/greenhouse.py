"""
collectors/greenhouse.py
Minimal collector for Greenhouse boards.
"""

from __future__ import annotations
from bs4 import BeautifulSoup
from html import unescape
import urllib.parse
import requests
import pandas as pd

def html_to_text(html: str) -> str:
    soup = BeautifulSoup(unescape(html), "html.parser")
    for t in soup(["style", "script"]):
        t.decompose()
    return soup.get_text(" ", strip=True)

def _extract_board_token(board_url: str) -> str:
    """
    Examples
    --------
    https://boards.greenhouse.io/carta           -> 'carta'
    https://boards.greenhouse.io/sourcegraph91/  -> 'sourcegraph91'
    """
    path = urllib.parse.urlsplit(board_url).path   # '/carta' or '/sourcegraph91/'
    token = path.strip("/").split("/")[0]          # remove leading/trailing slashes
    if not token:
        raise ValueError(f"Could not parse Greenhouse token from URL: {board_url}")
    return token


def get_postings(board_url: str) -> pd.DataFrame:
    """
    Fetch every open job on a Greenhouse board and return metadata + HTML description.

    Parameters
    ----------
    board_url : str
        The public board URL, e.g. 'https://boards.greenhouse.io/carta'

    Returns
    -------
    pandas.DataFrame
        Columns: posting_id, title, location, team, description_html
    """
    token = _extract_board_token(board_url)
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"

    resp = requests.get(api_url, timeout=30)
    resp.raise_for_status()                       # raises HTTPError on 4xx/5xx

    jobs = resp.json().get("jobs", [])
    rows: list[dict] = []

    for job in jobs:
        rows.append(
            {
                "posting_id": job["id"],
                "title": job["title"],
                "location": job.get("location", {}).get("name"),
                "team": ", ".join(d["name"] for d in job.get("departments", [])),
                "description": html_to_text(job["content"]),
            }
        )

    return pd.DataFrame(rows)