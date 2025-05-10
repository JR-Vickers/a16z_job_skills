"""
collectors/ashby.py
Minimal collector for Ashby boards.
"""

from __future__ import annotations
import urllib.parse
import requests
import pandas as pd, json, re
from bs4 import BeautifulSoup
from html import unescape
from urllib.parse import unquote


def _fetch_jobs(slug: str) -> list[dict]:
    # Try legacy REST first
    rest = f"https://api.ashbyhq.com/posting-api/job-board?organizationSlug={slug}"
    r = requests.get(rest, timeout=15)
    if r.status_code == 200:
        return r.json()["jobs"]

    # Fallback to GraphQL
    gql = f"https://jobs.ashbyhq.com/{slug}/non-user-graphql?op=ApiJobBoardWithTeams"
    r = requests.get(gql, timeout=15)
    r.raise_for_status()
    return r.json()["data"]["jobBoard"]["jobs"]

def _extract_board_token(board_url: str) -> str:
    """
    Examples
    --------
    https://jobs.ashbyhq.com/intangible.ai       -> 'intangible.ai'
    """
    path = urllib.parse.urlsplit(board_url).path   # '/intangible.ai'
    token = path.strip("/").split("/")[0]          # remove leading/trailing slashes
    if not token:
        raise ValueError(f"Could not parse Ashby token from URL: {board_url}")
    return token



QUERY_LIST = """
query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
  jobBoard: jobBoardWithTeams(
    organizationHostedJobsPageName: $organizationHostedJobsPageName
  ) {
    jobPostings { id title teamId locationName }
  }
}
"""

DETAIL_QUERY = """
query ApiJobPosting(
  $organizationHostedJobsPageName: String!,
  $jobPostingId: String!
) {
  jobPosting(
    organizationHostedJobsPageName: $organizationHostedJobsPageName,
    jobPostingId: $jobPostingId
  ) { descriptionHtml }
}
"""

def html_to_text(html: str) -> str:
    soup = BeautifulSoup(unescape(html), "html.parser")
    for t in soup(["style", "script"]):
        t.decompose()
    return soup.get_text(" ", strip=True)

def get_postings(board_url: str) -> pd.DataFrame:
    """Return a DataFrame of postings (+HTML & plain text) for one Ashby board."""
    slug = _extract_board_token(board_url)           # e.g. 'Hippocratic%20AI'
    slug = unquote(slug).lower().replace(" ", "-")   # → 'Hippocratic-AI'

    # ---- Step 1: list job postings -------------------------------------------
    list_url = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"
    payload = {"query": QUERY_LIST, "variables": {"organizationHostedJobsPageName": slug}}
    resp_json = requests.post(list_url, json=payload, timeout=30).json()

    # guard: if server returns errors, skip this board
    if "data" not in resp_json:
        print("Ashby error –", slug, resp_json.get("errors", resp_json)[:1])
        return pd.DataFrame()

    postings = resp_json["data"]["jobBoard"]["jobPostings"]

    # ---- Step 2: fetch description HTML for each posting ---------------------
    detail_url = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobPosting"
    rows = []
    for p in postings:
        variables = {
            "organizationHostedJobsPageName": slug,
            "jobPostingId": p["id"],
        }
        det = requests.post(
            detail_url,
            json={"query": DETAIL_QUERY, "variables": variables},
            timeout=30
        ).json()

        # Some postings can disappear between calls; guard again
        if "data" not in det:
            continue

        html = det["data"]["jobPosting"]["descriptionHtml"]
        rows.append(
            {
                "posting_id": p["id"],
                "title":       p["title"],
                "location":    p.get("locationName"),
                "team":        p.get("teamId"),
                "description_html": html,
                "description":      html_to_text(html),
            }
        )

    return pd.DataFrame(rows)