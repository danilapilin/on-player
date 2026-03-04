"""Client for assist.intmarksol.com API."""

import asyncio
import logging

import httpx

from config import API_BASE, API_KEY

logger = logging.getLogger(__name__)


async def get_recordings(
    phone: str, date: str, client: httpx.AsyncClient, retries: int = 3
) -> list[dict]:
    """Get call recordings for a phone number on a specific date."""
    for attempt in range(retries):
        try:
            resp = await client.get(
                API_BASE,
                params={
                    "api_key": API_KEY,
                    "action": "getRecordingsList",
                    "phone": phone,
                    "date": date,
                },
                timeout=30,
            )
            data = resp.json()
            if data.get("result") != "ok":
                return []
            return data.get("data", []) or []
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                logger.debug(f"Retry {attempt+1} for {phone}/{date}: {e}")
                await asyncio.sleep(wait)
            else:
                logger.warning(f"Failed after {retries} retries: {phone}/{date}: {e}")
                return []
    return []


async def get_phones_list(client: httpx.AsyncClient) -> dict[str, list[str]]:
    """Get all phone numbers grouped by operator."""
    resp = await client.get(
        API_BASE,
        params={"api_key": API_KEY, "action": "getPhonesList"},
        timeout=30,
    )
    data = resp.json()
    if data.get("result") != "ok":
        return {}
    return data.get("data", {})


def get_recording_url(rec_url: str) -> str:
    """Add API key to recording URL for download."""
    return f"{rec_url}&api_key={API_KEY}"
