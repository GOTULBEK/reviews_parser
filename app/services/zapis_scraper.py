import asyncio
import logging
from typing import Any
import uuid

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)

SITE_BASE = "https://zapis.kz"

ZAPIS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
    "api_key": "8e56a169-e1d6-48e6-b29d-27f60b701a54",
    "client_id": "4IaLZgWuLEgO1x05gHxt",
    "client_secret": "qUkXu54A6Jc6GAmwswgp",
    "api_version": "15",
    "referrer": "WEB",
    "city_id": "1",
    "country_code": "KZ"
}


async def search_branches(
    client: httpx.AsyncClient, query: str, city: str, max_branches: int
) -> list[dict]:
    url = f"{SITE_BASE}/rest/clients-app/v1/firms/search"
    params = {"q": query}
    try:
        res = await client.get(url, params=params, headers=ZAPIS_HEADERS)
        res.raise_for_status()
        data = res.json()
        firms = data.get("data", {}).get("firms", [])
        
        # Limit to max_branches
        firms = firms[:max_branches]
        
        return [
            {
                "gis_branch_id": str(firm["id"]),
                "firm_url": f"{SITE_BASE}/rest/clients-app/v1/firms/{firm['id']}",
                "city": city,
            }
            for firm in firms
        ]
    except Exception as e:
        logger.error("zapis.kz search failed: %s", e)
        return []


async def scrape_branch_preview(
    client: httpx.AsyncClient, gis_branch_id: str, firm_url: str
) -> dict:
    try:
        res = await client.get(firm_url, headers=ZAPIS_HEADERS)
        res.raise_for_status()
        data = res.json().get("data", {}).get("firm", {})
        return {
            "gis_branch_id": str(gis_branch_id),
            "firm_url": firm_url,
            "name": data.get("name"),
            "category": data.get("category") or data.get("type"),
            "address": data.get("address", "Адрес не найден"),
        }
    except Exception as e:
        logger.error("zapis.kz preview failed for %s: %s", gis_branch_id, e)
        return {
            "gis_branch_id": str(gis_branch_id),
            "firm_url": firm_url,
            "name": None,
            "address": "Адрес не найден",
        }


async def scrape_branch(client: httpx.AsyncClient, gis_branch_id: str, firm_url: str) -> dict:
    try:
        firm_res = await client.get(firm_url, headers=ZAPIS_HEADERS)
        firm_res.raise_for_status()
        firm_data = firm_res.json().get("data", {}).get("firm", {})

        base_reviews_url = f"{SITE_BASE}/rest/clients-app/v1/firms/{gis_branch_id}/reviews"
        
        all_reviews = []
        last_id = None
        reviews_data_base = {}
        
        while True:
            params = {"type": "all"}
            if last_id:
                params["id"] = last_id
                
            rev_res = await client.get(base_reviews_url, headers=ZAPIS_HEADERS, params=params)
            rev_res.raise_for_status()
            
            data = rev_res.json()
            rev_payload = data.get("data", {})
            if not last_id:
                reviews_data_base = rev_payload
                
            reviews = rev_payload.get("reviews", [])
            if not reviews:
                break
                
            for r in reviews:
                review_id = str(r.get("id") or uuid.uuid4())
                user = r.get("user", {})
                user_name = user.get("name") or "Аноним"
                
                grade_map = {"EXCELLENT": 5, "GOOD": 4, "NORMAL": 3, "BAD": 1}
                rating = grade_map.get(r.get("grade")) or r.get("rating")
                
                all_reviews.append({
                    "gis_review_id": review_id,
                    "user_name": user_name,
                    "rating": rating,
                    "text": r.get("text") or "",
                    "official_answer_text": r.get("replyText") or None,
                    "official_answer_date": r.get("replyDate"),
                    "hiding_reason": None,
                    "is_rated": True,
                    "date_created": r.get("date"),
                    "date_edited": None,
                    "review_url": f"{SITE_BASE}/salon/firm/{gis_branch_id}#reviews",
                    "raw": r,
                })
                
            last_id = reviews[-1].get("id")
            await asyncio.sleep(0.5)
            
        dist = {
            k: v for k, v in reviews_data_base.items() 
            if k in ["EXCELLENT", "GOOD", "BAD", "NORMAL"]
        }
        
        return {
            "gis_branch_id": str(gis_branch_id),
            "company_name": firm_data.get("name"),
            "category": firm_data.get("category") or firm_data.get("type"),
            "categories": [firm_data.get("category"), firm_data.get("type")] if firm_data.get("category") and firm_data.get("type") else [firm_data.get("category") or firm_data.get("type")],
            "address": firm_data.get("address"),
            "rating": float(reviews_data_base.get("averageRating", 0.0) or 0.0),
            "total_reviews": int(reviews_data_base.get("totalReviewsCount", 0) or 0),
            "rating_distribution": dist,
            "url": firm_url,
            "reviews": all_reviews,
        }
    except Exception as e:
        logger.exception("zapis.kz scrape failed for %s: %s", gis_branch_id, e)
        raise
