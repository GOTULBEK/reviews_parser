import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

import anthropic
from anthropic import AsyncAnthropic
from anthropic.types import Message
from sqlalchemy.dialects.postgresql import insert as pg_insert
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pydantic import ValidationError
from fastapi import HTTPException

from app.core.config import settings
from app.db.database import AsyncSessionLocal
from app.models.tasks import ClaudeApiCache
from app.schemas.dashboard import (
    InsightItem,
    ProblemItem,
    PriorityItem,
    RecommendationItem,
    ReplyTemplate,
    TopicBarItem,
    TopicListItem,
    TopicTrend,
    TopMention,
)

logger = logging.getLogger(__name__)

# Initialize client only if key is present
client = AsyncAnthropic(api_key=settings.anthropic_api_key) if settings.anthropic_api_key else None

def _format_reviews_for_prompt(reviews: list[dict]) -> str:
    """Formats reviews compactly to save tokens."""
    # reviews is expected to be a list of dicts with 'rating' and 'text'
    lines = []
    for r in reviews[:settings.max_reviews_to_analyze]:
        if r.get('text'):
            rating = r.get('rating') or '?'
            lines.append(f"[Rating: {rating}/5] {r['text']}")
            lines.append(f"[Rating: {rating}/5] {r['text']}")
    return "\n".join(lines)


def _extract_tool_input(response) -> dict | None:
    for block in response.content:
        if getattr(block, "type", None) == "tool_use":
            return block.input
    logger.warning("Claude did not return a tool_use block. Content: %s", response.content)
    return None

def _hash_request(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def _load_cached_response(request_hash: str) -> Message | None:
    try:
        async with AsyncSessionLocal() as s:
            row = await s.get(ClaudeApiCache, request_hash)
            if row is None:
                return None
            response_data = row.response
            row.hit_count = (row.hit_count or 0) + 1
            row.last_hit_at = datetime.now(timezone.utc)
            await s.commit()
    except Exception:
        logger.exception("Claude cache lookup failed")
        return None

    try:
        return Message.model_validate(response_data)
    except Exception:
        logger.exception("Cached Claude response failed to deserialize, ignoring entry")
        return None


async def _store_cached_response(request_hash: str, model_name: str, response: Message) -> None:
    try:
        payload = response.model_dump(mode="json")
    except Exception:
        logger.exception("Failed to serialize Claude response for caching")
        return

    try:
        async with AsyncSessionLocal() as s:
            stmt = (
                pg_insert(ClaudeApiCache)
                .values(request_hash=request_hash, model=model_name, response=payload)
                .on_conflict_do_nothing(index_elements=["request_hash"])
            )
            await s.execute(stmt)
            await s.commit()
    except Exception:
        logger.exception("Failed to save Claude response to cache")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((anthropic.RateLimitError, anthropic.APIError, anthropic.APIConnectionError)),
    reraise=True
)
async def _call_anthropic(**kwargs):
    if not client:
        raise ValueError("Anthropic client is not initialized")

    request_hash = _hash_request(kwargs)
    cached = await _load_cached_response(request_hash)
    if cached is not None:
        logger.info("Claude cache hit (hash=%s)", request_hash[:12])
        return cached

    response = await client.messages.create(**kwargs)
    await _store_cached_response(request_hash, kwargs.get("model", ""), response)
    return response


async def generate_problems(reviews: list[dict]) -> list[ProblemItem]:
    """Analyzes negative/neutral reviews to extract core problems."""
    if not client:
        logger.warning("Anthropic API key missing. Returning empty problems.")
        return []

    # Filter out 5-star reviews to save context window and focus on issues
    target_reviews = [r for r in reviews if r.get('rating', 5) <= 3]
    if not target_reviews:
        return []

    reviews_text = _format_reviews_for_prompt(target_reviews)
    
    tool_schema = {
        "name": "extract_problems",
        "description": "Extract the most critical recurring problems from the provided customer reviews.",
        "input_schema": {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "key": {"type": "string", "description": "Short slug, e.g., 'long_queues'"},
                            "title": {"type": "string", "description": "Human readable title of the problem"},
                            "mentions": {"type": "integer", "description": "Estimated number of times this was mentioned"},
                            "quotes": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "1-3 direct, short quotes from reviews proving the problem"
                            },
                            "recommendation": {"type": "string", "description": "Actionable advice to fix this"},
                            "kpi_hint": {"type": "string", "description": "Which metric this affects (e.g., 'avg_rating', 'replies_pct')"}
                        },
                        "required": ["key", "title", "mentions", "quotes"]
                    }
                }
            },
            "required": ["items"]
        }
    }

    try:
        response = await _call_anthropic(
            model=settings.claude_model,
            max_tokens=2048,
            temperature=0.2,
            system="You are a strict data analyst. Group similar complaints into clear, distinct problems. Do not invent quotes; extract them verbatim.",
            messages=[{"role": "user", "content": f"Analyze these reviews and extract the core problems:\n\n{reviews_text}"}],
            tools=[tool_schema],
            tool_choice={"type": "tool", "name": "extract_problems"}
        )

        data = _extract_tool_input(response)
        if not data:
            return []
        
        problems = []
        for item in data.get("items", []):
            try:
                problems.append(ProblemItem(**item))
            except ValidationError as e:
                logger.warning(f"Skipping malformed problem item: {item}. Error: {e}")
        return problems

    except Exception as e:
        logger.exception("Claude API failed during problem extraction")
        raise HTTPException(status_code=503, detail="AI service is currently unavailable. Please try again later.")


async def generate_actions(reviews: list[dict]) -> tuple[list[PriorityItem], list[InsightItem]]:
    """Analyzes the full corpus to generate actionable priorities and business insights."""
    if not client:
        return [], []

    reviews_text = _format_reviews_for_prompt(reviews)
    
    tool_schema = {
        "name": "extract_actions_and_insights",
        "description": "Formulate strategic priorities and hidden insights based on review sentiment.",
        "input_schema": {
            "type": "object",
            "properties": {
                "priorities": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "level": {"type": "integer", "description": "Priority level: 1 (Urgent) to 3 (Low)"},
                            "title": {"type": "string", "description": "The strategic goal, e.g., 'Improve Staff Training'"},
                            "items": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Specific steps to achieve this goal based on reviews"
                            }
                        },
                        "required": ["level", "title", "items"]
                    }
                },
                "insights": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string", "description": "Short label, e.g., 'Peak Hours', 'Hidden Strength'"},
                            "value": {"type": "string", "description": "The actual finding"},
                            "subtext": {"type": "string", "description": "Why this matters or context"}
                        },
                        "required": ["label", "value"]
                    }
                }
            },
            "required": ["priorities", "insights"]
        }
    }

    try:
        response = await _call_anthropic(
            model=settings.claude_model,
            max_tokens=2500,
            temperature=0.3,
            system="You are an expert operations consultant. Read these reviews and generate a realistic action plan and unique business insights.",
            messages=[{"role": "user", "content": f"Analyze these reviews:\n\n{reviews_text}"}],
            tools=[tool_schema],
            tool_choice={"type": "tool", "name": "extract_actions_and_insights"}
        )

        data = _extract_tool_input(response)
        if not data:
            return [], []
        
        priorities = []
        for p in data.get("priorities", []):
            try:
                priorities.append(PriorityItem(**p))
            except ValidationError as e:
                logger.warning(f"Skipping malformed priority item: {p}. Error: {e}")
                
        insights = []
        for i in data.get("insights", []):
            try:
                insights.append(InsightItem(**i))
            except ValidationError as e:
                logger.warning(f"Skipping malformed insight item: {i}. Error: {e}")
        
        return priorities, insights

    except Exception as e:
        logger.exception("Claude API failed during action extraction")
        raise HTTPException(status_code=503, detail="AI service is currently unavailable. Please try again later.")


async def generate_top_mentions(
    reviews: list[dict],
) -> tuple[list[TopMention], list[TopMention]]:
    """
    Analyzes the full review corpus and returns (top_problems, top_praise) as
    TopMention lists — the same contract used by /overview.
    Each item has: label (canonical lemma), mentions (count), examples (verbatim quotes).
    Returns ([], []) when no API key is configured or the call fails.
    """
    if not client:
        logger.warning("Anthropic API key missing. Returning empty top mentions.")
        return [], []

    if not reviews:
        return [], []

    reviews_text = _format_reviews_for_prompt(reviews)

    tool_schema = {
        "name": "extract_top_mentions",
        "description": (
            "Extract the most frequently mentioned complaint topics and praise topics "
            "from customer reviews. Labels must be short canonical Russian/Kazakh terms "
            "in lemma (dictionary) form. Quotes must be verbatim fragments from the reviews."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "top_problems": {
                    "type": "array",
                    "description": "Recurring complaint topics from negative/neutral reviews, sorted by frequency.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {
                                "type": "string",
                                "description": "Short canonical term in lemma form, e.g. 'очередь', 'грубый персонал'",
                            },
                            "mentions": {
                                "type": "integer",
                                "description": "Estimated number of reviews that mention this topic",
                            },
                            "examples": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "2-3 verbatim short quotes from reviews that illustrate this problem",
                            },
                        },
                        "required": ["label", "mentions", "examples"],
                    },
                },
                "top_praise": {
                    "type": "array",
                    "description": "Recurring praise topics from positive reviews, sorted by frequency.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {
                                "type": "string",
                                "description": "Short canonical term in lemma form, e.g. 'вежливый', 'чистота'",
                            },
                            "mentions": {
                                "type": "integer",
                                "description": "Estimated number of reviews that mention this topic",
                            },
                            "examples": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "2-3 verbatim short quotes from reviews that illustrate this praise",
                            },
                        },
                        "required": ["label", "mentions", "examples"],
                    },
                },
            },
            "required": ["top_problems", "top_praise"],
        },
    }

    try:
        response = await _call_anthropic(
            model=settings.claude_model,
            max_tokens=2048,
            temperature=0.2,
            system=(
                "You are a strict data analyst. "
                "Identify recurring topics by grouping semantically similar complaints and compliments. "
                "Labels must be in lemma (dictionary) form. "
                "Never invent quotes — extract them verbatim from the provided reviews. "
                "Return up to 8 items per category, sorted by mention count descending."
            ),
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Analyze these reviews and extract the top recurring problem topics "
                        f"and praise topics:\n\n{reviews_text}"
                    ),
                }
            ],
            tools=[tool_schema],
            tool_choice={"type": "tool", "name": "extract_top_mentions"},
        )

        data = _extract_tool_input(response)
        if not data:
            return [], []

        top_problems = []
        for item in data.get("top_problems", []):
            if not isinstance(item, dict):
                logger.warning(f"Skipping non-dict top_problem: {item!r}")
                continue
            try:
                top_problems.append(TopMention(**item))
            except (ValidationError, TypeError) as e:
                logger.warning(f"Skipping malformed top_problem: {item}. Error: {e}")

        top_praise = []
        for item in data.get("top_praise", []):
            if not isinstance(item, dict):
                logger.warning(f"Skipping non-dict top_praise: {item!r}")
                continue
            try:
                top_praise.append(TopMention(**item))
            except (ValidationError, TypeError) as e:
                logger.warning(f"Skipping malformed top_praise: {item}. Error: {e}")

        return top_problems, top_praise

    except Exception:
        logger.exception("Claude API failed during top mentions extraction")
        return [], []


async def generate_recommendations(
    top_problems: list[dict],
    top_praise: list[dict],
    kpis: dict,
) -> list[RecommendationItem]:
    """Generate exactly 3 actionable recommendation cards in Russian.

    Inputs:
      top_problems / top_praise: lists of {label, mentions, examples}.
      kpis: dict with avg_rating, negative_pct, replies_pct, reviews_total.
    """
    if not client:
        logger.warning("Anthropic API key missing. Returning empty recommendations.")
        return []

    if not top_problems and not top_praise:
        return []

    tool_schema = {
        "name": "generate_recommendations",
        "description": (
            "Produce exactly 3 actionable recommendations for a business owner, "
            "ordered by impact. Each recommendation references concrete data "
            "(numbers from KPIs or labels from top topics)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "minItems": 3,
                    "maxItems": 3,
                    "items": {
                        "type": "object",
                        "properties": {
                            "icon": {
                                "type": "string",
                                "enum": ["shield", "headphones", "volume", "warning", "trend", "star"],
                                "description": (
                                    "shield = answer-rate / SLA; headphones = address core problems; "
                                    "volume = amplify strengths; warning = urgent risk; trend = growing trend; "
                                    "star = standout positive."
                                ),
                            },
                            "tone": {
                                "type": "string",
                                "enum": ["red", "orange", "green"],
                                "description": "red = negative/risk, orange = warning/improve, green = strength/leverage.",
                            },
                            "title": {
                                "type": "string",
                                "description": "Short imperative headline in Russian, ≤60 chars.",
                            },
                            "body": {
                                "type": "string",
                                "description": (
                                    "One sentence in Russian, ≤200 chars, citing concrete numbers or labels "
                                    "from the provided context. No fluff."
                                ),
                            },
                        },
                        "required": ["icon", "tone", "title", "body"],
                    },
                }
            },
            "required": ["items"],
        },
    }

    context = {
        "kpis": kpis,
        "top_problems": top_problems[:6],
        "top_praise": top_praise[:6],
    }

    try:
        response = await _call_anthropic(
            model=settings.claude_model,
            max_tokens=1024,
            temperature=0.3,
            system=(
                "You are a customer-experience consultant for a business owner. "
                "Write in Russian. Produce exactly 3 recommendations grounded in the provided KPIs "
                "and topic lists. Always cite concrete numbers (e.g. 'Негатив: 38%, ответов: 90%') "
                "or labels from top_problems / top_praise. Avoid generic advice."
            ),
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Context (JSON):\n" + json.dumps(context, ensure_ascii=False)
                    ),
                }
            ],
            tools=[tool_schema],
            tool_choice={"type": "tool", "name": "generate_recommendations"},
        )

        data = _extract_tool_input(response)
        if not data:
            return []

        items: list[RecommendationItem] = []
        for raw in data.get("items", []):
            try:
                items.append(RecommendationItem(**raw))
            except ValidationError as e:
                logger.warning(f"Skipping malformed recommendation: {raw}. Error: {e}")
        return items

    except Exception:
        logger.exception("Claude API failed during recommendations extraction")
        return []


def _format_reviews_with_dates(reviews: list[dict]) -> str:
    """Compact review listing including date marker so Claude can spot trends."""
    lines = []
    for r in reviews[:settings.max_reviews_to_analyze]:
        text = (r.get("text") or "").strip()
        if not text:
            continue
        rating = r.get("rating") or "?"
        date_raw = r.get("date_created")
        date_marker = "----"
        if date_raw is not None:
            try:
                date_marker = str(date_raw)[:7]  # YYYY-MM
            except Exception:
                pass
        lines.append(f"[{date_marker} | {rating}/5] {text}")
    return "\n".join(lines)


async def generate_topics_module(reviews: list[dict]) -> dict | None:
    """Build the data block for the 'Темы отзывов' module from raw reviews.

    Returns a dict with keys: topic_bars, top_positive, top_negative,
    frequent_phrases, fastest_growing_negative, strongest_positive.
    Returns None if API key is missing or the call fails.
    """
    if not client:
        logger.warning("Anthropic API key missing. Skipping topics_module generation.")
        return None
    if not reviews:
        return None

    reviews_text = _format_reviews_with_dates(reviews)

    tool_schema = {
        "name": "build_topics_module",
        "description": (
            "Cluster reviews into 5-8 dominant topics, return per-topic positive/negative "
            "mention counts, top positive and negative topics, characteristic verbatim phrases, "
            "the fastest-growing negative topic, and the strongest positive topic. "
            "Labels in Russian, lemma form, lowercase."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "topic_bars": {
                    "type": "array",
                    "minItems": 4,
                    "maxItems": 8,
                    "description": "Dominant discussion topics with separate positive and negative mention counts.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string"},
                            "positive": {"type": "integer", "minimum": 0},
                            "negative": {"type": "integer", "minimum": 0},
                        },
                        "required": ["label", "positive", "negative"],
                    },
                },
                "top_positive": {
                    "type": "array",
                    "minItems": 3,
                    "maxItems": 6,
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string"},
                            "sentiment": {"type": "string", "enum": ["pos", "neg", "neu"]},
                            "mentions": {"type": "integer", "minimum": 0},
                        },
                        "required": ["label", "sentiment", "mentions"],
                    },
                },
                "top_negative": {
                    "type": "array",
                    "minItems": 3,
                    "maxItems": 6,
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string"},
                            "sentiment": {"type": "string", "enum": ["pos", "neg", "neu"]},
                            "mentions": {"type": "integer", "minimum": 0},
                        },
                        "required": ["label", "sentiment", "mentions"],
                    },
                },
                "frequent_phrases": {
                    "type": "array",
                    "minItems": 4,
                    "maxItems": 10,
                    "items": {"type": "string"},
                    "description": "Short verbatim phrases (2-4 words each), drawn from the reviews.",
                },
                "fastest_growing_negative": {
                    "type": "object",
                    "properties": {
                        "label": {"type": "string"},
                        "description": {
                            "type": "string",
                            "description": "One sentence in Russian explaining the recent uptick (cite a number if obvious).",
                        },
                    },
                    "required": ["label", "description"],
                },
                "strongest_positive": {
                    "type": "object",
                    "properties": {
                        "label": {"type": "string"},
                        "description": {
                            "type": "string",
                            "description": "One sentence in Russian explaining why this is the standout positive theme.",
                        },
                    },
                    "required": ["label", "description"],
                },
            },
            "required": [
                "topic_bars",
                "top_positive",
                "top_negative",
                "frequent_phrases",
                "fastest_growing_negative",
                "strongest_positive",
            ],
        },
    }

    try:
        response = await _call_anthropic(
            model=settings.claude_model,
            max_tokens=2500,
            temperature=0.2,
            system=(
                "You are a strict data analyst clustering customer reviews into topics. "
                "Use Russian labels in lemma (dictionary) form, lowercase. "
                "Each review is prefixed by [YYYY-MM | rating/5]. "
                "The fastest-growing negative topic is the negative cluster whose share has risen "
                "most in the most recent month vs the prior period. "
                "The strongest positive topic is the cluster most often co-occurring with 5/5 ratings. "
                "Quote phrases verbatim from the reviews — never invent."
            ),
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Build the topics module from these reviews:\n\n" + reviews_text
                    ),
                }
            ],
            tools=[tool_schema],
            tool_choice={"type": "tool", "name": "build_topics_module"},
        )

        data = _extract_tool_input(response)
        if not data:
            return None

        topic_bars = []
        for it in data.get("topic_bars", []):
            if not isinstance(it, dict):
                continue
            try:
                topic_bars.append(TopicBarItem(**it).model_dump())
            except (ValidationError, TypeError) as e:
                logger.warning(f"Skipping malformed topic_bar: {it}. Error: {e}")

        top_positive = []
        for it in data.get("top_positive", []):
            if not isinstance(it, dict):
                continue
            try:
                top_positive.append(TopicListItem(**it).model_dump())
            except (ValidationError, TypeError) as e:
                logger.warning(f"Skipping malformed top_positive: {it}. Error: {e}")

        top_negative = []
        for it in data.get("top_negative", []):
            if not isinstance(it, dict):
                continue
            try:
                top_negative.append(TopicListItem(**it).model_dump())
            except (ValidationError, TypeError) as e:
                logger.warning(f"Skipping malformed top_negative: {it}. Error: {e}")

        frequent_phrases = [
            p.strip() for p in data.get("frequent_phrases", []) if isinstance(p, str) and p.strip()
        ]

        fgn_raw = data.get("fastest_growing_negative")
        fgn = None
        if isinstance(fgn_raw, dict):
            try:
                fgn = TopicTrend(**fgn_raw).model_dump()
            except ValidationError as e:
                logger.warning(f"Skipping malformed fastest_growing_negative: {fgn_raw}. Error: {e}")

        sp_raw = data.get("strongest_positive")
        sp = None
        if isinstance(sp_raw, dict):
            try:
                sp = TopicTrend(**sp_raw).model_dump()
            except ValidationError as e:
                logger.warning(f"Skipping malformed strongest_positive: {sp_raw}. Error: {e}")

        return {
            "topic_bars": topic_bars,
            "top_positive": top_positive,
            "top_negative": top_negative,
            "frequent_phrases": frequent_phrases,
            "fastest_growing_negative": fgn,
            "strongest_positive": sp,
        }

    except Exception:
        logger.exception("Claude API failed during topics_module extraction")
        return None


async def generate_reply_templates(
    top_problems: list[dict],
    top_praise: list[dict],
) -> list[ReplyTemplate]:
    """Generate exactly 3 reply templates in Russian, grounded in the task's
    actual top problems and top praise (so language matches the real reviews)."""
    if not client:
        logger.warning("Anthropic API key missing. Returning empty reply templates.")
        return []

    if not top_problems and not top_praise:
        return []

    tool_schema = {
        "name": "generate_reply_templates",
        "description": (
            "Produce exactly 3 reply templates in Russian a customer-service team "
            "can paste under reviews: one for the dominant negative complaint, "
            "one for positive reviews, one for the second-most-common negative theme. "
            "Each template must read like a brand reply: polite, accountable, brand-neutral."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "minItems": 3,
                    "maxItems": 3,
                    "items": {
                        "type": "object",
                        "properties": {
                            "title": {
                                "type": "string",
                                "description": "Short Russian label, e.g. 'Ответ на жалобу по сервису'.",
                            },
                            "text": {
                                "type": "string",
                                "description": (
                                    "Full reply text in Russian, 2-4 sentences, "
                                    "polite, no emoji, no placeholders like {name}."
                                ),
                            },
                        },
                        "required": ["title", "text"],
                    },
                }
            },
            "required": ["items"],
        },
    }

    context = {
        "top_problems": top_problems[:6],
        "top_praise": top_praise[:6],
    }

    try:
        response = await _call_anthropic(
            model=settings.claude_model,
            max_tokens=1500,
            temperature=0.4,
            system=(
                "You write reply templates for a Russian-speaking customer-service team. "
                "Tailor each template to the actual top complaints/praise provided. "
                "Tone: polite, professional, accountable. No emoji. No placeholders. "
                "The first template addresses the dominant negative theme, the second "
                "addresses positive reviews, the third addresses the second-most-common negative theme."
            ),
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Context (JSON):\n" + json.dumps(context, ensure_ascii=False)
                    ),
                }
            ],
            tools=[tool_schema],
            tool_choice={"type": "tool", "name": "generate_reply_templates"},
        )

        data = _extract_tool_input(response)
        if not data:
            return []

        items: list[ReplyTemplate] = []
        for raw in data.get("items", []):
            try:
                items.append(ReplyTemplate(**raw))
            except ValidationError as e:
                logger.warning(f"Skipping malformed reply template: {raw}. Error: {e}")
        return items

    except Exception:
        logger.exception("Claude API failed during reply templates generation")
        return []