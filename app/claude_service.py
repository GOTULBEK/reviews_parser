import logging
from typing import Any
from anthropic import AsyncAnthropic
from .config import settings
from .schemas import InsightItem, ProblemItem, PriorityItem, TopMention

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
    return "\n".join(lines)


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
        response = await client.messages.create(
            model=settings.claude_model,
            max_tokens=2048,
            temperature=0.2,
            system="You are a strict data analyst. Group similar complaints into clear, distinct problems. Do not invent quotes; extract them verbatim.",
            messages=[{"role": "user", "content": f"Analyze these reviews and extract the core problems:\n\n{reviews_text}"}],
            tools=[tool_schema],
            tool_choice={"type": "tool", "name": "extract_problems"}
        )

        # Extract the tool use block
        tool_use = next(block for block in response.content if block.type == "tool_use")
        
        # Parse into Pydantic models
        problems = []
        for item in tool_use.input.get("items", []):
            problems.append(ProblemItem(**item))
        return problems

    except Exception as e:
        logger.exception("Claude API failed during problem extraction")
        return []


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
        response = await client.messages.create(
            model=settings.claude_model,
            max_tokens=2500,
            temperature=0.3,
            system="You are an expert operations consultant. Read these reviews and generate a realistic action plan and unique business insights.",
            messages=[{"role": "user", "content": f"Analyze these reviews:\n\n{reviews_text}"}],
            tools=[tool_schema],
            tool_choice={"type": "tool", "name": "extract_actions_and_insights"}
        )

        tool_use = next(block for block in response.content if block.type == "tool_use")
        data = tool_use.input
        
        priorities = [PriorityItem(**p) for p in data.get("priorities", [])]
        insights = [InsightItem(**i) for i in data.get("insights", [])]
        
        return priorities, insights

    except Exception as e:
        logger.exception("Claude API failed during action extraction")
        return [], []


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
        response = await client.messages.create(
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

        tool_use = next(block for block in response.content if block.type == "tool_use")
        data = tool_use.input

        top_problems = [TopMention(**item) for item in data.get("top_problems", [])]
        top_praise = [TopMention(**item) for item in data.get("top_praise", [])]
        return top_problems, top_praise

    except Exception:
        logger.exception("Claude API failed during top mentions extraction")
        return [], []