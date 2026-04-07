"""Multi-provider LLM client for Otak pipeline.

Drop-in replacement for claude_code.py with direct API calls to:
- Anthropic (Claude Sonnet, Haiku)
- Google (Gemini 3 Flash, 2.5 Flash, 2.5 Pro)
- OpenAI (GPT-4.1-mini, GPT-4.1-nano)

Key benefits over claude CLI wrapper:
- 5-30x cheaper (direct API vs CLI overhead)
- asyncio for massive parallelism (not limited to 4 subprocesses)
- Gemini models 100-300x cheaper than Sonnet with comparable quality
- Gemini structured output guarantees schema compliance

Usage:
    from llm_providers import generate, generate_parallel, MODELS

    # Single call
    result, meta = await generate(
        prompt="...", system_prompt="...", json_schema={...},
        model="gemini3-flash"  # or "sonnet", "haiku", "gpt41-mini", etc.
    )

    # Parallel calls
    results = await generate_parallel([
        {"prompt": "...", "system_prompt": "...", "json_schema": {...}, "model": "gemini3-flash"},
        {"prompt": "...", "system_prompt": "...", "json_schema": {...}, "model": "sonnet"},
    ])
"""
import asyncio
import json
import logging
import os
import random
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import anthropic
from google import genai
from google.genai import types as genai_types
from openai import AsyncOpenAI

log = logging.getLogger(__name__)

# Cost logging (optional)
try:
    from limbic.cerebellum.cost_log import cost_log as _cost_log
except ImportError:
    _cost_log = None

# ── Model Registry ──

MODELS = {
    # Anthropic
    "sonnet": {"provider": "anthropic", "id": "claude-sonnet-4-20250514", "input_price": 3.0, "output_price": 15.0},
    "haiku": {"provider": "anthropic", "id": "claude-haiku-4-5-20251001", "input_price": 1.0, "output_price": 5.0},
    # Google Gemini
    "gemini3-flash": {"provider": "gemini", "id": "gemini-3-flash-preview", "input_price": 0.10, "output_price": 0.40},
    "gemini25-flash": {"provider": "gemini", "id": "gemini-2.5-flash", "input_price": 0.15, "output_price": 0.60},
    "gemini25-pro": {"provider": "gemini", "id": "gemini-2.5-pro", "input_price": 1.25, "output_price": 10.0},
    # OpenAI
    "gpt41-mini": {"provider": "openai", "id": "gpt-4.1-mini", "input_price": 0.40, "output_price": 1.60},
    "gpt41-nano": {"provider": "openai", "id": "gpt-4.1-nano", "input_price": 0.10, "output_price": 0.40},
}

# Default model for each pipeline phase (optimized based on experiments)
PHASE_DEFAULTS = {
    "extract": "gemini3-flash",    # 4.0/5 quality, $0.001/call (was: sonnet via CLI $0.27)
    "place": "gemini3-flash",      # 96% agreement with Sonnet, $0.001/call (was: sonnet $0.31)
    "dedup": "gemini3-flash",      # Simple classification task, $0.0005/call (was: haiku $0.01)
    "links": "gemini3-flash",      # Simple classification task, $0.001/call (was: haiku $0.09)
    "balance": "gemini3-flash",    # Moderate task, $0.001/call (was: sonnet $0.27)
    "grade": "sonnet",             # Quality grading needs best model
}


def _calc_cost(model_key: str, input_tokens: int, output_tokens: int) -> float:
    m = MODELS[model_key]
    return (input_tokens * m["input_price"] + output_tokens * m["output_price"]) / 1_000_000


def _strip_gemini_schema(schema: dict) -> dict:
    """Make schema Gemini-compatible: remove null types, unsupported keywords."""
    if not isinstance(schema, dict):
        return schema
    result = {}
    for k, v in schema.items():
        if k == "type" and isinstance(v, list):
            # ["string", "null"] -> "string"
            non_null = [t for t in v if t != "null"]
            result[k] = non_null[0] if non_null else "string"
        elif isinstance(v, dict):
            result[k] = _strip_gemini_schema(v)
        elif isinstance(v, list):
            result[k] = [_strip_gemini_schema(item) if isinstance(item, dict) else item for item in v]
        else:
            result[k] = v
    return result


# ── Provider Implementations ──

async def _call_anthropic(model_id: str, system_prompt: str, user_prompt: str,
                          json_schema: dict, max_tokens: int, **kwargs) -> dict:
    client = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_KEY"])
    start = time.time()
    try:
        response = await client.messages.create(
            model=model_id,
            max_tokens=max_tokens,
            system=system_prompt + "\n\nRespond with valid JSON only. No text before or after.",
            messages=[
                {"role": "user", "content": user_prompt + "\n\nRespond with valid JSON."},
                {"role": "assistant", "content": "{"},
            ],
        )
        raw = "{" + response.content[0].text
        result = json.loads(raw)
        return {
            "result": result,
            "input_tokens": response.usage.input_tokens,
            "output_tokens": response.usage.output_tokens,
            "duration_s": time.time() - start,
        }
    except json.JSONDecodeError:
        # Try to salvage: find last valid JSON closing brace
        raw = "{" + response.content[0].text
        # Truncate at last }
        idx = raw.rfind("}")
        if idx > 0:
            try:
                result = json.loads(raw[:idx+1])
                return {
                    "result": result,
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                    "duration_s": time.time() - start,
                    "warning": "truncated JSON",
                }
            except json.JSONDecodeError:
                pass
        raise
    finally:
        await client.close()


async def _call_gemini(model_id: str, system_prompt: str, user_prompt: str,
                       json_schema: dict, max_tokens: int,
                       thinking_budget: int | None = None) -> dict:
    client = genai.Client(api_key=os.environ["GEMINI_KEY"])
    start = time.time()
    clean_schema = _strip_gemini_schema(json_schema)
    config_kwargs = dict(
        system_instruction=system_prompt,
        response_mime_type="application/json",
        response_schema=clean_schema,
        max_output_tokens=max_tokens,
    )
    if thinking_budget is not None:
        config_kwargs["thinking_config"] = genai_types.ThinkingConfig(
            thinking_budget=thinking_budget
        )
    response = await client.aio.models.generate_content(
        model=model_id,
        contents=user_prompt,
        config=genai_types.GenerateContentConfig(**config_kwargs),
    )
    raw = response.text
    result = json.loads(raw)
    thoughts = response.usage_metadata.thoughts_token_count or 0
    return {
        "result": result,
        "input_tokens": response.usage_metadata.prompt_token_count or 0,
        "output_tokens": response.usage_metadata.candidates_token_count or 0,
        "thoughts_tokens": thoughts,
        "duration_s": time.time() - start,
    }


async def _call_openai(model_id: str, system_prompt: str, user_prompt: str,
                       json_schema: dict, max_tokens: int, **kwargs) -> dict:
    client = AsyncOpenAI(api_key=os.environ["OPENAI_KEY"])
    start = time.time()
    try:
        response = await client.chat.completions.create(
            model=model_id,
            messages=[
                {"role": "system", "content": system_prompt + "\nRespond with valid JSON only."},
                {"role": "user", "content": user_prompt + "\nReturn JSON."},
            ],
            response_format={"type": "json_object"},
            max_completion_tokens=max_tokens,
        )
        raw = response.choices[0].message.content
        result = json.loads(raw)
        return {
            "result": result,
            "input_tokens": response.usage.prompt_tokens,
            "output_tokens": response.usage.completion_tokens,
            "duration_s": time.time() - start,
        }
    finally:
        await client.close()


_PROVIDERS = {
    "anthropic": _call_anthropic,
    "gemini": _call_gemini,
    "openai": _call_openai,
}

MAX_RETRIES = 3
BACKOFF_BASE = 2  # seconds: 2s, 8s, 32s


def _is_retryable(e: Exception) -> bool:
    """Check if an exception is retryable (rate limits, server errors, timeouts)."""
    err_str = str(e).lower()
    err_type = type(e).__name__.lower()

    # Rate limits and server errors
    if any(code in err_str for code in ("429", "500", "503", "resource_exhausted",
                                         "rate limit", "overloaded", "quota")):
        return True
    # Connection/timeout errors
    if any(k in err_type for k in ("timeout", "connection", "transport")):
        return True
    if any(k in err_str for k in ("timeout", "connection reset", "connection refused",
                                    "temporarily unavailable", "service unavailable")):
        return True
    return False


def _get_retry_after(e: Exception) -> float | None:
    """Extract Retry-After value from exception if available."""
    if hasattr(e, "response") and hasattr(e.response, "headers"):
        ra = e.response.headers.get("Retry-After") or e.response.headers.get("retry-after")
        if ra:
            try:
                return float(ra)
            except (ValueError, TypeError):
                pass
    return None


# ── Public API ──

async def generate(
    prompt: str,
    system_prompt: str,
    json_schema: dict,
    model: str = "gemini3-flash",
    max_tokens: int = 8192,
    phase: str | None = None,
    thinking_budget: int | None = None,
) -> tuple[dict, dict]:
    """Generate structured output from any provider.

    Args:
        prompt: User prompt
        system_prompt: System prompt
        json_schema: JSON schema for structured output
        model: Model key from MODELS dict, or None to use phase default
        max_tokens: Max output tokens
        phase: Pipeline phase name (for default model selection)
        thinking_budget: Gemini thinking token budget (0=disable, None=default)

    Returns:
        (result_dict, metadata_dict) compatible with existing pipeline
    """
    if model is None and phase:
        model = PHASE_DEFAULTS.get(phase, "gemini3-flash")

    # Fallback chain: retry with a more reliable model on JSON parse errors
    FALLBACK = {"gemini3-flash": "gemini25-flash"}

    model_info = MODELS[model]
    provider_fn = _PROVIDERS[model_info["provider"]]

    start = time.time()
    # Pass thinking_budget only to Gemini provider
    extra_kwargs = {}
    if thinking_budget is not None and model_info["provider"] == "gemini":
        extra_kwargs["thinking_budget"] = thinking_budget

    try:
        # Retry with exponential backoff for transient errors
        raw = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                raw = await provider_fn(model_info["id"], system_prompt, prompt, json_schema, max_tokens, **extra_kwargs)
                break
            except Exception as e:
                if _is_retryable(e) and attempt < MAX_RETRIES:
                    retry_after = _get_retry_after(e)
                    delay = retry_after if retry_after else BACKOFF_BASE * (4 ** attempt) + random.uniform(0, 1)
                    log.warning("Retryable error (attempt %d/%d), waiting %.1fs: %s",
                               attempt + 1, MAX_RETRIES, delay, str(e)[:100])
                    await asyncio.sleep(delay)
                    continue
                raise

        cost = _calc_cost(model, raw["input_tokens"], raw["output_tokens"])

        metadata = {
            "total_cost_usd": cost,
            "input_tokens": raw["input_tokens"],
            "output_tokens": raw["output_tokens"],
            "duration_s": raw["duration_s"],
            "model": model,
            "model_id": model_info["id"],
            "provider": model_info["provider"],
        }
        if raw.get("warning"):
            metadata["warning"] = raw["warning"]

        log.info("generate: model=%s provider=%s duration=%.1fs cost=$%.4f",
                 model, model_info["provider"], raw["duration_s"], cost)

        if _cost_log:
            try:
                _cost_log.log(project="otak", model=f"{model_info['provider']}/{model_info['id']}",
                              prompt_tokens=raw["input_tokens"], completion_tokens=raw["output_tokens"],
                              cost_usd=cost)
            except Exception:
                pass

        return raw["result"], metadata

    except (json.JSONDecodeError, RuntimeError) as e:
        fallback_model = FALLBACK.get(model)
        if fallback_model and "json" not in str(type(e).__name__).lower():
            # For RuntimeError, only retry if it wraps a JSON error
            err_str = str(e).lower()
            if not any(k in err_str for k in ("json", "unterminated", "expecting value", "decode")):
                fallback_model = None

        if fallback_model:
            log.warning("generate: %s failed with JSON error, retrying with %s: %s",
                        model, fallback_model, str(e)[:100])
            fb_info = MODELS[fallback_model]
            fb_fn = _PROVIDERS[fb_info["provider"]]
            try:
                fb_kwargs = {}
                if thinking_budget is not None and fb_info["provider"] == "gemini":
                    fb_kwargs["thinking_budget"] = thinking_budget
                raw = await fb_fn(fb_info["id"], system_prompt, prompt, json_schema, max_tokens, **fb_kwargs)
                cost = _calc_cost(fallback_model, raw["input_tokens"], raw["output_tokens"])
                metadata = {
                    "total_cost_usd": cost,
                    "input_tokens": raw["input_tokens"],
                    "output_tokens": raw["output_tokens"],
                    "duration_s": time.time() - start,
                    "model": fallback_model,
                    "model_id": fb_info["id"],
                    "provider": fb_info["provider"],
                    "fallback_from": model,
                }
                log.info("generate: fallback model=%s duration=%.1fs cost=$%.4f",
                         fallback_model, raw["duration_s"], cost)
                if _cost_log:
                    try:
                        _cost_log.log(project="otak", model=f"{fb_info['provider']}/{fb_info['id']}",
                                      prompt_tokens=raw["input_tokens"], completion_tokens=raw["output_tokens"],
                                      cost_usd=cost)
                    except Exception:
                        pass
                return raw["result"], metadata
            except Exception as e2:
                log.error("generate fallback also failed: model=%s error=%s", fallback_model, e2)
                raise RuntimeError(f"LLM call failed ({model}→{fallback_model}): {e2}") from e2
        else:
            log.error("generate failed: model=%s error=%s", model, e)
            raise RuntimeError(f"LLM call failed ({model}): {e}") from e

    except Exception as e:
        elapsed = time.time() - start
        log.error("generate failed: model=%s error=%s", model, e)
        raise RuntimeError(f"LLM call failed ({model}): {e}") from e


async def generate_parallel(
    tasks: list[dict],
    max_concurrent: int = 20,
) -> list[tuple[dict | None, dict]]:
    """Run multiple generate() calls in parallel.

    Args:
        tasks: List of dicts with keys: prompt, system_prompt, json_schema, model, tag
        max_concurrent: Maximum concurrent API calls (default 20)

    Returns:
        List of (result_or_None, metadata) in same order as input.
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _run(task: dict, idx: int):
        async with semaphore:
            try:
                result, meta = await generate(
                    prompt=task["prompt"],
                    system_prompt=task["system_prompt"],
                    json_schema=task["json_schema"],
                    model=task.get("model", "gemini3-flash"),
                    max_tokens=task.get("max_tokens", 8192),
                    phase=task.get("phase"),
                    thinking_budget=task.get("thinking_budget"),
                )
                meta["tag"] = task.get("tag", f"task-{idx}")
                return (result, meta)
            except Exception as e:
                return (None, {
                    "error": str(e),
                    "model": task.get("model", "gemini3-flash"),
                    "tag": task.get("tag", f"task-{idx}"),
                })

    coros = [_run(t, i) for i, t in enumerate(tasks)]
    return await asyncio.gather(*coros)


# ── Sync wrapper for pipeline integration ──

def generate_sync(
    prompt: str,
    system_prompt: str,
    json_schema: dict,
    model: str = "gemini3-flash",
    max_tokens: int = 8192,
    phase: str | None = None,
    thinking_budget: int | None = None,
) -> tuple[dict, dict]:
    """Synchronous wrapper for generate(). For use in existing sync pipeline code."""
    return asyncio.run(generate(prompt, system_prompt, json_schema, model, max_tokens, phase, thinking_budget))


def generate_parallel_sync(
    tasks: list[dict],
    max_concurrent: int = 20,
) -> list[tuple[dict | None, dict]]:
    """Synchronous wrapper for generate_parallel()."""
    return asyncio.run(generate_parallel(tasks, max_concurrent))
