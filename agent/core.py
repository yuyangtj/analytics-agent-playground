"""The agentic loop: ask a business question, let the agent query the DB via tool-use, get an answer."""

import json
from dataclasses import dataclass, field

import anthropic
import duckdb

from . import providers, tools

DEFAULT_SYSTEM_PROMPT = (
    "You are an analytics assistant. Answer the user's question about the business "
    "using the `run_sql` tool to query the database as needed. Give a clear, direct "
    "final answer."
)


@dataclass
class AgentResult:
    answer: str
    queries: list[str] = field(default_factory=list)
    turns: int = 0
    transcript: list[dict] = field(default_factory=list)
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0


def _cache_block(text: str) -> dict:
    return {"type": "text", "text": text, "cache_control": {"type": "ephemeral"}}


def _with_cache_breakpoint(messages: list[dict]) -> list[dict]:
    """Marks the last content block of the last message as a cache breakpoint.

    Prompt caching works on a growing prefix: everything up to and including a
    cache_control block gets cached. Each turn we move the breakpoint to the new
    last message, so the (unchanged) prefix from prior turns is served from cache
    and only the newly-appended suffix is cached fresh.
    """
    if not messages:
        return messages
    msgs = list(messages)
    last = msgs[-1]
    content = last["content"]

    if isinstance(content, str):
        new_content = [_cache_block(content)]
    else:
        new_content = [
            block if isinstance(block, dict) else block.model_dump()
            for block in content
        ]
        new_content[-1] = {**new_content[-1], "cache_control": {"type": "ephemeral"}}

    msgs[-1] = {**last, "content": new_content}
    return msgs


def ask(
    question: str,
    db_path: str,
    provider: str,
    model: str | None = None,
    max_turns: int = 8,
    system_prompt: str | None = None,
    reasoning_effort: str | None = None,
) -> AgentResult:
    api_key, base_url, resolved_model, default_reasoning_effort = providers.resolve(provider, model)
    resolved_reasoning_effort = reasoning_effort or default_reasoning_effort
    client = anthropic.Anthropic(api_key=api_key, base_url=base_url) if base_url else anthropic.Anthropic(api_key=api_key)

    # Kimi's Anthropic-compatible endpoint accepts reasoning_effort as an extra body field
    # (not part of the standard Anthropic Messages API); exact placement per Kimi Code docs
    # is underspecified, so this is our best-effort mapping - verify if Kimi changes behavior.
    extra_body = {"reasoning_effort": resolved_reasoning_effort} if resolved_reasoning_effort else None

    con = duckdb.connect(db_path, read_only=True)

    system = system_prompt or DEFAULT_SYSTEM_PROMPT
    # system prompt and tool definitions are identical every turn, so they're always
    # worth caching (cheap, and covers the common case where the run never grows
    # past a couple of turns).
    cached_system = [_cache_block(system)]
    cached_tools = [{**tools.RUN_SQL_TOOL, "cache_control": {"type": "ephemeral"}}]

    messages = [{"role": "user", "content": question}]
    queries_run: list[str] = []
    cache_read_tokens = 0
    cache_creation_tokens = 0

    try:
        for turn in range(1, max_turns + 1):
            response = client.messages.create(
                model=resolved_model,
                max_tokens=2048,
                system=cached_system,
                tools=cached_tools,
                messages=_with_cache_breakpoint(messages),
                extra_body=extra_body,
            )

            cache_read_tokens += getattr(response.usage, "cache_read_input_tokens", 0) or 0
            # Kimi's Anthropic-compat endpoint has been observed to always report 0 here even
            # when a cache write demonstrably just happened (confirmed via cache_read_input_tokens
            # on the next call) - treat cache_creation_tokens as unreliable on that provider.
            cache_creation_tokens += getattr(response.usage, "cache_creation_input_tokens", 0) or 0

            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason != "tool_use":
                final_text = "".join(
                    block.text for block in response.content if block.type == "text"
                )
                return AgentResult(
                    answer=final_text,
                    queries=queries_run,
                    turns=turn,
                    transcript=messages,
                    cache_read_tokens=cache_read_tokens,
                    cache_creation_tokens=cache_creation_tokens,
                )

            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                query = block.input.get("query", "")
                queries_run.append(query)
                result = tools.run_sql(con, query)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result),
                    }
                )

            messages.append({"role": "user", "content": tool_results})

        return AgentResult(
            answer="[max_turns reached without a final answer]",
            queries=queries_run,
            turns=max_turns,
            transcript=messages,
            cache_read_tokens=cache_read_tokens,
            cache_creation_tokens=cache_creation_tokens,
        )
    finally:
        con.close()
