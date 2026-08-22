"""CLI wrapper around agent.core.ask.

Usage:
    python -m agent.cli --provider claude --db data/business.duckdb --question "..."
    python -m agent.cli --provider kimi --model kimi-k2-turbo-preview --db data/business.duckdb --question "..." --verbose
"""

import argparse
import sys

from . import core, providers


def main():
    parser = argparse.ArgumentParser(description="Ask the analytics agent a business question.")
    parser.add_argument("--provider", choices=list(providers.PROVIDERS), required=True)
    parser.add_argument("--db", default="data/business.duckdb", help="Path to the DuckDB file.")
    parser.add_argument("--question", required=True)
    parser.add_argument("--model", default=None, help="Override the provider's default model.")
    parser.add_argument("--system-prompt", default=None, help="Override the default neutral system prompt.")
    parser.add_argument("--max-turns", type=int, default=8)
    parser.add_argument(
        "--reasoning-effort",
        default=None,
        choices=["low", "high", "max"],
        help="Kimi k3/k3-256k only; overrides the provider default (low).",
    )
    parser.add_argument("--verbose", action="store_true", help="Print SQL queries the agent ran.")
    args = parser.parse_args()

    try:
        result = core.ask(
            question=args.question,
            db_path=args.db,
            provider=args.provider,
            model=args.model,
            max_turns=args.max_turns,
            system_prompt=args.system_prompt,
            reasoning_effort=args.reasoning_effort,
        )
    except providers.ProviderConfigError as e:
        print(f"Config error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.verbose:
        print(f"--- {result.turns} turn(s), {len(result.queries)} SQL quer{'y' if len(result.queries) == 1 else 'ies'} ---")
        for i, q in enumerate(result.queries, 1):
            print(f"[{i}] {q}")
        print(
            f"cache: {result.cache_read_tokens} tokens read from cache, "
            f"{result.cache_creation_tokens} tokens written to cache "
            "(Kimi's usage reporting may always show 0 here even when writes occur)"
        )
        print("---")

    print(result.answer)


if __name__ == "__main__":
    main()
