"""Drives the agent-under-test through benchmark/questions.yaml and (optionally) grades it.

Calls agent.core.ask in-process for each question - no shelling out to a CLI needed, since
agent/ already implements the tool-use loop directly against the anthropic SDK.

Usage:
    python -m benchmark.run --provider kimi --db data/business.duckdb
    python -m benchmark.run --provider claude --limit 5 --grade
    python -m benchmark.run --provider kimi --question-id i05 --verbose
"""

import argparse
import json
import os
import time

import yaml

from agent import core, providers
from grader import grade as grade_module

QUESTIONS_PATH = os.path.join(os.path.dirname(__file__), "questions.yaml")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


def load_questions(path: str) -> list[dict]:
    with open(path) as f:
        return yaml.safe_load(f)


def run(
    questions: list[dict],
    db_path: str,
    provider: str,
    model: str | None = None,
    max_turns: int = 8,
    verbose: bool = False,
) -> dict:
    results = {}
    for i, q in enumerate(questions, 1):
        print(f"[{i}/{len(questions)}] {q['id']}: {q['question']}")
        try:
            agent_result = core.ask(
                question=q["question"],
                db_path=db_path,
                provider=provider,
                model=model,
                max_turns=max_turns,
            )
            answer_text = agent_result.answer
            if verbose:
                print(f"  ({agent_result.turns} turns, {len(agent_result.queries)} SQL queries)")
                print(f"  -> {answer_text[:200]}{'...' if len(answer_text) > 200 else ''}")
        except providers.ProviderConfigError:
            raise
        except Exception as e:
            answer_text = None
            print(f"  ERROR: {e}")

        results[q["id"]] = {
            "answer": answer_text,
            "queries": agent_result.queries if answer_text is not None else [],
            "turns": agent_result.turns if answer_text is not None else 0,
        }
    return results


def main():
    parser = argparse.ArgumentParser(description="Run the benchmark question set against the agent-under-test.")
    parser.add_argument("--provider", choices=list(providers.PROVIDERS), required=True)
    parser.add_argument("--db", default="data/business.duckdb")
    parser.add_argument("--questions", default=QUESTIONS_PATH)
    parser.add_argument("--model", default=None)
    parser.add_argument("--max-turns", type=int, default=8)
    parser.add_argument("--limit", type=int, default=None, help="Only run the first N questions.")
    parser.add_argument("--question-id", default=None, help="Run a single question by id.")
    parser.add_argument("--out", default=None, help="Path to write raw answers JSON (default: benchmark/results/<run_id>.json).")
    parser.add_argument("--grade", action="store_true", help="Chain straight into grader.grade after running.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    questions = load_questions(args.questions)
    if args.question_id:
        questions = [q for q in questions if q["id"] == args.question_id]
        if not questions:
            raise SystemExit(f"No question with id '{args.question_id}' found.")
    elif args.limit:
        questions = questions[: args.limit]

    results = run(
        questions=questions,
        db_path=args.db,
        provider=args.provider,
        model=args.model,
        max_turns=args.max_turns,
        verbose=args.verbose,
    )

    os.makedirs(RESULTS_DIR, exist_ok=True)
    run_id = time.strftime("%Y%m%d-%H%M%S")
    out_path = args.out or os.path.join(RESULTS_DIR, f"{run_id}.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nWrote {len(results)} answers to {out_path}")

    if args.grade:
        print()
        all_questions = load_questions(args.questions)
        scorecards = [
            grade_module.grade_one(q, results.get(q["id"], {}).get("answer"))
            for q in all_questions
            if q["id"] in results
        ]
        summary = grade_module.summarize(scorecards)
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
