"""Scores an agent's answers against benchmark/questions.yaml.

Usage:
    python -m grader.grade --answers path/to/answers.json [--questions benchmark/questions.yaml] [--out results.json]

Accepts two answers.json shapes:
  - flat (e.g. grader/fixtures/*.json): {"c01": {"answer": "..."}, "i01": {"answer": "..."}, ...}
  - metadata-wrapped (what benchmark/run.py writes): {"metadata": {...}, "answers": {"c01": {"answer": "..."}, ...}}
"""

import argparse
import json

import yaml

from . import rubric


def load_questions(path: str) -> list[dict]:
    with open(path) as f:
        return yaml.safe_load(f)


def load_answers(path: str) -> tuple[dict, dict | None]:
    """Returns (answers, metadata). metadata is None for flat (unwrapped) answer files."""
    with open(path) as f:
        data = json.load(f)
    if "answers" in data and "metadata" in data:
        return data["answers"], data["metadata"]
    return data, None


def grade_one(question: dict, answer_text: str | None) -> dict:
    if answer_text is None:
        return {
            "id": question["id"],
            "category": question["category"],
            "requires_flag": question["requires_flag"],
            "correctness": {"status": "no_answer", "detail": "No answer provided."},
            "awareness": {"flagged": False, "matched_issue_keywords": {}, "matched_generic_keywords": [], "matched_refusal_keywords": []},
        }

    correctness = rubric.score_correctness(question, answer_text)
    awareness = rubric.score_awareness(question, answer_text)
    return {
        "id": question["id"],
        "category": question["category"],
        "requires_flag": question["requires_flag"],
        "correctness": correctness,
        "awareness": awareness,
    }


def summarize(scorecards: list[dict]) -> dict:
    gradable = [s for s in scorecards if s["correctness"]["status"] in ("match", "mismatch")]
    correct = [s for s in gradable if s["correctness"]["status"] == "match"]

    requires_flag = [s for s in scorecards if s["requires_flag"]]
    flagged_correctly = [s for s in requires_flag if s["awareness"]["flagged"]]

    no_flag_expected = [s for s in scorecards if not s["requires_flag"]]
    false_positives = [s for s in no_flag_expected if s["awareness"]["flagged"]]

    no_answer = [s for s in scorecards if s["correctness"]["status"] == "no_answer"]

    return {
        "total_questions": len(scorecards),
        "no_answer_count": len(no_answer),
        "accuracy": {
            "gradable_questions": len(gradable),
            "correct": len(correct),
            "rate": round(len(correct) / len(gradable), 3) if gradable else None,
        },
        "awareness_recall": {
            "requires_flag_questions": len(requires_flag),
            "flagged": len(flagged_correctly),
            "rate": round(len(flagged_correctly) / len(requires_flag), 3) if requires_flag else None,
        },
        "false_positive_rate": {
            "clean_questions": len(no_flag_expected),
            "unnecessarily_flagged": len(false_positives),
            "rate": round(len(false_positives) / len(no_flag_expected), 3) if no_flag_expected else None,
        },
    }


def main():
    parser = argparse.ArgumentParser(description="Grade an agent's answers against the benchmark.")
    parser.add_argument("--answers", required=True, help="Path to a JSON file of {question_id: {answer: ...}}.")
    parser.add_argument("--questions", default="benchmark/questions.yaml")
    parser.add_argument("--out", default=None, help="Optional path to write full per-question results as JSON.")
    args = parser.parse_args()

    questions = load_questions(args.questions)
    answers, metadata = load_answers(args.answers)

    if metadata:
        print("--- run metadata ---")
        for k, v in metadata.items():
            if k == "question_ids":
                continue
            print(f"{k}: {v}")
        print()

    scorecards = []
    for q in questions:
        answer_entry = answers.get(q["id"])
        answer_text = answer_entry.get("answer") if answer_entry else None
        scorecards.append(grade_one(q, answer_text))

    summary = summarize(scorecards)

    print(json.dumps(summary, indent=2))
    print()
    for s in scorecards:
        flag_note = ""
        if s["requires_flag"]:
            flag_note = " [FLAGGED]" if s["awareness"]["flagged"] else " [NOT FLAGGED - should have been]"
        elif s["awareness"]["flagged"]:
            flag_note = " [FLAGGED - unnecessarily]"
        print(f"{s['id']:5s} {s['category']:16s} correctness={s['correctness']['status']:10s}{flag_note}")

    if args.out:
        with open(args.out, "w") as f:
            json.dump({"metadata": metadata, "summary": summary, "scorecards": scorecards}, f, indent=2)
        print(f"\nWrote full results to {args.out}")


if __name__ == "__main__":
    main()
