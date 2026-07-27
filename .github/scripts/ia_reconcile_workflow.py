#!/usr/bin/env python3
"""Validate IA reconciliation workflow inputs and render its Step Summary."""

import argparse
import json
import os
from pathlib import Path
import re
import sys
from uuid import UUID


LOG_PATH = Path("internet-archive-reconciliation.log")
CONTEXT_PATH = Path("reconciliation-run-context.json")
WORK_IDS_PATH = Path("normalised-work-ids.txt")
REPORT_PATH = Path("internet-archive-reconciliation.json")
STATUS_PATH = Path("reconciliation-exit-status.txt")

# Dry-run only inspects item state, so it may discover a large candidate batch.
DRY_RUN_MAX_BATCH_SIZE = 200
# Apply runs mutate IA and each affected work can incur ~932s of extended
# upload-propagation polling; a conservative cap keeps a fully lagging batch
# within the 180-minute apply job timeout with margin for source retrieval,
# uploads and report generation. Larger campaigns use multiple sequential runs.
APPLY_MAX_BATCH_SIZE = 7


def maximum_batch_size(mode):
    """Return the worst-case selectable-works cap for the given mode."""
    return DRY_RUN_MAX_BATCH_SIZE if mode == "dry-run" else APPLY_MAX_BATCH_SIZE


def annotation(title, message):
    """Emit an escaped GitHub error annotation."""
    safe = (
        message.replace("%", "%25")
        .replace("\r", "%0D")
        .replace("\n", "%0A")
    )
    print(f"::error title={title}::{safe}")


def uuid_input(value, label, errors):
    if not value:
        return None
    try:
        return str(UUID(value))
    except (AttributeError, ValueError):
        message = f"{label} is not a valid UUID: {value!r}"
        errors.append(message)
        annotation("Malformed UUID", message)
        return None


def integer_input(name, minimum, maximum, errors):
    raw = os.environ.get(f"INPUT_{name.upper()}", "")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        message = f"{name} must be an integer; received {raw!r}"
        errors.append(message)
        annotation(f"Invalid {name}", message)
        return None
    if value < minimum or (maximum is not None and value > maximum):
        bounds = (
            f"between {minimum} and {maximum}"
            if maximum is not None
            else f"at least {minimum}"
        )
        message = f"{name} must be {bounds}; received {value}"
        errors.append(message)
        annotation(f"Invalid {name}", message)
        return None
    return value


def validate_inputs(mode):
    """Normalise input files and fail with annotations for unsafe requests."""
    LOG_PATH.write_text("", encoding="utf-8")
    errors = []

    publisher_raw = os.environ.get("INPUT_PUBLISHER_ID", "").strip()
    publisher_id = uuid_input(publisher_raw, "publisher_id", errors)

    explicit_entries = [
        entry.strip()
        for entry in re.split(
            r"[,\r\n]+", os.environ.get("INPUT_WORK_IDS", "")
        )
        if entry.strip()
    ]
    explicit_ids = sorted(set(filter(None, (
        uuid_input(entry, "work_ids entry", errors)
        for entry in explicit_entries
    ))))

    limit = integer_input("limit", 1, 200, errors)
    offset = integer_input("offset", 0, None, errors)

    if not publisher_raw and not explicit_entries:
        message = "At least one of publisher_id or work_ids must be supplied"
        errors.append(message)
        annotation("No selection criteria", message)

    maximum_batch = maximum_batch_size(mode)
    possible_batch = None
    if limit is not None:
        possible_batch = (
            limit + len(explicit_ids)
            if publisher_raw
            else len(explicit_ids)
        )
        if possible_batch > maximum_batch:
            if mode == "apply":
                message = (
                    f"Apply batches may select at most {APPLY_MAX_BATCH_SIZE} "
                    f"works; this request can select up to {possible_batch}."
                )
                annotation("Apply batch exceeds cap", message)
            else:
                message = (
                    "The requested batch can select up to "
                    f"{possible_batch} works; the hard limit is "
                    f"{DRY_RUN_MAX_BATCH_SIZE}"
                )
                annotation("Combined batch exceeds 200", message)
            errors.append(message)

    if mode == "apply":
        if os.environ.get("INPUT_CONFIRM_APPLY") != "APPLY":
            message = "Apply mode requires confirm_apply to be exactly APPLY"
            errors.append(message)
            annotation("Missing apply confirmation", message)
        if os.environ.get("GITHUB_REF") != "refs/heads/develop":
            message = (
                "Apply mode is restricted to refs/heads/develop; received "
                f"{os.environ.get('GITHUB_REF')!r}"
            )
            errors.append(message)
            annotation("Apply ref restriction", message)

    context = {
        "artifact_name": os.environ["ARTIFACT_NAME"],
        "explicit_work_id_count": len(explicit_ids),
        "github_ref": os.environ.get("GITHUB_REF"),
        "limit": limit,
        "maximum_batch_size": maximum_batch,
        "mode": mode,
        "offset": offset,
        "possible_batch_size": possible_batch,
        "publisher_id": publisher_id,
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "validation_errors": errors,
    }
    CONTEXT_PATH.write_text(
        json.dumps(context, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    WORK_IDS_PATH.write_text(
        "".join(f"{work_id}\n" for work_id in explicit_ids),
        encoding="utf-8",
    )

    if errors:
        with LOG_PATH.open("a", encoding="utf-8") as log:
            for error in errors:
                log.write(f"VALIDATION ERROR: {error}\n")
        return 2

    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as out:
        out.write(f"publisher_id={publisher_id or ''}\n")
        out.write(f"explicit_count={len(explicit_ids)}\n")
        out.write(f"limit={limit}\n")
        out.write(f"offset={offset}\n")
    return 0


def _load_context():
    if not CONTEXT_PATH.exists():
        return {}
    try:
        return json.loads(CONTEXT_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _report_lines(context):
    if not REPORT_PATH.exists():
        return [
            "Execution failed before a complete report was generated. "
            "Review the uploaded stderr log and run context."
        ]

    try:
        report = json.loads(REPORT_PATH.read_text(encoding="utf-8"))
        summary = report["summary"]
        results = report.get("results") or []
        lines = [
            "## Summary counts",
            "",
            "| Inspected | Current | Repairable | Ambiguous | Failed | Repaired |",
            "| ---: | ---: | ---: | ---: | ---: | ---: |",
            "| {inspected} | {current} | {repairable} | {ambiguous} | "
            "{failed} | {repaired} |".format(**summary),
            "",
            "## Counts by status",
            "",
        ]
        by_status = summary.get("by_status") or {}
        if by_status:
            lines.extend(
                f"- `{status}`: {count}"
                for status, count in by_status.items()
            )
        else:
            lines.append("- No statuses reported")

        if context.get("mode") == "apply":
            lines.extend([
                "",
                "## Apply actions",
                "",
                "- Attempted actions: " + str(sum(
                    len(result.get("attempted_actions") or [])
                    for result in results
                )),
                "- Applied actions: " + str(sum(
                    len(result.get("applied_actions") or [])
                    for result in results
                )),
                "- Uncertain actions: " + str(sum(
                    len(result.get("uncertain_actions") or [])
                    for result in results
                )),
            ])
        return lines
    except (KeyError, OSError, TypeError, ValueError) as error:
        return [
            "The report exists but could not be parsed for this summary: "
            f"{error}. Review the uploaded report and log."
        ]


def write_summary():
    """Write a concise summary without exposing per-work report details."""
    context = _load_context()
    lines = [
        "# Internet Archive reconciliation",
        "",
        f"- Mode: {context.get('mode', 'unavailable')}",
    ]
    if context.get("publisher_id"):
        lines.append(f"- Publisher ID: `{context['publisher_id']}`")
    cli_status = (
        STATUS_PATH.read_text(encoding="utf-8").strip()
        if STATUS_PATH.exists()
        else "unavailable"
    )
    artifact_name = (
        context.get("artifact_name")
        or os.environ.get("ARTIFACT_NAME", "unavailable")
    )
    lines.extend([
        "- Explicit work IDs: "
        f"{context.get('explicit_work_id_count', 'unavailable')}",
        f"- Requested limit: {context.get('limit', 'invalid')}",
        f"- Requested offset: {context.get('offset', 'invalid')}",
        f"- CLI exit status: {cli_status}",
        f"- Artifact: `{artifact_name}`",
        "",
    ])

    validation_errors = context.get("validation_errors") or []
    if validation_errors:
        lines.extend(["## Validation errors", ""])
        lines.extend(f"- {error}" for error in validation_errors)
        lines.append("")
    lines.extend(_report_lines(context))

    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open(
        "a", encoding="utf-8"
    ) as summary_file:
        summary_file.write("\n".join(lines) + "\n")
    return 0


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--mode", choices=("dry-run", "apply"), required=True)
    subparsers.add_parser("summary")
    return parser.parse_args(argv)


def main(argv=None):
    arguments = parse_arguments(argv)
    if arguments.command == "validate":
        return validate_inputs(arguments.mode)
    return write_summary()


if __name__ == "__main__":
    sys.exit(main())
