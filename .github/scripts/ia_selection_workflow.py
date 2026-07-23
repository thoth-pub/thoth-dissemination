#!/usr/bin/env python3
"""Safe output, summary and final-status handling for scheduled IA selection."""

import argparse
import json
import os
from pathlib import Path
import sys


def _read_status(path):
    try:
        return int(Path(path).read_text(encoding='utf-8').strip())
    except (OSError, TypeError, ValueError):
        return 1


def _read_report(path):
    try:
        report = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, TypeError, ValueError):
        return None
    return report if isinstance(report, dict) else None


def _read_selected_ids(path):
    try:
        selected = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, TypeError, ValueError):
        return None
    if not isinstance(selected, list) or not all(
            isinstance(work_id, str) for work_id in selected):
        return None
    return selected


def _append_output(name, value):
    output_path = os.environ.get('GITHUB_OUTPUT')
    if not output_path:
        raise RuntimeError('GITHUB_OUTPUT is not set')
    with open(output_path, 'a', encoding='utf-8') as output:
        output.write('{}={}\n'.format(name, value))


def emit_outputs(args):
    report = _read_report(args.report)
    selected = _read_selected_ids(args.selected_ids)
    status = _read_status(args.status)
    valid_report = report is not None
    valid_ids = selected is not None

    if selected is None:
        selected = []
    compact_ids = json.dumps(selected, separators=(',', ':'))
    _append_output('work_ids', compact_ids)
    _append_output(
        'selected_count',
        report.get('selected_count', 0) if valid_report else 0)
    _append_output(
        'omitted_count',
        report.get('omitted_count', 0) if valid_report else 0)
    _append_output(
        'truncated',
        str(bool(report.get('truncated')) if valid_report else False).lower())
    _append_output('selection_exit_status', status)
    _append_output('report_valid', str(valid_report).lower())
    _append_output('ids_valid', str(valid_ids).lower())
    _append_output('artifact_name', args.artifact_name)
    return 0


def write_summary(args):
    report = _read_report(args.report)
    status = _read_status(args.status)
    summary_path = os.environ.get('GITHUB_STEP_SUMMARY')
    if not summary_path:
        raise RuntimeError('GITHUB_STEP_SUMMARY is not set')

    lines = [
        '## Internet Archive scheduled selection',
        '',
        '- Selection exit status: `{}`'.format(status),
        '- Artifact: `{}`'.format(args.artifact_name),
    ]
    if report is None:
        lines.extend([
            '- Diagnostics: expected selection report is missing or invalid',
            '',
        ])
    else:
        window = report.get('window') or {}
        lines.extend([
            '- Window start: `{}`'.format(window.get('start', 'unavailable')),
            '- Window end: `{}`'.format(window.get('end', 'unavailable')),
            '- Lookback hours: `{}`'.format(
                window.get('lookback_hours', 'unavailable')),
            '- Configured publishers: `{}`'.format(
                len(report.get('publisher_ids') or [])),
            '- Queried works: `{}`'.format(report.get('queried_count', 0)),
            '- Eligible works: `{}`'.format(report.get('eligible_count', 0)),
            '- Selected works: `{}`'.format(report.get('selected_count', 0)),
            '- Omitted works: `{}`'.format(report.get('omitted_count', 0)),
            '- Truncated: `{}`'.format(
                str(bool(report.get('truncated'))).lower()),
            '',
            '### Excluded by reason',
            '',
        ])
        excluded_counts = report.get('excluded_counts') or {}
        if excluded_counts:
            lines.extend(
                '- `{}`: {}'.format(reason, excluded_counts[reason])
                for reason in sorted(excluded_counts)
            )
        else:
            lines.append('- None')
        lines.append('')

    with open(summary_path, 'a', encoding='utf-8') as summary:
        summary.write('\n'.join(lines))
    return 0


def selection_guard(args):
    status = _read_status(args.status)
    report = _read_report(args.report)
    selected = _read_selected_ids(args.selected_ids)
    if status != 0:
        print(
            'Selection failed with exit status {}'.format(status),
            file=sys.stderr)
        return 1
    if report is None:
        print('Selection report is missing or invalid', file=sys.stderr)
        return 1
    if selected is None:
        print('Selected work ID output is missing or invalid', file=sys.stderr)
        return 1
    report_ids = [
        entry.get('work_id') for entry in report.get('selected', [])
        if isinstance(entry, dict)
    ]
    if selected != report_ids:
        print(
            'Selected work ID output does not match the report',
            file=sys.stderr)
        return 1
    return 0


def final_guard(_args):
    selection_result = os.environ.get('SELECTION_RESULT', '')
    dissemination_result = os.environ.get('DISSEMINATION_RESULT', '')
    selected_count = os.environ.get('SELECTED_COUNT', '0')
    omitted_count = os.environ.get('OMITTED_COUNT', '0')
    truncated = os.environ.get('TRUNCATED', 'false').lower() == 'true'
    artifact_name = os.environ.get('ARTIFACT_NAME', 'ia-selection diagnostics')

    errors = []
    if selection_result != 'success':
        errors.append(
            'selection job result was `{}`'.format(
                selection_result or 'unavailable'))
    if selected_count != '0' and dissemination_result != 'success':
        errors.append(
            'dissemination job result was `{}`'.format(
                dissemination_result or 'unavailable'))
    if selected_count == '0' and dissemination_result not in (
            'skipped', 'success'):
        errors.append(
            'empty selection had unexpected dissemination result `{}`'.format(
                dissemination_result or 'unavailable'))
    if truncated:
        errors.append(
            '{} eligible works were omitted by the bounded selection'.format(
                omitted_count))

    summary_path = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary_path:
        with open(summary_path, 'a', encoding='utf-8') as summary:
            summary.write('## Internet Archive final status\n\n')
            if errors:
                for error in errors:
                    summary.write('- Failure: {}\n'.format(error))
                if truncated:
                    summary.write(
                        '- Inspect artifact `{}` and run bounded manual '
                        'reconciliation or another reviewed bounded operation '
                        'for omitted works.\n'.format(artifact_name))
            else:
                summary.write(
                    '- Selection and all required dissemination completed '
                    'without overflow.\n')

    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


def get_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)

    outputs = subparsers.add_parser('outputs')
    outputs.add_argument('--report', required=True)
    outputs.add_argument('--selected-ids', required=True)
    outputs.add_argument('--status', required=True)
    outputs.add_argument('--artifact-name', required=True)
    outputs.set_defaults(func=emit_outputs)

    summary = subparsers.add_parser('summary')
    summary.add_argument('--report', required=True)
    summary.add_argument('--status', required=True)
    summary.add_argument('--artifact-name', required=True)
    summary.set_defaults(func=write_summary)

    guard = subparsers.add_parser('selection-guard')
    guard.add_argument('--report', required=True)
    guard.add_argument('--selected-ids', required=True)
    guard.add_argument('--status', required=True)
    guard.set_defaults(func=selection_guard)

    final = subparsers.add_parser('final-guard')
    final.set_defaults(func=final_guard)
    return parser


def main(argv=None):
    args = get_parser().parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
