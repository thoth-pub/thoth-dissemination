import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / ".github" / "scripts" / "ia_reconcile_workflow.py"
WORK_ID = "11111111-2222-3333-4444-555555555555"
WORK_ID_2 = "22222222-3333-4444-5555-666666666666"
PUBLISHER_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


class TestIAReconcileWorkflow(unittest.TestCase):
    def run_helper(self, *arguments, **environment):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        workdir = Path(temporary_directory.name)
        github_output = workdir / "github-output"
        step_summary = workdir / "step-summary"
        env = {
            **os.environ,
            "ARTIFACT_NAME": "ia-reconciliation-123-1",
            "GITHUB_OUTPUT": str(github_output),
            "GITHUB_REF": "refs/heads/develop",
            "GITHUB_RUN_ATTEMPT": "1",
            "GITHUB_RUN_ID": "123",
            "GITHUB_STEP_SUMMARY": str(step_summary),
            "INPUT_CONFIRM_APPLY": "",
            "INPUT_LIMIT": "100",
            "INPUT_OFFSET": "0",
            "INPUT_PUBLISHER_ID": "",
            "INPUT_WORK_IDS": "",
            **environment,
        }
        result = subprocess.run(
            [sys.executable, str(HELPER), *arguments],
            cwd=workdir,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        return result, workdir

    def test_mixed_work_ids_are_normalised_deduplicated_and_sorted(self):
        result, workdir = self.run_helper(
            "validate",
            "--mode",
            "dry-run",
            INPUT_WORK_IDS=f"{WORK_ID_2},\n{WORK_ID}\n{WORK_ID_2},,",
        )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(
            (workdir / "normalised-work-ids.txt").read_text().splitlines(),
            [WORK_ID, WORK_ID_2],
        )
        context = json.loads(
            (workdir / "reconciliation-run-context.json").read_text()
        )
        self.assertEqual(context["explicit_work_id_count"], 2)
        self.assertEqual(context["possible_batch_size"], 2)
        self.assertEqual(context["validation_errors"], [])

    def test_no_selection_is_rejected_with_annotation_and_diagnostics(self):
        result, workdir = self.run_helper(
            "validate", "--mode", "dry-run"
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("title=No selection criteria", result.stdout)
        self.assertIn(
            "VALIDATION ERROR:",
            (workdir / "internet-archive-reconciliation.log").read_text(),
        )
        self.assertTrue(
            (workdir / "reconciliation-run-context.json").is_file()
        )

    def test_malformed_uuid_is_rejected(self):
        result, _ = self.run_helper(
            "validate",
            "--mode",
            "dry-run",
            INPUT_WORK_IDS="not-a-uuid",
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("title=Malformed UUID", result.stdout)

    def test_invalid_limit_and_offset_are_rejected(self):
        cases = (
            ({"INPUT_LIMIT": "0", "INPUT_WORK_IDS": WORK_ID}, "Invalid limit"),
            (
                {"INPUT_OFFSET": "-1", "INPUT_WORK_IDS": WORK_ID},
                "Invalid offset",
            ),
        )
        for environment, annotation_title in cases:
            with self.subTest(annotation_title=annotation_title):
                result, _ = self.run_helper(
                    "validate", "--mode", "dry-run", **environment
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn(
                    f"title={annotation_title}", result.stdout
                )

    def test_combined_publisher_batch_over_200_is_rejected(self):
        result, workdir = self.run_helper(
            "validate",
            "--mode",
            "dry-run",
            INPUT_LIMIT="200",
            INPUT_PUBLISHER_ID=PUBLISHER_ID,
            INPUT_WORK_IDS=WORK_ID,
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("title=Combined batch exceeds 200", result.stdout)
        context = json.loads(
            (workdir / "reconciliation-run-context.json").read_text()
        )
        self.assertEqual(context["possible_batch_size"], 201)

    def test_apply_requires_confirmation_and_develop_ref(self):
        result, _ = self.run_helper(
            "validate",
            "--mode",
            "apply",
            INPUT_WORK_IDS=WORK_ID,
            GITHUB_REF="refs/heads/feature/make-ia-idempotent",
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("title=Missing apply confirmation", result.stdout)
        self.assertIn("title=Apply ref restriction", result.stdout)

    def test_apply_summary_counts_actions_without_per_work_details(self):
        result, workdir = self.run_helper(
            "validate",
            "--mode",
            "apply",
            INPUT_CONFIRM_APPLY="APPLY",
            INPUT_WORK_IDS=WORK_ID,
        )
        self.assertEqual(result.returncode, 0)
        report = {
            "results": [{
                "work_id": WORK_ID,
                "attempted_actions": ["upload_pdf_original", "update_metadata"],
                "applied_actions": ["upload_pdf_original"],
                "uncertain_actions": ["update_metadata"],
            }],
            "summary": {
                "inspected": 1,
                "current": 0,
                "repairable": 0,
                "ambiguous": 0,
                "failed": 1,
                "repaired": 0,
                "by_status": {"error": 1},
            },
        }
        (workdir / "internet-archive-reconciliation.json").write_text(
            json.dumps(report)
        )
        (workdir / "reconciliation-exit-status.txt").write_text("1\n")

        summary_result = subprocess.run(
            [sys.executable, str(HELPER), "summary"],
            cwd=workdir,
            env={
                **os.environ,
                "ARTIFACT_NAME": "ia-reconciliation-123-1",
                "GITHUB_STEP_SUMMARY": str(workdir / "step-summary"),
            },
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(summary_result.returncode, 0)
        summary = (workdir / "step-summary").read_text()
        self.assertIn("- Attempted actions: 2", summary)
        self.assertIn("- Applied actions: 1", summary)
        self.assertIn("- Uncertain actions: 1", summary)
        self.assertNotIn(WORK_ID, summary)


if __name__ == "__main__":
    unittest.main()
