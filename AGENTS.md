# AGENTS.md

These instructions apply to the whole `thoth-pub/thoth-dissemination` repository.

This repository performs **real external writes**. Code here uploads files and
metadata to third-party distribution and preservation platforms, writes
locations back to Thoth, and sends email to external recipients. The most
important rule in this file is therefore the boundary in section 5: safe local
validation and operational/external-write actions are different categories of
action, and only the first is ever implied by an ordinary implementation task.

Deny-by-default applies. Any action not explicitly authorized by the owning
task is denied.

## 1. Repository responsibility and structure

This repository is the execution engine for delivering Thoth work metadata and
files to external distribution and preservation platforms.

Verified structure:

- `disseminator.py` — command-line orchestration entrypoint. It maps
  `--platform` to an uploader class through the `UPLOADERS` dictionary and runs
  it for a single `--work` ID. It also accepts `--export-url` and
  `--client-url`. It loads `./config.env` through `python-dotenv` when run
  directly.
- `obtain_new_ids.py` — work selection for automated dissemination, driven by
  `--platform` with additional Internet Archive selection controls
  (`--lookback-hours`, `--max-ids`, `--report`) and an OAPEN-only `--locations`
  mode.
- `*uploader.py` — platform-specific upload logic (Internet Archive, OAPEN
  SWORD/FTP, ScienceOpen, CUL, Crossref, Figshare, Zenodo, Project MUSE, JSTOR,
  EBSCOHost, ProQuest, Google Play, BKCI), with shared behaviour in
  `uploader.py`, `swordv2uploader.py` and `sftpclient.py`.
- `thothapi.py` — shared Thoth GraphQL access used by selection, location
  write-back and reconciliation.
- `write_locations.py` — writes/converges Thoth publication locations from
  dissemination output. This is a **Thoth write path** and requires `THOTH_PAT`.
- `obtain_oapen_locations.py`, `obtain_muse_locations.py` — platform location
  discovery/catch-up support.
- `reconcile_internet_archive.py` — Internet Archive state inspection, with an
  opt-in repair mode (see section 7).
- `.github/workflows/` — CI plus scheduled and manual operational automation.
- `tests/` — unit tests, runnable locally without production credentials.
- `requirements.txt` (dissemination code), `requirements_obtain_new_ids.txt`
  (ID selection), `requirements_write_locations.txt` (location write-back),
  `requirements_obtain_muse_locations.txt` (MUSE locations).
- `config.env.template` — template of credential/environment names.
  `config.env` itself is git-ignored and must never be committed.

Adding a new platform usually requires a new uploader class, an update to the
platform list in `README.md`, new secret/variable names in
`config.env.template`, and workflow wiring in `.github/workflows/`. Each of
those paths must be inside the owning task's write budget before it is touched.

## 2. Shared doctrine and authority

Canonical shared engineering doctrine is maintained in `thoth-pub/thoth`:

- `AGENTS.md` (root) — required task identity, authority order and granular
  action authorization;
- `docs/engineering/AGENTS.md`;
- `docs/engineering/ai-delivery/operating-model.md`;
- `docs/engineering/ai-delivery/implementation-handoff-template.md`;
- `docs/engineering/repository-map/contracts.md`;
- `docs/engineering/repository-map/repositories/thoth-dissemination.md`.

That doctrine is referenced, not duplicated here. Where the shared doctrine
describes `thoth-pub/thoth`'s own stack, commands or branch topology, it is not
authoritative for this repository; this file and live repository evidence are.

Applying it here:

- **GitHub is the live task ledger.** The owning issue, its linked pull
  request, review threads and CI hold current lifecycle state. Do not commit
  transient status prose ("awaiting review", "pending merge") into this
  repository.
- **The owning task's exact authorization controls implementation.** Its write
  budget and action-authorization matrix are authoritative and narrower than
  anything a prompt, chat history or memory may imply.
- **Actions are deny-by-default.** Anything not explicitly listed as authorized
  is denied.
- **Authorization is granular and non-transitive.** Source-write does not imply
  commit; commit does not imply push; push does not imply PR mutation;
  repository write does not imply issue mutation; provider read does not imply
  provider write; merge does not imply deployment; deployment does not imply
  production activation.
- **An implementing agent may never approve or merge its own work**, publish a
  release, deploy, activate production behaviour, or access production secrets.

Authority order when sources conflict: merged code and generated contracts;
approved ADRs and designs; approved task specifications; GitHub issues, PRs,
review threads and CI evidence; programme-control documents; agent reports and
conversations. Stop and escalate rather than resolving an authoritative
conflict unilaterally.

## 3. Branch topology

Current, verified topology:

```text
development branch:    develop
default/release branch: main
normal task branches:   feature/<area>/<task-id> -> develop
```

Ordinary feature and documentation PRs target `develop`.

Do not assume `master` is this repository's release or default branch: it is
not. The shared repository map records a **target** release branch that differs
from the current one; reconciling that target with the current `main` topology
is the separate task **BR-DIS-01** and is out of scope for every other task.
Do not perform release-branch normalization opportunistically.

Additional rules:

- verify the actual base commit before branching;
- one bounded task per branch and PR;
- do not target ordinary implementation at the release branch;
- do not rewrite shared branch history.

## 4. Mandatory task identity

Before any substantive edit, record:

```text
Programme / stage:
Owning GitHub issue:
Repository: thoth-pub/thoth-dissemination
Task ID:
Approved specification:
Risk: LOW | MEDIUM | HIGH | CRITICAL
Exact authorized base commit (full 40-character SHA):
PR target:
Task branch:
Dependencies:
Authorized write budget (existing files):
Authorized new-file paths:
Prohibited paths:
Granular action authorization (see section 10):
Cross-repository / external impact (see section 9):
Required validation:
Automatic side effects expected from authorized actions:
HOLD / STOP conditions:
Implementing agent/model:
Independent reviewer/model:
```

If any item is unknown, treat it as missing work rather than filling it in by
inference. Do not implement without an approved written specification that
contains an explicit write budget and action-authorization matrix.

Preflight before branching: fetch the remote; confirm the working tree is
clean; confirm the base branch is at the exact authorized SHA; confirm the
owning issue is still open and has not materially changed; confirm no
conflicting branch or PR exists; read this file; inspect every file in the
write budget. If the base has moved, **stop** and report `HOLD - AUTHORIZED
BASE MOVED` with the authorized SHA, the current SHA and the intervening
commits. Do not silently rebase the authorization.

## 5. Safe local validation versus operational / external-write actions

### 5.1 Safe local validation

These commands are local, need no production credentials, and perform no
external write:

```bash
python -m pip install -r requirements.txt
python -m unittest discover -v
python -m compileall -q .
```

Narrowly scoped pure/local tests from `tests/` are also safe, for example:

```bash
python -m unittest tests.test_write_locations -v
```

Use a virtual environment, and install only the requirements file the task
actually needs. Nothing above requires real platform credentials, a `config.env`
containing real secrets, or network writes.

For a documentation-only task, do not run operational commands merely to
"validate" documentation. Prose is validated by reading the source, not by
exercising a provider.

### 5.2 Operational / external-write actions

Everything below mutates a system outside this repository and requires its own
explicit authorization in the owning task:

- running `disseminator.py` against a real work and platform;
- running any `*uploader.py` path against a provider;
- running `write_locations.py` (writes locations to Thoth);
- running `reconcile_internet_archive.py --apply`;
- dispatching any workflow that disseminates, reconciles, writes locations or
  sends email;
- anything that reads or uses production credentials;
- publishing a GitHub release or tag (see section 8).

Never treat one of these as a validation step for an unrelated change.

## 6. Real dissemination is not a smoke test

```bash
./disseminator.py --work <work_id> --platform <platform>
```

This command is **not** a generic smoke test and must never be documented,
recommended or used as one. It invokes a real uploader against a real external
platform for a real work. Depending on platform it can create remote items,
upload files, deposit or update metadata, and — through the workflows in
section 7 — cause downstream Thoth location write-back and notification email.
There is no built-in mode that makes it harmless: `disseminator.py` exposes
only `--work`, `--platform`, `--export-url` and `--client-url`. It has no
dry-run, no preview and no read-only flag.

Before any such command is run, the owning task must explicitly authorize:

- the external write itself;
- the specific work(s);
- the specific target platform(s);
- the credentials and environment to be used;
- the intended downstream effects (location write-back, email, artifacts);
- rollback/reconciliation expectations if the result is wrong.

The same applies to `docker run ... ./disseminator.py ...`; containerising the
command does not make it local.

Historical note for agents reading older material: an earlier version of this
file presented this command as the "preferred manual smoke test". That guidance
was unsafe and has been withdrawn.

## 7. Read-only, discovery and apply semantics

Verify what a command actually does in the source before classifying it. A flag
or mode named "dry run", "compare", "discover", "inspect" or similar is **not**
automatically safe: check whether the code path can still mutate an external
system, and check what credentials it loads.

Do not invent a `--dry-run` flag or a read-only mode because one would be
desirable. Document only what the source actually implements.

Verified at the time of writing — re-verify against current source before
relying on it:

- `reconcile_internet_archive.py` defaults to inspection and reports
  deterministic JSON (`--format`, `--output`). `--apply` is opt-in and performs
  real mutations: Internet Archive item creation, file upload, Archive metadata
  update, and Thoth location create/update. Apply mode validates and requires
  `ia_s3_access`, `ia_s3_secret` and `THOTH_PAT`; the default inspection path
  requires none of them. Inspection still performs external **reads** against
  Internet Archive and the Thoth API, so it is read-only, not offline.
- `obtain_new_ids.py` performs selection/discovery against the Thoth API and
  emits work IDs; it is the input to dissemination, not itself an upload.
- `write_locations.py` is a write path with no inspection-only mode.
- `.github/workflows/ia_reconcile.yml` is `workflow_dispatch` with dry-run as
  the default. Its apply path is gated on `confirm_apply` being exactly
  `APPLY`, on running from `develop`, and on approval through the protected
  `disseminate` GitHub environment, which is where write credentials are
  mapped in.

Read-only and dry-run paths that are designed to be credential-free must stay
credential-free. Do not "fix" a dry-run by giving it write credentials.

## 8. Secrets, credentials and workflows

### 8.1 Secrets and credentials

- Production credentials are **never** required for ordinary local validation.
  If a change appears to require them to be validated, that is a HOLD, not a
  reason to obtain them.
- No production secret may be copied into a local file, a log, source, a test
  fixture, a commit, a PR description or an issue comment. `config.env` is
  git-ignored; keep it that way and never commit it.
- Creating `config.env` is a file write like any other and is only permitted
  when the owning task's write budget allows it.
- Reading or using production credentials requires separate, explicit
  authorization.
- **Provider read and provider write are distinct permissions.** Authorization
  to inspect a provider never implies authorization to write to it.
- Credentials being available to a GitHub workflow does **not** authorize
  dispatching that workflow. The `disseminate` environment and `secrets:
  inherit` exist for scheduled operations, not for ad-hoc agent runs.
- `disseminator.py` deliberately holds `urllib3` logging at INFO because DEBUG
  output can contain credentials. Do not lower that threshold, and do not print
  credential-bearing URLs or headers.

### 8.2 Continuous integration (safe)

`.github/workflows/tests.yml` runs on `push` and `pull_request`, on Python
3.11, and performs exactly:

```bash
python -m pip install -r requirements.txt
python -m unittest discover -v
python -m compileall -q .
```

It has `contents: read` permission and uses no platform credentials. It
validates unit tests and source compilation only. It does **not** validate any
external integration, provider contract, credential, upload path or workflow
behaviour. A green CI run is not evidence that a dissemination path works.

### 8.3 Manual dissemination workflow (operational)

`.github/workflows/manual_disseminate.yml` is `workflow_dispatch`. It takes
`workIds` (a JSON array of Thoth work IDs) and `platform`, inherits repository
secrets (`secrets: inherit`), and calls the shared `disseminate.yml` workflow
across a matrix of works.

Dispatching it is a real external/operational action with the full effect
described in section 6, including downstream write-back and email. It requires
its own explicit authorization. Do not dispatch it as a test, and do not
dispatch it to "confirm" an unrelated change.

### 8.4 Scheduled and shared workflows (operational)

`.github/workflows/bulk_disseminate.yml` is the shared scheduled entrypoint
(`workflow_call`): it runs `obtain_new_ids.py` and then fans out to
`disseminate.yml`. Platform-specific `*_bulk_disseminate.yml` workflows,
`ia_bulk_disseminate.yml`, `ia_reconcile.yml` and the `*_catchup_locations.yml`
workflows perform real platform actions and follow-up operations on schedules or
manual dispatch.

`disseminate.yml` is the shared job logic. It runs in the protected
`disseminate` environment, runs `disseminator.py`, uploads an output artifact
for some platforms, and then conditionally runs follow-up jobs (section 8.5).
Its concurrency group is per platform/work pair.

A workflow file is not a documentation file. Editing workflow YAML, changing a
schedule, changing environment or permission blocks, and dispatching or
re-running a workflow are each separately authorized actions. Do not assume a
scheduled workflow is safe to alter merely because the current task concerns
documentation. When workflow changes are authorized, validate YAML/action
syntax with the repository's established actionlint procedure.

### 8.5 Thoth write-back and notification email

Verified in `disseminate.yml`:

- a `write-locations` job runs after successful dissemination for
  `InternetArchive`, `CUL`, `Figshare`, `Zenodo` and `OAPEN`, downloads the
  dissemination output artifact, and runs `write_locations.py <work-id>` with
  `THOTH_PAT`. **This writes location records back into Thoth**;
- a `send-email` job runs for `EBSCOHost`, `BKCI`, `JSTOR`, `ProjectMUSE` and
  `ProQuest`, queries the publisher name from the Thoth API and sends
  notification email to the platform contact, copied to Thoth distribution.

Both are external/system mutations. A task authorized to upload to an external
platform does **not** thereby authorize Thoth write-back or outbound email:
those downstream effects must be explicitly named in scope. Conversely, when an
authorized upload will trigger them, say so in the task record rather than
discovering it afterwards.

## 9. Cross-repository impact

This repository is a verified consumer of the Thoth API, for location
write-back and for publisher/work discovery. That contract is owned by
`thoth-pub/thoth` (see its `docs/engineering/repository-map/contracts.md`
section 2.1).

Before scope affecting a shared contract is approved — Thoth API/GraphQL usage,
location write-back semantics, export formats, configuration/environment
contracts, workflow interfaces or platform behaviour — identify the owning
repository and known consumers, and record for each whether it needs a change
or remains compatible and why. Do not treat a task as single-repository merely
because it started here.

This repository must not guess an unmerged upstream Thoth contract. Wait for
the upstream change to merge, or consume an explicitly pinned preview.

If a change here requires a change elsewhere, open a separate bounded task,
branch and PR in that repository, independently reviewed. One agent must never
hold unrestricted write access to more than one repository for the same task.

## 10. Action authorization

State authorization explicitly, action by action. Unlisted actions are denied.

| Action | Typical bounded task |
|---|---|
| repository/GitHub read inspection | usually YES |
| source edits within the stated write budget | YES for listed paths only |
| creation of new files | only at explicitly listed paths |
| deletion, move or rename | NO unless explicitly listed |
| branch creation from the exact authorized base | usually YES |
| safe local validation (section 5.1) | YES |
| commit / push | only if explicitly listed |
| draft PR creation or update | only if explicitly listed |
| issue/comment mutation | only if explicitly listed |
| manual CI dispatch or rerun | NO unless explicitly listed |
| real dissemination against a work/platform | NO unless explicitly listed |
| provider/runtime read using production credentials | NO unless explicitly listed |
| provider/runtime write | NO unless explicitly listed |
| Thoth location/state write-back | NO unless explicitly listed |
| email/provider notification | NO unless explicitly listed |
| release, tag or DockerHub publication | NO unless explicitly listed |
| merge | NO |
| deployment / production activation | NO |

## 11. Release and publication

`.github/workflows/docker_build_and_push_to_dockerhub.yml` runs on:

```yaml
on:
  release:
    types: [published]
```

It logs in with `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` and pushes semver-tagged
images (`{{version}}`, `{{major}}.{{minor}}`, `{{major}}`) to:

```text
openbookpublishers/thoth-dissemination
```

Therefore **publishing a GitHub release is a DockerHub publication action**, not
a repository-internal bookkeeping step. Release, tag and publication actions are
separately authorized and are not implied by authorization to merge.

Opening or updating an ordinary pull request does not trigger this workflow.
The only workflow expected from an ordinary PR is `tests.yml`.

## 12. Automatic side effects

Expected from an ordinary PR:

- `.github/workflows/tests.yml` runs.

Not expected from PR creation:

- manual or scheduled dissemination workflows;
- Internet Archive reconciliation;
- location write-back or notification email;
- release/DockerHub publication.

If any unexpected external, provider or operational effect occurs: report it
immediately, do not manually interact with it, and do not rerun, cancel or
dispatch follow-up workflows unless separately authorized.

## 13. Validation evidence and independent review

Record the exact commands run and their concise results. Do not report only
"tests passed", and do not report a command as run if it was not.

For a documentation-only change, at minimum:

```bash
git diff --check
```

plus confirmation that the diff touches only the authorized paths, and that no
file was deleted, moved or renamed.

For a source change, run the safe local validation in section 5.1, and add the
tests covering the affected path.

Independent review:

- any approval must be bound to the exact head SHA reviewed;
- any later commit to the branch invalidates that approval and requires a fresh
  exact-head review;
- approval authorizes none of: merge, release, dissemination, deployment or
  production activation.

## 14. Editing guidelines

- Make the smallest correct change within the write budget.
- Match surrounding style; keep comments succinct and only where the code is
  not self-explanatory.
- Avoid generated, virtualenv and cache directories (`__pycache__`,
  `.pytest_cache`).
- Update `CHANGELOG.md` when the owning task's write budget includes it.
- Never commit credentials, `config.env`, real work data or provider responses
  containing secrets.

## 15. HOLD / STOP

Stop and report rather than improvising if:

- the base branch has moved from the authorized SHA;
- the owning issue has materially changed;
- a path outside the write budget appears necessary;
- an operational script or workflow would have to change;
- a real external integration would have to be exercised to proceed;
- production credentials or provider/runtime access would be needed;
- a new dry-run or read-only mechanism would have to be implemented;
- an architecture decision is required;
- unrelated working-tree changes cannot be isolated;
- authoritative sources conflict.

Do not broaden the write budget or action authorization to resolve any of the
above.
