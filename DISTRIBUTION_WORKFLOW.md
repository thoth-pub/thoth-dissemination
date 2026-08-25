# Thoth Dissemination Workflow (Repository Reference)

This document describes the workflow currently implemented in `thoth-dissemination`: what is distributed to each platform, which works are selected, under what conditions, and how often.

## 1) End-to-end architecture

1. Most scheduled (or manual) GitHub workflows call `bulk_disseminate.yml` with:
   - `platform`
   - `env_publishers` (JSON list of publisher IDs)
   - `env_exceptions` (JSON list of work IDs to skip)
2. Internet Archive instead uses a dedicated daily
   `ia_bulk_disseminate.yml` selection/reporting job.
3. The selector runs `obtain_new_ids.py --platform <platform>` and emits a
   compact JSON list of work IDs.
4. The calling workflow fans out to one `disseminate.yml` run per work ID.
   Scheduled IA uses at most four parallel works.
5. `disseminate.yml` prevents overlapping ordinary runs for the same
   platform/work pair.
6. `disseminate.yml` runs `disseminator.py --work <work-id> --platform <platform>`.
7. Platform uploaders either:
   - return upload locations (which can be written back to Thoth), or
   - return no locations (and rely on email notifications / later catch-up).

## 2) Global selection rules (automatic runs)

All automatic runs share these rules from `obtain_new_ids.py`:

- `ENV_PUBLISHERS` is required, must be JSON, non-empty, and each publisher ID is validated against Thoth. Publisher authority is `env` unless the repository variable described in section 9 says otherwise.
- `ENV_EXCEPTIONS` is optional; if present, matching work IDs are removed.
- Default target status is `ACTIVE`.
- Automatic selection is book-focused:
  - Default `work_types` excludes chapters (`BOOK_CHAPTER` is not included).
  - Some routes use `bookIds`, which also excludes chapters (and Book Sets).
- Date-windowed finders use publication date filters in code:
  - `WeeklyIDFinder`: previous 7 days.
  - `MonthlyIDFinder`: previous calendar month.
  - `GooglePlayIDFinder`: previous day.
  - `CrossrefIDFinder`: updated within ~last 1.25 hours.
- `InternetArchiveIDFinder` is deliberately different:
  - required `IA_ENV_PUBLISHERS` and optional `IA_ENV_EXCEPTIONS` values are
    validated as JSON UUID arrays;
  - selection uses `updatedAtWithRelations`, so relation-only changes qualify;
  - the captured UTC interval is
    `updatedAtWithRelations > start && updatedAtWithRelations <= end`;
  - the default 30-hour lookback creates six hours of overlap between daily
    runs, and the documented hard maximum is 168 hours;
  - only active IA-supported book-level works with a PDF and a non-empty
    canonical PDF `fullTextUrl` qualify;
  - selection performs no PDF/export download and no request to source URLs;
  - eligible works are ordered by oldest update, then UUID, before the
    200-work cap.

## 3) Platform schedule and scope matrix

GitHub Actions schedules run in UTC.

| Platform | Workflow | Cadence | Selector class | High-level scope |
|---|---|---|---|---|
| InternetArchive | `ia_bulk_disseminate.yml` | Daily at 04:40 | `InternetArchiveIDFinder` | Active supported works updated in the captured previous 30 hours with a usable canonical PDF source |
| Crossref | `cr_bulk_disseminate.yml` | Hourly at :45 | `CrossrefIDFinder` | Active + qualifying Forthcoming works updated recently |
| Figshare | `fs_bulk_disseminate.yml` | Monthly, day 7 at 04:40 | `MonthlyIDFinder` | Previous calendar month, active |
| Zenodo | `zn_bulk_disseminate.yml` | Monthly, day 7 at 04:40 | `MonthlyIDFinder` | Previous calendar month, active |
| CUL | `cul_bulk_disseminate.yml` | Monthly, day 7 at 04:40 | `MonthlyIDFinder` | Previous calendar month, active |
| GooglePlay | `gp_bulk_disseminate.yaml` | Daily at 05:50 | `GooglePlayIDFinder` | Previous day, active |
| BKCI | `bkci_bulk_disseminate.yaml` | Monthly, day 6 at 06:40 | `BKCIIDFinder` | Previous month, active, excludes textbooks |
| OAPEN | `oapen_bulk_disseminate.yaml` | Weekly, Mon at 02:20 | `WeeklyIDFinder` | Previous week, active |
| EBSCOHost | `eh_bulk_disseminate.yaml` | Weekly, Tue at 02:20 | `WeeklyIDFinder` | Previous week, active |
| JSTOR | `jstor_bulk_disseminate.yaml` | Weekly, Wed at 02:20 | `WeeklyIDFinder` | Previous week, active |
| ProjectMUSE | `muse_bulk_disseminate.yaml` | Weekly, Thu at 02:20 | Intended `WeeklyIDFinder` | Previous week, active |
| ProQuest | `pq_bulk_disseminate.yaml` | Weekly, Fri at 02:20 | `WeeklyIDFinder` | Previous week, active |
| ScienceOpen | No scheduled workflow | Manual only | N/A | Per-work manual dissemination |

## 4) Platform-by-platform payload and conditions

### Internet Archive (`InternetArchive`)
- Scheduled selection:
  - Daily at 04:40 UTC with a default 30-hour relation-aware update window
  - Uses `IA_ENV_PUBLISHERS` and optional `IA_ENV_EXCEPTIONS`
  - Captures one upper boundary and excludes later updates for the next run
  - Processes at most 200 works, oldest update first, with four-way parallelism
  - Uploads a 30-day selection report/log artifact and writes aggregate counts
    to the Step Summary
  - Reports every overflow record and fails the final status after the bounded
    selected batch completes
- Upload content:
  - Required: PDF publication file
  - Also uploads: `json::thoth` metadata file
  - Uses work ID as IA identifier
- Preconditions:
  - `ia_s3_access` / `ia_s3_secret`
  - New identifiers must be explicitly available; owned existing items are
    updated idempotently
  - PDF canonical location must exist and be downloadable during dissemination
- Automatic writeback:
  - Returns Thoth location (`INTERNET_ARCHIVE`) and idempotently creates,
    updates, or preserves it.
- Operations:
  - Unchanged selected works safely no-op.
  - Use the selection artifact plus bounded manual reconciliation for omitted
    or ambiguous works.

### OAPEN (`OAPEN`)
- Upload content:
  - Only `onix_3.0::oapen` metadata XML is uploaded to OAPEN FTP.
- Preconditions:
  - `oapen_ftp_user` / `oapen_ftp_pw`
- Automatic writeback:
  - No immediate location returned by uploader.
  - Separate weekly catch-up (`oapen_catchup_locations.yaml`) queries OAPEN + DOAB APIs and writes missing locations to Thoth.

### ScienceOpen (`ScienceOpen`)
- Upload content (manual flow only):
  - `csv::thoth` metadata
  - Cover image (`jpg` or `png`)
  - PDF file
  - Packaged as zip and uploaded to publisher/date folder.
- Preconditions:
  - `so_ftp_user` / `so_ftp_pw`
  - Paperback ISBN used for filename root
  - PDF required
- Chapter support:
  - Explicitly marked as not yet implemented.

### CUL (`CUL`)
- Upload content:
  - SWORDv2 deposit with:
    - Required PDF
    - Supplemental `json::thoth`
    - Metadata mapped via `JISC_ROUTER` profile
- Preconditions:
  - `cul_pilot_user` / `cul_pilot_pw`
  - PDF canonical location required
- Automatic writeback:
  - Returns location (`OTHER`) and workflow writes it to Thoth.

### Crossref (`Crossref`)
- Upload content:
  - Only `doideposit::crossref` XML submitted via HTTPS.
- Preconditions:
  - Per-publisher credentials:
    - `crossref_user_<publisher_id>`
    - `crossref_pw_<publisher_id>`
  - DOI prefix must resolve via Crossref prefix API.
- Selection specifics:
  - Includes `ACTIVE` and `FORTHCOMING`.
  - Forthcoming entries are removed unless both DOI and publication date exist.
- Automatic writeback:
  - No location writeback (metadata deposit workflow only).

### Figshare (`Figshare`)
- Upload content:
  - Creates one project per work.
  - Creates one article per available publication format.
  - For each article uploads:
    - Publication file
    - `json::thoth` metadata file
- Preconditions:
  - `figshare_token`
  - Work must have:
    - Long Abstract
    - Licence supported by Figshare
    - At least one Main Contribution
    - At least one Subject of type `KEYWORD`
  - Existing article containing same Thoth work ID blocks upload.
- Publication formats considered:
  - Iterates all types in `PUB_FORMATS` (PDF, XML-as-zip, EPUB, AZW3, MOBI, DOCX, FICTION_BOOK), uploading whichever are available.
- Automatic writeback:
  - Returns per-publication locations (`OTHER`) and workflow writes them to Thoth.

### Zenodo (`Zenodo`)
- Upload content:
  - One deposition per work.
  - Uploads every available publication file.
  - Uploads supplemental `*_metadata.json` (`json::thoth`).
- Preconditions:
  - `zenodo_token`
  - Work must have:
    - Long Abstract
    - DOI
    - Licence resolvable to a Zenodo licence ID
    - At least one Main Contribution
  - Existing Zenodo record with `notes:"thoth-work-id:<id>"` blocks upload.
- Automatic writeback:
  - Returns per-publication locations (`OTHER`) and workflow writes them to Thoth.

### Project MUSE (`ProjectMUSE`)
- Upload content:
  - `onix_3.0::project_muse`
  - JPG cover (strict `.jpg`)
  - PDF and/or EPUB (at least one required)
- Preconditions:
  - Per-publisher credentials:
    - `muse_ftp_user_<publisher_id>`
    - `muse_ftp_pw_<publisher_id>`
  - PDF ISBN used as filename root
- Automatic writeback:
  - No location writeback; intended notification email flow.

### JSTOR (`JSTOR`)
- Upload content:
  - `onix_3.0::jstor`
  - JPG cover
  - PDF
- Preconditions:
  - `jstor_ftp_user`, `jstor_ftp_pw`
  - Per-publisher folder secret:
    - `jstor_ftp_folder_<publisher_id>`
  - PDF ISBN used as filename root
- Automatic writeback:
  - No location writeback; notification email flow.

### EBSCOHost (`EBSCOHost`)
- Upload content:
  - `onix_2.1::ebsco_host`
  - PDF and/or EPUB (at least one)
- Preconditions:
  - `ebsco_ftp_user`, `ebsco_ftp_pw`
  - Filename root prefers PDF ISBN, falls back to EPUB ISBN
- Automatic writeback:
  - No location writeback; notification email flow.

### ProQuest (`ProQuest`)
- Upload content:
  - `onix_2.1::proquest_ebrary`
  - Cover file (any extension)
  - PDF and/or EPUB (intended)
- Preconditions:
  - `proquest_ftp_user`, `proquest_ftp_pw`
  - Uploads to `/upload`
- Automatic writeback:
  - No location writeback; notification email flow.

### Google Play (`GooglePlay`)
- Upload content:
  - Content files (PDF and/or EPUB) into:
    - `ebooks/<collection_code>/...`
  - Metadata file `onix_3.0::google_books` into:
    - `onix/<collection_code>-full/...`
- Preconditions:
  - `google_play_bucket`
  - `google_play_coll_<publisher_id>`
  - GitHub OIDC + GCP service account/workload identity provider secrets
  - At least one of PDF or EPUB required
- Automatic writeback:
  - No location writeback; no notification email step.

### BKCI (`BKCI`)
- Upload content:
  - PDF file
  - Minimal CSV metadata (`Title, ISBN, Publication date, Filename`)
- Preconditions:
  - Per-publisher credentials:
    - `bkci_ftp_user_<publisher_id>`
    - `bkci_ftp_pw_<publisher_id>`
  - PDF with ISBN required
  - Automatic selector excludes textbooks
- Automatic writeback:
  - No location writeback; notification email flow.

## 5) Location writeback and notification model

### Immediate writeback platforms
From `disseminate.yml`, these platforms return location lines and trigger `write_locations.py`:
- `InternetArchive`
- `CUL`
- `Figshare`
- `Zenodo`

### Deferred location writeback
- OAPEN/DOAB locations are handled by `oapen_catchup_locations.yaml`:
  1. find active works with PDF and missing OAPEN location
  2. query OAPEN/DOAB APIs by DOI
  3. write locations to Thoth

### Email notifications
From `disseminate.yml`, email job runs for:
- `OAPEN`, `EBSCOHost`, `BKCI`, `JSTOR`, `ProjectMUSE`, `ProQuest`

## 6) What is distributed: books vs chapters

- Automatic dissemination is currently configured for book-like work types only.
- `BOOK_CHAPTER` is not included in automatic ID-finder filters.
- `ScienceOpen` uploader explicitly states chapter dissemination is not yet implemented.
- `Figshare` metadata mapping contains explicit `BOOK_CHAPTER` type handling, so chapter dissemination is most plausible via manual per-work runs (not via current scheduled bulk selectors).

## 7) Known implementation caveats in current repo state

1. `muse_bulk_disseminate.yaml` passes `platform: 'MUSE'`, but platform matching in `obtain_new_ids.py` and `disseminator.py` expects `ProjectMUSE`. As coded, scheduled Project MUSE bulk runs will not resolve the platform correctly.
2. `ProQuest` uploader intends PDF-or-EPUB support, but it retrieves PDF ISBN before fallback logic, so EPUB-only records can fail early.

## 8) Manual operations

- `manual_disseminate.yml` supports ad-hoc dissemination of explicit work ID arrays to a chosen platform string.
- `disseminator.py` can also be run directly for one work ID and one platform.
- `reconcile_internet_archive.py` supports bounded read-only inspection and
  guarded apply repair for explicit works or a publisher selection. Use it to
  review omitted or ambiguous scheduled-selection records.

## 9) Publisher source modes (`PUBLISHER_SOURCE_MODES`)

Publisher discovery for scheduled dissemination supports three modes,
implemented in `publisher_source.py` and consumed by `obtain_new_ids.py`.

| Mode | Publisher authority | Publisher Services API | Effect on selected works |
|---|---|---|---|
| `env` | environment configuration | not queried | current behaviour |
| `compare` | environment configuration | queried observationally | none |
| `api` | Publisher Services assignments | authoritative, fail-closed | selection follows API assignments |

### Configuration

One non-secret repository-level variable, `PUBLISHER_SOURCE_MODES`, holds a
JSON object keyed by this repository's dissemination platform names:

```json
{
  "OAPEN": "compare",
  "InternetArchive": "compare"
}
```

- a missing variable, an empty variable or a missing platform key means `env`;
- the only accepted values are the exact lower-case strings `env`, `compare`
  and `api`;
- malformed JSON, a non-object value, a non-string mode, an unsupported mode,
  an unknown platform key or a wildcard/default key is a visible configuration
  failure for the affected pathway, never a silent activation;
- no wildcard or default key can activate a non-`env` mode;
- `compare` and `api` are rejected for destinations with no automated
  publisher-discovery pathway (manual-only, pull-feed and inactive).

`.github/workflows/bulk_disseminate.yml` reads
`${{ vars.PUBLISHER_SOURCE_MODES }}` centrally, so the eleven platform-specific
callers need no per-platform configuration.
`.github/workflows/ia_bulk_disseminate.yml` reads the same variable.

The variable does not currently exist. Creating or changing it is a separate
authorized repository-configuration action; merging this source cannot make
`compare` or `api` active.

### Platform mapping

The 17 pinned upstream `DistributionPlatform` values are classified
exhaustively, with no wildcard, `OTHER` or nearest-match fallback. An
unrecognised upstream platform is a contract incompatibility and fails closed.
OAPEN and DOAB are queried and reconciled as two upstream platforms but
projected onto the single existing OAPEN/DOAB execution adapter; a
disagreement between their publisher sets is an error, never a silent union.

### API discovery

Every discovery request uses `publisherCountByDistributionPlatform`, then
pages `publishersByDistributionPlatform` with an explicit positive limit of at
most 100 and the deterministic order `{field: PUBLISHER_ID, direction: ASC}`,
until the result is complete. Publisher UUIDs are normalized, and the fetched
result is reconciled against the count query. A malformed page, a duplicate or
invalid identity, or a count mismatch fails closed. An empty publisher set is
legitimate only when the reported count is zero and the fully consumed result
is empty; in `api` mode that is a successful empty selection which never
broadens to all publishers. Discovery uses only public, anonymous reads and
needs no credential.

### Comparison evidence

`compare` keeps the legacy publisher set authoritative. It never changes the
selected work IDs, the work-ID JSON on stdout or the process exit status: an
API or reporting failure is recorded as comparison status `ERROR` through the
report file, workflow summary and stderr only, and a failed or missing report
is never clean comparison evidence.

The canonical report is written to its own file
(`--comparison-report <path>`), versioned
`thoth-dissemination-publisher-comparison/1`, deterministic, sorted, free of
timestamps and secrets, with bounded sanitized GraphQL diagnostics. Generic
bulk dissemination publishes it as a 30-day workflow artifact plus a step
summary; Internet Archive adds it to the existing 30-day selection diagnostics
artifact and its own summary step. Both reporting steps are non-gating.

### The OAPEN/DOAB location catch-up is unaffected

`obtain_new_ids.py --platform OAPEN --locations` enters forced legacy `env`
behaviour before any mode resolution, so it never reads
`PUBLISHER_SOURCE_MODES` and never calls Publisher Services discovery,
whatever OAPEN's scheduled dissemination mode is.

### Rollout and rollback

Activation is staged separately per pathway: `compare` first, then evidence
review, then `api`. Rollback is a configuration change only - set the affected
platform back to `env` or remove its entry - because `compare` creates no
dissemination, provider, location, email, job or configuration side effect and
cannot suppress an otherwise successful legacy selection.
