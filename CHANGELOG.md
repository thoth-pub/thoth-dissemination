# Changelog
All notable changes to thoth-dissemination will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [[1.7.0]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.7.0) - 2026-08-26

### Added
  - Added API-backed publisher discovery for automated dissemination using Thoth Publisher Services, with per-platform `env`, `compare` and `api` publisher-source modes controlled through the `PUBLISHER_SOURCE_MODES` repository variable.
  - Added `compare` mode for safe rollout: existing environment-configured publishers remain authoritative for dissemination while the Publisher Services API is queried observationally and deterministic comparison reports are produced without changing selected works, stdout or successful exit behaviour.
  - Added `api` mode for eventual cutover to Publisher Services as the authoritative publisher source, with complete paginated discovery, publisher-count reconciliation, strict UUID validation, fail-closed error handling and no fallback to legacy environment configuration.
  - Added runtime validation of the pinned Thoth distribution-platform contract, including the complete supported platform inventory and linked OAPEN/DOAB consistency checks.
  - Added deterministic publisher-source comparison artifacts and workflow summaries with bounded, sanitized diagnostics and 30-day artifact retention.

### Changed
  - Centralised automated dissemination publisher-source selection while preserving the existing platform-specific publisher variables and legacy `ENV_PUBLISHERS` behaviour in the default `env` mode.
  - Kept OAPEN/DOAB location catch-up, manual dissemination and non-automated platform pathways outside Publisher Services cutover so their existing publisher-selection behaviour is unchanged.
  - Hardened Publisher Services error reporting and credential redaction so API and comparison failures cannot expose authorization headers, bearer/basic credentials or other sensitive values.
  - Strengthened repository-local engineering controls for this external-write repository, clearly separating safe local validation from real dissemination, Thoth location write-back, email, provider mutation and release/publication actions.

### Deployment
  - `env` remains the default publisher-source mode after release. `compare` and `api` remain inactive unless `PUBLISHER_SOURCE_MODES` is separately configured, allowing comparison and cutover to be activated per dissemination pathway under the controlled rollout process.

## [[1.6.4]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.6.4) - 2026-07-28
### Fixed
  - Canonicalise Internet Archive managed metadata line endings (collapsing `\r\n` and bare `\r` to `\n`) consistently across desired metadata, the current-state comparison, patches, and final verification, and deduplicate repeatable values (`collection`, `creator`, `isbn`, `subject`, `language`, `issn`) that collapse to the same stored string while preserving first-occurrence order, so a value differing only by line ending (e.g. a subject supplied as both `Ancient\r\nGreek Thought` and `Ancient\nGreek Thought`) is no longer treated as a perpetual metadata discrepancy that blocks convergence
  - Defer the Internet Archive JSON sidecar upload during reconciliation until after the Thoth location has been created or updated: because the `json::thoth` export embeds the publication's locations, a sidecar built and uploaded before the location mutation is immediately stale and needed a second apply to converge. Reconciliation now uploads and strictly verifies the PDF and managed metadata, mutates the Thoth location, rebuilds the desired state from a fresh post-location export (confirming the PDF MD5 is unchanged), then uploads the sidecar exactly once and verifies its final remote MD5; the dry-run transparently predicts the post-location `upload_json_original`, so a first-time create now converges in a single apply across a fresh invocation with no hidden mutation and no duplicate JSON upload. Partial-failure boundaries are preserved: a PDF/metadata verification failure prevents the location mutation and JSON upload, a location mutation failure prevents the JSON upload, a post-location rebuild failure or PDF-source drift is reported with the location applied and the JSON not uploaded, and an accepted-but-pending JSON original uses the existing bounded propagation verification without re-uploading

## [[1.6.3]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.6.3) - 2026-07-27
### Fixed
  - Normalise the Internet Archive JSON sidecar into a deterministic canonical representation before upload and checksum calculation, stripping only the volatile top-level `jsonGeneratedAt` generation timestamp, so semantically unchanged Thoth metadata no longer produces a different expected JSON MD5 on every run and managed JSON originals can converge to `current`
  - Preserve exact JSON numbers when canonicalising the Internet Archive sidecar: fractional tokens are parsed with `parse_float=Decimal` and re-serialised as unquoted JSON numbers by an explicit canonical encoder, so a high-precision metadata value (e.g. `9007199254740993.0`) is no longer silently rounded through a binary float and distinct source values can no longer collapse to the same expected MD5; non-standard `NaN`/`Infinity` constants are now rejected during parsing before any non-finite float is constructed

## [[1.6.2]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.6.2) - 2026-07-27
### Fixed
  - Improved Internet Archive upload verification to handle delayed original-file propagation

## [[1.6.1]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.6.1) - 2026-07-24
### Fixed
  - Exclude Internet Archive-derived metadata (`imagecount`) from apply-time final-state verification, matching read-only inspection, so metadata repairs converge instead of timing out (and no longer abort before Thoth location creation) on items where Internet Archive holds a derived value

## [[1.6.0]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.6.0) - 2026-07-24
### Changed
  - Request canonical work titles and abstracts as plain text through `thothlibrary`'s `markup_format` argument at the shared metadata fetch, so Internet Archive and other shared-GraphQL consumers (BKCI CSV, SWORD Dublin Core profiles) receive plain text rather than JATS markup; Crossref and ONIX are unaffected as they use the separate Export API
  - Removed the downstream markup handling this replaces (the `thothapi` query monkey-patch, the Internet Archive comparison-time tag stripping, and the OAPEN `STRIP_TAGS` regex)
  - Require `thothlibrary` 1.2.0
  - Set the dissemination service version to the next feature release, 1.6.0
### Fixed
  - Treat the Internet Archive-derived `imagecount` metadata field as owned by Internet Archive: seed it on item creation but accept its derived value instead of repeatedly patching it, so items converge and bulk-dissemination verification no longer times out
  - Stop reporting Internet Archive title and abstract metadata as stale when it differs only by the markup Internet Archive strips on storage

## [[1.5.0]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.5.0) - 2026-07-23
### Added
  - Added a manual, dry-run-by-default Internet Archive reconciliation workflow with bounded input validation, protected `develop`-only apply mode, diagnostic artifacts, and Step Summaries
  - Added a read-only-by-default Internet Archive reconciliation CLI with explicit and publisher selection provenance, deterministic JSON/JSONL reports, and guarded apply mode
  - Added deterministic scheduled Internet Archive selection reports, 30-day diagnostic artifacts, Step Summaries, and a visible final overflow guard

### Changed
  - Made Internet Archive dissemination idempotently converge managed originals and metadata, verify their final remote state in one bounded polling loop, and preserve unrelated Archive data
  - Changed scheduled Internet Archive dissemination from monthly publication-date selection to daily `updatedAtWithRelations` selection using a captured 30-hour UTC window, a 200-work oldest-first cap, and four-way parallelism
  - Prevent simultaneous ordinary dissemination of the same platform/work pair while allowing different works and platforms to proceed independently
  - Emit compact valid JSON arrays from all automatic ID finders
  - Distinguished mutable Internet Archive metadata from initial-only `mediatype` and administrator-only collection state in reconciliation reports
  - Made Thoth location write-back converge create, update, and no-op states while preserving canonical and checksum data
  - Report all reconciliation recommendations separately from safe automatic, attempted, applied, and uncertain actions
  - Set the dissemination service version to the next feature release, 1.5.0

### Fixed
  - Refresh and reclassify Internet Archive item ownership immediately before the first repair mutation
  - Send explicit GraphQL nulls when clearing nullable location full-text URLs
  - Keep reconciliation JSON/JSONL stdout machine-readable during Thoth location mutations while preserving explicit standalone location ID output
  - Load local `config.env` values before reconciliation apply credential validation without overriding exported environment variables
  - Reinspect remote state after an Archive repair succeeds but the following Thoth location mutation fails
  - Refuse to create or repair an Internet Archive item unless its missing identifier is explicitly reported as available
  - Block automatic Archive and Thoth mutations when an existing item has missing or incompatible initial-only `mediatype` metadata
  - Stop automatic collection membership patches and report existing items outside the Thoth Archiving Network collection for manual Internet Archive administrator coordination

## [[1.4.1]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.4.1) - 2026-07-20
### Fixed
  - Limit number of concurrent configurations to 250 in oapen\_catchup\_locations.yml

## [[1.4.0]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.4.0) - 2026-07-17
### Fixed
  - Made oapen-catchup-locations workflow idempotent per platform: each publication's missing platforms (OAPEN/DOAB) are detected independently, and only locations for actually missing platforms are created; duplicate-location errors are handled as no-ops

### Changed
  - `OapenLocationsIDFinder.post_process()` emits 3-tuples `(publication_id, doi, missing_platforms)` instead of 2-tuples
  - `obtain_oapen_locations.py` only queries each API when that platform is missing; health-check is per-platform
  - `write_locations.py` treats duplicate-platform ThothError as an idempotent skip and preserves original error message for other failures

## [[1.3.2]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.3.1) - 2026-06-08
### Changed
  - Added JPEG cover image file to EBSCOHost automated upload
  - Upgraded GitHub Actions dependency `dawidd6/action-send-mail` from Node 20 to 24

## [[1.3.1]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.3.1) - 2026-06-03
### Fixed
  - Fixed bugs preventing automatic location writing for Project MUSE and Internet Archive

## [[1.3.0]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.3.0) - 2026-06-01
### Added
  - GitHub Action for recurring automatic check and update of Project MUSE locations not yet listed in Thoth
### Changed
  - Upgraded GitHub Actions dependencies from Node 20 to 24 (`docker/setup-qemu-action@v4`, `docker/setup-buildx-action@v4`, `docker/login-action@v4`, `docker/build-push-action@v7`, `docker/metadata-action@v6`, `actions/upload-artifact@v7`, `actions/download-artifact@v8`, `actions/checkout@v6`, `actions/setup-python@v6`, `oNaiPs/secrets-to-env-action@v1.8`, `google-github-actions/auth@v3`)

## [[1.2.2]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.2.2) - 2026-05-12
### Changed
  - Internet Archive automatic dissemination now runs monthly and only picks up newly-published works, bringing it in line with other archive platforms
### Fixed
  - Fixed oapen-catchup-locations bug by adding missing checksum fields to locations lists

## [[1.2.1]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.2.1) - 2026-05-07
### Fixed
  - Updated thothlibrary dependency to v1.1.2 to include fix for bug in createLocations method

## [[1.2.0]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.2.0) - 2026-05-05
### Changed
  - Enhanced automatic location writing to include checksums returned from IA/Zenodo

## [[1.1.0]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.1.0) - 2026-04-30
### Added
  - [56](https://github.com/thoth-pub/thoth-dissemination/issues/56) - Created an OAPEN profile for SWORD v2 Uploader, to facilitate automatic dissemination of works from Thoth to OAPEN.
### Changed
  - Converted OAPEN automatic dissemination workflows to use SWORDv2 server connection instead of uploading to FTP server
### Fixed
  - Minor improvements to Zenodo workflow

## [[1.0.4]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.0.4) - 2026-04-23
### Fixed
  - Corrected typo in Project MUSE automated workflow

## [[1.0.3]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.0.3) - 2026-04-14
### Fixed
  - Corrected naming of updated credentials

## [[1.0.2]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.0.2) - 2026-04-01
### Fixed
  - Reinstate accidentally deleted import

## [[1.0.1]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.0.1) - 2026-04-01
### Fixed
  - Updated remaining references to deprecated credentials

## [[1.0.0]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v1.0.0) - 2026-04-01
### Changed
  - Updated for compatibility with Thoth v1.0.0

## [[0.1.38]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.38) - 2026-01-09
### Fixed
  - Correct metadata field formatting to comply with changed `internetarchive` dependency behaviour under v5.7.1

## [[0.1.37]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.37) - 2026-01-09
### Fixed
  - Prevent failures to download publisher content files due to firewalls blocking scripts

## [[0.1.36]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.36) - 2026-01-05
### Fixed
  - Minor bugfixes (incomplete environment secret retrieval, incorrect case matching syntax)

## [[0.1.35]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.35) - 2025-12-15
### Added
  - GitHub Actions for recurring automatic uploads of newly published works to EBSCOHost, JSTOR, Project MUSE and ProQuest.
### Fixed
  - Added support for using environment secrets as workaround for exceeding repository secrets limit.
### Changed
  - Upgraded `internetarchive` dependency to v5.7.1

## [[0.1.34]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.34) - 2025-12-03
### Changed
  - Updated Thoth email address used in cc of automated emails.

## [[0.1.33]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.33) - 2025-11-20
### Changed
  - Added publisher name to subject line and body of automated emails sent to platforms.

## [[0.1.32]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.32) - 2025-08-21
### Changed
  - Fixed a bug with GitHub Actions where all ```manual-disseminate``` jobs were returning exit code 1 (failure), whether they actually succeeded or not.

## [[0.1.31]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.31) - 2025-08-13
### Changed
  - Only write `ERROR` logging messages to the GitHub Actions Job Summary, removing `INFO` messages so the Job Summaries are easier to read for batch runs of the `manual-disseminate` workflow.

## [[0.1.30]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.30) - 2025-08-06
### Changed
  - Replaced unmaintained dependency `pysftp` with latest version of `paramiko`

## [[0.1.29]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.29) - 2025-08-04
### Fixed
  - Added explicit import of downgraded version of `paramiko`, as workaround for import error caused by deprecated module in v4.0

## [[0.1.28]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.28) - 2025-08-04
### Changed
  - Write error logging messages from dissemination workflows to GitHub Actions Job Summary section so they are easier to read
  - Added Forthcoming works with DOI and publication date to automatic Crossref DOI deposit

## [[0.1.27]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.27) - 2025-07-10
### Fixed
  - Removed Python import for compatibility with Python 3.10

## [[0.1.26]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.26) - 2025-07-09
### Changed
  - Downgraded `requests` to `2.32.3` to maintain compatibility with `thothlibrary`

## [[0.1.25]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.25) - 2025-07-09
### Added
  - Dissemination workflow for Clarivate Web of Science Book Citation Index (BKCI)

## [[0.1.24]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.24) - 2025-07-02
### Changed
  - Changed Project MUSE filename root for upload from Paperback to PDF ISBN

## [[0.1.23]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.23) - 2025-07-01
### Changed
  - Fixed logic for jpeg file extension for cover image

## [[0.1.22]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.22) - 2025-06-27
### Changed
  - Added support for .jpeg file extension for cover image to avoid error

## [[0.1.21]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.21) - 2025-05-06
### Changed
  - Improved error messages for lack of PDF ISBN in GoogleBooks uploader

## [[0.1.20]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.20) - 2025-03-03
### Added
  - GitHub Action for recurring automatic check and update of OAPEN/DOAB locations not yet listed in Thoth

## [[0.1.19]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.19) - 2025-02-24
### Fixed
  - Fixed date calculation error in OAPEN automatic upload workflow

## [[0.1.18]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.18) - 2025-02-20
### Added
  - GitHub Actions for recurring automatic uploads of newly published works to OAPEN

## [[0.1.17]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.17) - 2024-12-03
### Added
  - Support for uploading files and metadata to Google Cloud server for crawl ingest by Google Play (including recurring automatic uploads)
  - Automatic email notifications on upload for platforms where this is required (OAPEN, EBSCOHost)

## [[0.1.16]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.16) - 2024-10-21
### Changed
  - Finalised JSTOR upload workflow
  - Upgraded dependencies: thothlibrary v0.26.2, internetarchive v4.1.0, requests v2.32.3

## [[0.1.15]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.15) - 2024-08-06
### Added
  - Support for uploading files and metadata to Project MUSE, JSTOR, EBSCOHost, ProQuest (Ebook Central)
### Fixed
  - Minor fixes/improvements to GitHub Actions (job dependencies, environment variables)

## [[0.1.14]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.14) - 2024-07-24
### Changed
  - Upgraded thothlibrary dependency to release v0.26.0 (includes improved error handling)

## [[0.1.13]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.13) - 2024-07-01
### Fixed
  - Fixed bug in GitHub Action causing automatic writing of location info to Thoth to fail

## [[0.1.12]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.12) - 2024-06-06
### Added
  - Support for uploading files and metadata to Zenodo (including recurring automatic uploads)
  - GitHub Actions for recurring automatic uploads of newly published works to CUL
  - Automatic writing of location info to Thoth on successful dissemination

## [[0.1.11]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.11) - 2024-04-03
### Fixed
  - Fixed bug in logic for finding targets for automatic dissemination
### Changed
  - Upgraded GitHub Actions dependency from Node 16 to 20 (`docker/metadata-action@v5`)

## [[0.1.10]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.10) - 2024-03-27
### Changed
  - Enhanced basic SWORD v2 (DSpace v7) functionality to fit CUL requirements
  - Upgraded GitHub Actions dependencies from Node 16 to 20 (`docker/setup-qemu-action@v3`, `docker/setup-buildx-action@v3`, `docker/login-action@v3`, `docker/build-push-action@v5`, `actions/checkout@v4`, `actions/setup-python@v5`, `oNaiPs/secrets-to-env-action@v1.5`)

## [[0.1.9]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.9) - 2023-12-05
### Added
  - GitHub Actions for recurring automatic uploads of newly published works to Figshare

## [[0.1.8]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.8) - 2023-08-23
### Changed
  - Changed recurring Crossref DOI deposit from daily to hourly

## [[0.1.7]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.7) - 2023-07-03
### Changed
  - Made existing GitHub Actions more general/reliable

## [[0.1.6]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.6) - 2023-06-28
### Fixed
  - Amended Crossref credential environment variables naming to allow running in GitHub Actions

## [[0.1.5]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.5) - 2023-06-21
### Added
  - Support for uploading files and metadata to Figshare
  - Support for sending DOI deposit files to Crossref
  - GitHub Actions for recurring automatic DOI deposit to Crossref
### Changed
  - Reworked existing GitHub Actions for greater extensibility
  - Added support for content file types other than PDF
  - Improved error handling

## [[0.1.4]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.4) - 2023-02-01
### Fixed
  - Corrected GitHub Actions syntax to pick up environment variables stored as repository secrets

## [[0.1.3]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.3) - 2023-01-30
### Added
  - GitHub Actions for recurring automatic uploads of newly published works to Internet Archive

## [[0.1.2]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.2) - 2022-12-13
### Changed
  - Fix Dockerfile for compatibility with GitHub Actions

## [[0.1.1]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.1) - 2022-12-12
### Added
  - Automatic publishing of release images to Dockerhub

## [[0.1.0]](https://github.com/thoth-pub/thoth-dissemination/releases/tag/v0.1.0) - 2022-11-23
### Added
  - Basic functionality for uploading files and metadata to Internet Archive, OAPEN, ScienceOpen, and platforms using SWORD v2 (as implemented for DSpace v7)
