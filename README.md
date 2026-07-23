# thoth-dissemination
Dissemination of work metadata and files from Thoth to distribution/archiving platforms.

## Usage

### Config
```sh
git clone https://github.com/thoth-pub/thoth-dissemination.git
cd thoth-dissemination
cp config.env.template config.env
```
Fill out `config.env` with credentials for desired platforms.

### Run with Python
```sh
pip3 install -r ./requirements.txt
```
```python
./disseminator.py --work ${work} --platform ${platform}
```

### Run with Docker (locally)
```sh
docker build . -t ${imagename} # Dockerfile handles Python package requirements
docker run --rm ${imagename} ./disseminator.py --work ${work_id} --platform ${platform}
```

### Run with Docker (from Dockerhub)
```sh
docker run --rm --env-file config.env openbookpublishers/thoth-dissemination:latest ./disseminator.py --work ${work_id} --platform ${platform}
```

### Options
`--work` = Thoth ID of work to be disseminated

`--platform` = Destination distribution/archiving platform (one of `InternetArchive`, `OAPEN`, `ScienceOpen`, `CUL`, `Crossref`, `Figshare`, `Zenodo`, `ProjectMUSE`, `JSTOR`, `EBSCOHost`, `ProQuest`, `GooglePlay`, `BKCI`)

See also `--help`.

### Scheduled Internet Archive dissemination

The **ia-bulk-disseminate** workflow runs daily at 04:40 UTC. It selects
configured-publisher works whose `updatedAtWithRelations` timestamp is within
one captured UTC window: greater than the start and less than or equal to the
end. The default 30-hour lookback creates a six-hour overlap between ordinary
daily runs; manual runs may use a positive lookback up to 168 hours.

Selection requires `IA_ENV_PUBLISHERS` to be a non-empty JSON array of
publisher UUIDs. `IA_ENV_EXCEPTIONS` is an optional JSON array of work UUIDs
to exclude. Configured publishers and exceptions are validated and normalised
before the read-only query runs.

Only active `MONOGRAPH`, `EDITED_BOOK`, `JOURNAL_ISSUE`, `TEXTBOOK`, and
`BOOK_SET` records with a PDF publication and a non-empty canonical PDF
`fullTextUrl` are eligible. Selection does not download the PDF, request an
export, or make a request to the source URL. Eligible records are ordered by
oldest relation-aware update and then work UUID. At most 200 are selected and
four different works disseminate concurrently.

Every run writes a concise Step Summary and retains a 30-day
`ia-selection-<run>-<attempt>` artifact containing the machine-readable report
and sanitised selection log. If more than 200 works are eligible, all omitted
records are reported; the bounded selected batch finishes, then the final
workflow job fails visibly. Inspect the artifact and use bounded manual
reconciliation or another reviewed bounded operation for omitted or ambiguous
records.

The reusable dissemination workflow prevents simultaneous ordinary runs for
the same platform/work pair. Different works and platforms remain independent.
Because Internet Archive upload and Thoth location write-back are idempotent,
an unchanged selected work safely converges as a no-op.

### Reconcile Internet Archive state

Inspect one or more works without write credentials or remote mutations:

```sh
python3 reconcile_internet_archive.py --work-id ${work_id}
python3 reconcile_internet_archive.py --publisher-id ${publisher_id} --limit 100
```

Reports are deterministic JSON by default. Use `--format jsonl`, `--output
<path>`, or `--apply` to select JSONL output, write an artifact, or apply safe
repairs. JSON and JSONL stdout contain report data only; operational messages
and successful location mutation IDs are logged to stderr. With `--output`, the
report is written only to the requested file and stdout remains empty.

Local CLI runs load `ia_s3_access`, `ia_s3_secret`, and `THOTH_PAT` from
`./config.env`; already-exported process environment values take precedence.
Apply mode requires all three credentials, while dry-run requires none. GitHub
Actions continues to supply write credentials through the protected
`disseminate` environment rather than a local configuration file.

New Archive items are created with initial `mediatype: texts` metadata and
membership in the `thoth-archiving-network` collection; final verification
confirms both invariants. `mediatype` is initial-upload-only, while existing
collection membership can be changed only by Internet Archive administrators.
If an existing Thoth-owned item has an incompatible `mediatype` or does not
include the Thoth collection, reconciliation reports a `metadata_conflict`
with separate initial-only and admin-only details and a manual recommendation.
Direct dissemination and apply mode do not upload files, modify Archive
metadata, add ownership markers, or write Thoth locations for that work while
the conflict remains. Collection remediation requires manual coordination with
Internet Archive; the reconciler reports but does not mutate these records and
does not prescribe deleting or recreating the item.

The **internet-archive-reconciliation** workflow can also be launched manually
from the repository's **Actions** tab. Dry-run is the default and receives no
Archive or Thoth write credentials. Select works with an optional publisher
UUID, comma- or newline-separated work UUIDs, or both. Blank work entries are
ignored and duplicates are removed. The workflow rejects batches that could
exceed 200 works; use 50-100 works for normal runs.

Every run uploads a 30-day diagnostic artifact and writes aggregate counts to
the GitHub Step Summary, including when validation or reconciliation fails.
Review a recent dry-run artifact before applying repairs. Apply runs require
`confirm_apply` to be exactly `APPLY`, must run from `develop`, and wait for
approval through the protected `disseminate` GitHub environment. Configure
required reviewers on that environment in repository settings; workflow YAML
cannot create the protection rule. Apply execution maps only the
`IA_S3_ACCESS`, `IA_S3_SECRET`, and `THOTH_PAT` write credentials into the
workflow job.
