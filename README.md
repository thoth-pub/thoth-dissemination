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

### Reconcile Internet Archive state

Inspect one or more works without write credentials or remote mutations:

```sh
python3 reconcile_internet_archive.py --work-id ${work_id}
python3 reconcile_internet_archive.py --publisher-id ${publisher_id} --limit 100
```

Reports are deterministic JSON by default. Use `--format jsonl`, `--output
<path>`, or `--apply` to select JSONL output, write an artifact, or apply safe
repairs. Apply mode requires `ia_s3_access`, `ia_s3_secret`, and `THOTH_PAT`.

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
