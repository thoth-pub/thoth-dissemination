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
