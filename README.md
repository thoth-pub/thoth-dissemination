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

### Run with Docker
```sh
docker build . -t ${imagename} # Dockerfile handles Python package requirements
docker run --rm ${imagename} ./disseminator.py --work ${work_id} --platform ${platform}
```

### Options
`--work` = Thoth ID of work to be disseminated

`--platform` = Destination distribution/archiving platform (one of `InternetArchive`, `OAPEN`, `ScienceOpen`, `CUL`, `Crossref`, `Figshare`)

See also `--help`.