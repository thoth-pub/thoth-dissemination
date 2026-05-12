# AGENTS.md

## Repository Overview
- This repository disseminates Thoth work metadata and files to external platforms.
- Main entrypoint: `disseminator.py`.
- Automated ID selection lives in `obtain_new_ids.py`.
- Platform-specific upload logic is split across `*uploader.py` modules.
- GitHub Actions workflows live in `.github/workflows/`.

## Code Structure Notes
- `disseminator.py` dispatches to an uploader class based on `--platform`.
- Adding a new platform usually requires:
  - a new uploader class,
  - updating the platform list in `README.md`,
  - adding any new secrets/variables to `config.env.template`,
  - wiring any workflow support in `.github/workflows/`.
- `bulk_disseminate.yml` is the shared workflow for scheduled dissemination.
- `disseminate.yml` contains the shared job logic, including follow-up tasks like writing locations and sending emails.

## Local Workflow
- Use Python requirements from `requirements.txt` for dissemination code.
- Use `requirements_obtain_new_ids.txt` for ID-selection scripts.
- Use `requirements_write_locations.txt` for location-writing support.
- Copy `config.env.template` to `config.env` for local runs.
- Do not commit real credentials or secrets.

## Validation
- Preferred manual smoke test:
  - `./disseminator.py --work <work_id> --platform <platform>`
- GitHub Actions are the authoritative integration path.
- For action testing, use a fork rather than the production repository when possible.
- Keep in mind scheduled workflows only run if enabled in GitHub Actions.

## Editing Guidelines
- Make the smallest correct change.
- Avoid touching generated, virtualenv, or cache directories.
- Keep comments succinct and only add them where the code is not self-explanatory.
