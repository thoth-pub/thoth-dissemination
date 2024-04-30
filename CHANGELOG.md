# Changelog
All notable changes to thoth-dissemination will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
  - Support for uploading files and metadata to Zenodo

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
