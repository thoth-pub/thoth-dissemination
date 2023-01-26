# iabulkupload
The tools in this folder can be used for bulk upload of Thoth publishers' back catalogues to Internet Archive via the Thoth Dissemination Service.
This README records the steps taken to upload the OBP and punctum back catalogues to the [Thoth Archiving Network collection](https://archive.org/details/thoth-archiving-network) on 2022-11-28/29.
See also [the README for the Thoth Dissemination Service itself](https://github.com/thoth-pub/thoth-dissemination/blob/v0.1.0/README.md).

### Steps to upload
1. Check out clean version of Thoth Dissemination Service v0.1.0 to parent folder `thoth-dissemination`.
2. Ensure that the appropriate Internet Archive credentials are present in `../config.env`.
3. In parent folder `thoth-dissemination`, build Thoth Dissemination Service v0.1.0 docker image with name `testdissem` by running
```
docker build . -t testdissem
```
4. Ensure that the desired publisher Thoth IDs (and short names) are present in `./obtain_work_ids.py`.
5. Create lists of Thoth IDs of works to be uploaded by running
```
./obtain_work_ids.py
```
6. For each list, start the upload process by running
```
./bulkupload.sh [publisher]_list.txt 2>> disseminator.log
```
7. Check `./disseminator.log` for any `ERROR` messages. If necessary, cancel the upload process using `ctrl+C`. Once errors are resolved, the upload process can be re-started (successfully uploaded work IDs will be skipped).
8. Once upload process completes, check that all work IDs present in the `./[publisher]_list.txt` files also appear in `./uploaded.txt`.

#### Alternative credentials handling
Instead of filling out `../config.env` in step 2, credentials can be set as environment variables if some changes are made to `./bulkupload.sh`. In place of line 30 (`docker run --rm testdissem ./disseminator.py --work $work_id --platform InternetArchive`), do either of the following:
- pass the credentials directly to the docker container as environment variables:
```
docker run --env ia_s3_secret=[xxx] --env ia_s3_access=[yyy] --rm testdissem ./disseminator.py --work $work_id --platform InternetArchive
```
- use the undockerised run method given in the comment in line 33, having set the credentials as environment variables in the shell (`export ia_s3_secret=[xxx]; export ia_s3_access=[yyy]`):
```
python3 ../disseminator.py --work $work_id --platform InternetArchive
```