#!/usr/bin/env python3

"""
This script generates the UDF which uses Micropython to be used in BigQuery.
Supporting files can optionally be automatically pushed into a specified
Google Cloud Storage bucket. A Python file can be provided whose code will
be used used in the generated UDF.
"""

import os
from argparse import ArgumentParser
from os import listdir
from os.path import isfile, join

from google.cloud import bigquery
from google.cloud import storage

parser = ArgumentParser(description=__doc__)

parser.add_argument(
    "--bq-dataset",
    default="",
    required=False,
    help="The dataset in Google Big Query where the fonction should be created.",
)
parser.add_argument(
    "--gcs-bucket",
    default="",
    required=True,
    help="The bucket in Google Cloud storage where the micropython should be uploaded to.",
)
parser.add_argument(
    "--gcs-path",
    default="",
    required=False,
    help="The path in Google Cloud storage where the micropython should be uploaded to.",
)


def push_files_to_gcs(gcs_bucket, gcs_path, files):
    """
    Automatically push provided files to Google cloud storage.
    """
    client = storage.Client()
    bucket = client.get_bucket(gcs_bucket)

    for file in files:
        filename = os.path.basename(file)
        blob = bucket.blob(gcs_path + filename)
        blob.upload_from_filename(file)


def push_query_to_bq(bq_dataset, files):
    """
    Automatically push provided files to Google cloud storage.
    """
    sql_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
    sql_filenames = [join(sql_dir, f) for f in listdir(sql_dir) if isfile(join(sql_dir, f))]

    client = bigquery.Client()

    for sql_filename in sql_filenames:
        with open(sql_filename) as sql_file:
            sql_query = sql_file.read()

        query_job = client.query(sql_query)

        results = query_job.result()  # Waits for job to complete.


def generate_exemplary_udf(gcs_bucket, gcs_path, files):
    """
    Generates an exemplary UDF that integrates Micropython.
    """

    external_files = ",\n".join(list(map(
        lambda f: 'library = "gs://{}/{}{}"'.format(
            gcs_bucket, gcs_path, os.path.basename(f)
        ),
        files,
    )))

    return """
CREATE TEMP FUNCTION
udf_func()
RETURNS STRING
LANGUAGE js AS \"\"\"
    mp_js_init(64 * 1024);
    const pythonCode = `{}`;
    return mp_js_exec_str(pythonCode);
\"\"\"
OPTIONS (
{});
SELECT
    udf_func()
FROM (
SELECT
1 x,
2 y)
    """.format(external_files)


def main():
    args = parser.parse_args()

    push_files_to_gcs(args.gcs_bucket, args.gcs_path)
    push_query_to_bq(args.bq_dataset)

    gcs_bucket = args.gcs_bucket

    print(
        generate_exemplary_udf(args.gcs_bucket, args.gcs_path)
    )


if __name__ == "__main__":
    main()
