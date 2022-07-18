# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import csv
import datetime
import gzip
import json
import logging
import os
import pathlib
import shutil
import typing
from urllib.parse import urlparse

import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage


def main(
    source_url: str,
    pipeline_name: str,
    source_file_zipfile: pathlib.Path,
    delete_zipfile: bool,
    source_file: pathlib.Path,
    target_file: pathlib.Path,
    chunksize: str,
    target_gcs_bucket: str,
    target_gcs_path: str,
    project_id: str,
    dataset_id: str,
    destination_table: str,
    schema_filepath: str,
    normalize_fields: typing.List[dict]
) -> None:

    logging.info(f"{pipeline_name} process started")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)
    execute_pipeline(
        source_url=source_url,
        source_file_zipfile=source_file_zipfile,
        delete_zipfile=(delete_zipfile.title() == "True"),
        source_file=source_file,
        target_file=target_file,
        chunksize=chunksize,
        target_gcs_bucket=target_gcs_bucket,
        target_gcs_path=target_gcs_path,
        project_id=project_id,
        dataset_id=dataset_id,
        destination_table=destination_table,
        schema_filepath=schema_filepath,
        normalize_fields=normalize_fields
    )
    logging.info(f"{pipeline_name} process completed")


def execute_pipeline(
    source_url: str,
    source_file_zipfile: pathlib.Path,
    delete_zipfile: bool,
    source_file: pathlib.Path,
    target_file: pathlib.Path,
    chunksize: str,
    target_gcs_bucket: str,
    target_gcs_path: str,
    project_id: str,
    dataset_id: str,
    destination_table: str,
    schema_filepath: str,
    normalize_fields: typing.List[dict]
) -> None:
    # download_file(source_url, source_file_zipfile)
    extract_path = os.path.split(source_file_zipfile)[0]
    unzip_path = str.replace(os.path.split(source_file_zipfile)[1], ".tar.gz", "")
    # shutil.unpack_archive(filename=source_file_zipfile, extract_dir=f"{extract_path}/{unzip_path}")
    if delete_zipfile:
        os.remove(source_file_zipfile)
    process_source_file(
        source_file=source_file,
        target_file=target_file,
        chunksize=chunksize,
        normalize_fields=normalize_fields
    )
    import pdb; pdb.set_trace()
    # if os.path.exists(target_file):
    #     upload_file_to_gcs(
    #         file_path=target_file,
    #         target_gcs_bucket=target_gcs_bucket,
    #         target_gcs_path=target_gcs_path,
    #     )
    #     table_exists = create_dest_table(
    #         project_id=project_id,
    #         dataset_id=dataset_id,
    #         table_id=destination_table,
    #         schema_filepath=schema_path,
    #         bucket_name=target_gcs_bucket,
    #     )
    #     if table_exists:
    #         delete_source_file_data_from_bq(
    #             project_id=project_id,
    #             dataset_id=dataset_id,
    #             table_id=destination_table,
    #             source_url=source_url,
    #         )
    #         load_data_to_bq(
    #             project_id=project_id,
    #             dataset_id=dataset_id,
    #             table_id=destination_table,
    #             file_path=target_file,
    #             truncate_table=False,
    #             field_delimiter="|",
    #         )
    #     else:
    #         error_msg = f"Error: Data was not loaded because the destination table {project_id}.{dataset_id}.{destination_table} does not exist and/or could not be created."
    #         raise ValueError(error_msg)
    # else:
    #     logging.info(
    #         f"Informational: The data file {target_file} was not generated because no data file was available.  Continuing."
    #     )


def process_source_file(
    source_file: str,
    target_file: str,
    chunksize: str,
    normalize_fields: typing.List[dict]
) -> None:
    logging.info(f"Processing source file {source_file}")
    for field_metadata in normalize_fields:
        import pdb; pdb.set_trace()

    # for field_metadata in normalize_fields:
    #     if field_metadata["root_data"] == "True":
    #         df = field_metadata["json_field"].apply(lambda x: pd.json_normalize(x, sep="_"))[ordinal][:]
    #     import pdb; pdb.set_trace()
        # df_source = load_json_file_into_dataframe(json_filepath=source_file)
        # df = explode_json_column_dataframe(
        #         df = df_source,
        #         id_col = id_col,
        #         json_col = json_col,
        #         ordinal = 0,
        #         join_datasets = True
        #     )
    # csv.field_size_limit(512 << 10)
    # csv.register_dialect("TabDialect", quotechar='"', delimiter="\t", strict=True)
    # with open(
    #     source_file,
    # ) as reader:
    #     data = []
    #     chunk_number = 1
    #     for index, line in enumerate(csv.reader(reader, "TabDialect"), 0):
    #         data.append(line)
    #         if index % int(chunksize) == 0 and index > 0:
    #             process_dataframe_chunk(
    #                 data,
    #                 input_headers,
    #                 data_dtypes,
    #                 target_file,
    #                 chunk_number,
    #                 datetime_list,
    #                 null_string_list,
    #                 source_file,
    #                 source_url,
    #             )
    #             data = []
    #             chunk_number += 1

    #     if data:
    #         process_dataframe_chunk(
    #             data,
    #             input_headers,
    #             data_dtypes,
    #             target_file,
    #             chunk_number,
    #             datetime_list,
    #             null_string_list,
    #             source_file,
    #             source_url,
    #         )


def gz_decompress(infile: str, tofile: str, delete_zipfile: bool = False) -> None:
    logging.info(f"Decompressing {infile}")
    with open(infile, "rb") as inf, open(tofile, "w", encoding="utf8") as tof:
        decom_str = gzip.decompress(inf.read()).decode("utf-8")
        tof.write(decom_str)
    if delete_zipfile:
        os.remove(infile)


def explode_json_column_dataframe(
    df: pd.DataFrame,
    id_col: str,
    json_col: str,
    ordinal: int = 0,
    join_datasets: bool = False
) -> pd.DataFrame:
    logging.info(f"Extracting JSON column {json_col} using id column {id_col}")
    df_data = df[json_col].apply(lambda x: pd.json_normalize(x, sep="_"))[ordinal][:]
    df_data_exploded = df_data.explode(json_col)
    df_data_return = pd.concat([df_data_exploded[id_col].reset_index(drop=True), pd.json_normalize(df_data_exploded[json_col])], axis=1)
    if join_datasets:
        df_data_return = pd.merge(df_data, df_data_return, how='outer', on=id_col)
    return df_data_return


def load_json_file_into_dataframe(
    json_filepath: str
) -> pd.DataFrame:
    with open(json_filepath) as json_file:
        data = json.load(json_file)
        df = pd.json_normalize(data)
        return df


def process_dataframe_chunk(
    data: typing.List[str],
    input_headers: typing.List[str],
    data_dtypes: dict,
    target_file: str,
    chunk_number: int,
    datetime_list: typing.List[str],
    null_string_list: typing.List[str],
    source_file: str,
    source_url: str,
) -> None:
    df = pd.DataFrame(data, columns=input_headers)
    set_df_datatypes(df, data_dtypes)
    target_file_batch = str(target_file).replace(
        ".csv", "-" + str(chunk_number) + ".csv"
    )
    process_chunk(
        df=df,
        target_file_batch=target_file_batch,
        target_file=target_file,
        skip_header=(not chunk_number == 1),
        datetime_list=datetime_list,
        null_string_list=null_string_list,
        source_file=source_file,
        source_url=source_url,
    )


def set_df_datatypes(df: pd.DataFrame, data_dtypes: dict) -> pd.DataFrame:
    logging.info("Setting data types")
    for key, item in data_dtypes.items():
        df[key] = df[key].astype(item)
    return df


def process_chunk(
    df: pd.DataFrame,
    target_file_batch: str,
    target_file: str,
    skip_header: bool,
    datetime_list: typing.List[str],
    null_string_list: typing.List[str],
    source_file: str,
    source_url: str,
) -> None:
    logging.info(f"Processing batch file {target_file_batch}")
    df = add_metadata_columns(df, source_file, source_url)
    save_to_new_file(df, file_path=str(target_file_batch), sep="|")
    append_batch_file(target_file_batch, target_file, skip_header, not (skip_header))
    logging.info(f"Processing batch file {target_file_batch} completed")


def add_metadata_columns(
    df: pd.DataFrame, source_file: str, source_url: str
) -> pd.DataFrame:
    df["etl_timestamp"] = datetime.datetime.now()
    df["source_file"] = os.path.split(source_file)[1]
    df["source_url"] = source_url
    return df


def download_file(source_url: str, source_file: pathlib.Path) -> None:
    logging.info(f"downloading file {source_file} from {source_url}")
    r = requests.get(source_url, stream=True)
    with open(source_file, "wb") as f:
        for chunk in r:
            f.write(chunk)


def load_data_to_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    file_path: str,
    truncate_table: bool,
    field_delimiter: str = "|",
) -> None:
    logging.info(
        f"Loading data from {file_path} into {project_id}.{dataset_id}.{table_id} started"
    )
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = field_delimiter
    if truncate_table:
        job_config.write_disposition = "WRITE_TRUNCATE"
    else:
        job_config.write_disposition = "WRITE_APPEND"
    job_config.skip_leading_rows = 1  # ignore the header
    job_config.autodetect = False
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    job.result()
    logging.info(
        f"Loading data from {file_path} into {project_id}.{dataset_id}.{table_id} completed"
    )


def create_dest_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    schema_filepath: list,
    bucket_name: str,
) -> bool:
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    logging.info(f"Attempting to create table {table_ref} if it doesn't already exist")
    client = bigquery.Client()
    table_exists = False
    try:
        table = client.get_table(table_ref)
        table_exists_id = table.table_id
        logging.info(f"Table {table_exists_id} currently exists.")
    except NotFound:
        table = None
    if not table:
        logging.info(
            (
                f"Table {table_ref} currently does not exist.  Attempting to create table."
            )
        )
        if check_gcs_file_exists(schema_filepath, bucket_name):
            schema = create_table_schema([], bucket_name, schema_filepath)
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Table {table_ref} was created".format(table_id))
            table_exists = True
        else:
            file_name = os.path.split(schema_filepath)[1]
            file_path = os.path.split(schema_filepath)[0]
            logging.info(
                f"Error: Unable to create table {table_ref} because schema file {file_name} does not exist in location {file_path} in bucket {bucket_name}"
            )
            table_exists = False
    else:
        table_exists = True
    return table_exists


def check_gcs_file_exists(file_path: str, bucket_name: str) -> bool:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    exists = storage.Blob(bucket=bucket, name=file_path).exists(storage_client)
    return exists


def delete_source_file_data_from_bq(
    project_id: str, dataset_id: str, table_id: str, source_url: str
) -> None:
    logging.info(
        f"Deleting data from {project_id}.{dataset_id}.{table_id} where source_url = '{source_url}'"
    )
    client = bigquery.Client(project=project_id)
    query_delete = f"""
        DELETE
        FROM {project_id}.{dataset_id}.{table_id}
        WHERE source_url = '{source_url}'
    """
    query_job = client.query(query_delete)  # Make an API request.
    query_job.result()


def create_table_schema(
    schema_structure: list, bucket_name: str = "", schema_filepath: str = ""
) -> list:
    logging.info(f"Defining table schema... {bucket_name} ... {schema_filepath}")
    schema = []
    if not (schema_filepath):
        schema_struct = schema_structure
    else:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(schema_filepath)
        schema_struct = json.loads(blob.download_as_string(client=None))
    for schema_field in schema_struct:
        fld_name = schema_field["name"]
        fld_type = schema_field["type"]
        try:
            fld_descr = schema_field["description"]
        except KeyError:
            fld_descr = ""
        fld_mode = schema_field["mode"]
        schema.append(
            bigquery.SchemaField(
                name=fld_name, field_type=fld_type, mode=fld_mode, description=fld_descr
            )
        )
    return schema


def save_to_new_file(df: pd.DataFrame, file_path: str, sep: str = "|") -> None:
    logging.info(f"Saving data to target file.. {file_path} ...")
    df.to_csv(file_path, index=False, sep=sep)


def append_batch_file(
    batch_file_path: str, target_file_path: str, skip_header: bool, truncate_file: bool
) -> None:
    with open(batch_file_path, "r") as data_file:
        if truncate_file:
            target_file = open(target_file_path, "w+").close()
        with open(target_file_path, "a+") as target_file:
            if skip_header:
                logging.info(
                    f"Appending batch file {batch_file_path} to {target_file_path} with skip header"
                )
                next(data_file)
            else:
                logging.info(
                    f"Appending batch file {batch_file_path} to {target_file_path}"
                )
            target_file.write(data_file.read())
            if os.path.exists(batch_file_path):
                os.remove(batch_file_path)


def upload_file_to_gcs(
    file_path: pathlib.Path, target_gcs_bucket: str, target_gcs_path: str
) -> None:
    if os.path.exists(file_path):
        logging.info(
            f"Uploading output file to gs://{target_gcs_bucket}/{target_gcs_path}"
        )
        storage_client = storage.Client()
        bucket = storage_client.bucket(target_gcs_bucket)
        blob = bucket.blob(target_gcs_path)
        blob.upload_from_filename(file_path)
    else:
        logging.info(
            f"Cannot upload file to gs://{target_gcs_bucket}/{target_gcs_path} as it does not exist."
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    main(
        source_url=os.environ.get("SOURCE_URL", ""),
        pipeline_name=os.environ.get("PIPELINE_NAME", ""),
        source_file_zipfile=pathlib.Path(os.environ.get("SOURCE_FILE_ZIPFILE", "")).expanduser(),
        delete_zipfile=os.environ.get("CHUNKSIZE", "False"),
        source_file=pathlib.Path(os.environ.get("SOURCE_FILE", "")).expanduser(),
        target_file=pathlib.Path(os.environ.get("TARGET_FILE", "")).expanduser(),
        chunksize=os.environ.get("CHUNKSIZE", "1000000"),
        target_gcs_bucket=os.environ.get("TARGET_GCS_BUCKET", ""),
        target_gcs_path=os.environ.get("TARGET_GCS_PATH", ""),
        project_id=os.environ.get("PROJECT_ID", ""),
        dataset_id=os.environ.get("DATASET_ID", ""),
        destination_table=os.environ.get("DESTINATION_TABLE", ""),
        schema_filepath=os.environ.get("SCHEMA_FILEPATH", ""),
        normalize_fields=json.loads(os.environ.get("NORMALIZE_FIELDS", r"[]"))
    )
