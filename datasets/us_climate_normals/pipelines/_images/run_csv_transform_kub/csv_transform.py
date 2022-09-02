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

# from distutils.command.upload import upload
import glob
from http.client import BAD_REQUEST
import json
import logging
import math
import os
import pathlib
import typing
import datetime

import pandas as pd
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
import google.api_core.exceptions as google_api_exceptions
from airflow import DAG
import airflow.providers.google.cloud.transfers.gcs_to_bigquery as gcs2bq


def main(
    project_id: str,
    dataset_id: str,
    target_gcs_bucket:str
) -> None:
    # logging.info(f"{pipeline_name} process started")

    source_local_folder_root = f'/usr/local/google/home/nlarge/Desktop/test_us_climate_normals'
    root_pipeline_gs_folder = "normals-hourly"
    folders_list = ['', '/1981-2010', '/1991-2010', '/2006-2020']
    file_prefix = [
        # 'AQC', 'AQW', 'CAW', 'CQC', 'FMC', 'FMW',
        # 'GQC', 'GQW', 'JQW', 'MQW', 'PSC', 'PSW',
        # 'RMC', 'RMW', 'RQC', 'RQW', 'USC', 'USW',
        # 'VQC', 'VQW', 'WQW'
        'USW'
    ]
    source_local_schema_folder = f'{source_local_folder_root}/schema'
    for fldr in folders_list:
        for prefix in file_prefix:
            logging.info(f"Processing {prefix} files in {fldr}/access folder ...")
            source_local_process_folder_root = f'{source_local_folder_root}/{root_pipeline_gs_folder}'
            folder_to_process = f'{source_local_process_folder_root}{fldr}/access'
            if not prefix_files_exist(prefix, folder_to_process):
                logging.info(f" ... No files exist with {prefix} prefix in folder {folder_to_process}.  Skipping.")
            else:
                first_file_path = return_first_file_for_prefix(prefix, folder_to_process)
                if fldr == "":
                    fldr_ident = "access"
                else:
                    fldr_ident = str.replace(fldr, "/", "")
                destination_table=f'normals_hourly_{prefix}_{fldr_ident.replace("-", "_")}'
                schema_filepath_gcs_path=f'data/us_climate_normals/schema/normals_hourly/{fldr}'
                output_schema_file=f"{source_local_schema_folder}/{destination_table}_schema.json"
                schema_filepath = f'{schema_filepath_gcs_path}{fldr_ident}/{ os.path.basename(output_schema_file) }'
                # if not table_exists(project_id, dataset_id, destination_table):
                #     # filename = return_first_file_for_prefix(prefix, folder_to_process)
                #     if not gcs_file_exists(bucket=target_gcs_bucket, file_path=output_schema_file):
                #         generate_schema_file_from_source_file(
                #             filename=first_file_path,
                #             output_schema_file=output_schema_file,
                #             copy_schema_file_to_gcs=True,
                #             schema_filepath_bucket=target_gcs_bucket,
                #             schema_filepath_gcs_path=schema_filepath
                #         )
                #     create_dest_table(
                #         project_id=project_id,
                #         dataset_id=dataset_id,
                #         table_id=destination_table,
                #         schema_filepath=schema_filepath,
                #         bucket_name=target_gcs_bucket,
                #     )
                create_schema_and_table(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    destination_table=destination_table,
                    target_gcs_bucket=target_gcs_bucket,
                    output_schema_file=output_schema_file,
                    file_path=first_file_path,
                    schema_filepath_gcs_path=schema_filepath,
                )
                for file_path in sorted(glob.glob(f'{folder_to_process}/{prefix}*.csv')):
                    filename = os.path.basename(file_path)
                    target_gcs_path=f"{root_pipeline_gs_folder}/{fldr_ident}/{filename}"
                    upload_file_to_gcs(
                        file_path=file_path,
                        target_gcs_bucket=target_gcs_bucket,
                        target_gcs_path=f"data/us_climate_normals/{target_gcs_path}"
                    )
                    load_data_gcs_to_bq(
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_id=destination_table,
                        source_bucket=target_gcs_bucket,
                        output_schema_file=output_schema_file,
                        schema_filepath=schema_filepath,
                        gcs_file_path=f"data/us_climate_normals/{target_gcs_path}",
                        local_file_path=file_path,
                        field_delimiter=",",
                        truncate_load=(file_path == first_file_path)
                    )

                    # load_data_to_bq(
                    #     project_id=project_id,
                    #     dataset_id=dataset_id,
                    #     table_id=destination_table,
                    #     file_path=filename,
                    #     field_delimiter=",",
                    #     truncate_load=(filename == first_file_path)
                    # )
                # logging.info(schema_content)
                # logging.info(f"{pipeline_name} process completed")


def create_schema_and_table(
    project_id: str,
    dataset_id: str,
    destination_table: str,
    target_gcs_bucket: str,
    output_schema_file: str,
    file_path: str,
    schema_filepath_gcs_path: str,
) -> None:
    logging.info(f"creating schema and table ... destination_table={destination_table} target_gcs_bucket={target_gcs_bucket} output_schema_file={output_schema_file} file_path={file_path} schema_filepath_gcs_path={schema_filepath_gcs_path} ")
    if not table_exists(project_id, dataset_id, destination_table):
        # filename = return_first_file_for_prefix(prefix, folder_to_process)
        if not gcs_file_exists(bucket=target_gcs_bucket, file_path=output_schema_file):
            generate_schema_file_from_source_file(
                # filename=first_file_path,
                filename=file_path,
                output_schema_file=output_schema_file,
                copy_schema_file_to_gcs=True,
                schema_filepath_bucket=target_gcs_bucket,
                # schema_filepath_gcs_path=schema_filepath
                schema_filepath_gcs_path=schema_filepath_gcs_path
            )
        create_dest_table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=destination_table,
            schema_filepath=schema_filepath_gcs_path,
            bucket_name=target_gcs_bucket,
        )


def gcs_file_exists(bucket: str, file_path: str) -> bool:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    return storage.Blob(bucket=bucket, name=file_path).exists(storage_client)


def prefix_files_exist(
    prefix: str,
    folder_to_process: str
) -> bool:
    file_list = glob.glob(f'{folder_to_process}/{prefix}*.csv')
    return len(file_list) > 0


def return_first_file_for_prefix(
    prefix: str,
    folder_to_process: str
) -> str:
    file_list = glob.glob(f'{folder_to_process}/{prefix}*.csv')
    return sorted(file_list)[0]


def extract_header_from_file(filename: str, sep: str = ",") -> typing.List[str]:
    with open(filename) as f:
        first_line = f.readline()
    return first_line.replace("-", "_").split(sep)


def generate_schema_file_from_source_file(
        filename: str,
        output_schema_file: str,
        copy_schema_file_to_gcs: bool,
        schema_filepath_bucket: str = "",
        schema_filepath_gcs_path: str = "",
        input_sep: str = ","
) -> None:
    # import pdb; pdb.set_trace()
    schema_content = "[\n"
    for fld in extract_header_from_file(filename, input_sep):
        data_type = ""
        fld = fld.replace('"', '').strip()
        if fld in ( 'LATITUDE', 'LONGITUDE', 'ELEVATION' ):
            data_type = "FLOAT"
        elif fld[len(fld)-11:] == '_ATTRIBUTES' or fld in ('STATION', 'DATE', 'NAME'):
            data_type = "STRING"
        elif data_type == "":
            data_type = "FLOAT"
        schema_content += f'  {{\n    "name": "{fld}",\n    "type": "{data_type}",\n    "mode": "NULLABLE",\n    "description": ""\n  }},\n'
    schema_content = schema_content[:-3]
    schema_content += "  }\n]\n"
    pathlib.Path(os.path.dirname(output_schema_file)).mkdir(
        parents=True, exist_ok=True
    )
    with open(output_schema_file, "w+") as schema_file:
        schema_file.write(schema_content)
    if copy_schema_file_to_gcs:
        upload_file_to_gcs(
            file_path=output_schema_file,
            target_gcs_bucket=schema_filepath_bucket,
            target_gcs_path=schema_filepath_gcs_path
        )


def table_exists(project_id: str, dataset_id: str, table_name: str) -> bool:
    client = bigquery.Client(project=project_id)
    tables = client.list_tables(dataset_id)
    found_table = False
    for tbl in tables:
        if tbl.table_id == table_name:
            found_table = True
    return found_table


def load_data_gcs_to_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    source_bucket: str,
    schema_filepath: str,
    gcs_file_path: str,
    local_file_path: str,
    output_schema_file: str,
    field_delimiter: str,
    truncate_load: bool = False
) -> None:
    if truncate_load:
        logging.info(f"Loading data from {gcs_file_path} into {project_id}.{dataset_id}.{table_id} (with truncate table) started")
    else:
        logging.info(f"Loading data from {gcs_file_path} into {project_id}.{dataset_id}.{table_id} (with append data) started")
    if truncate_load:
        write_disposition = 'WRITE_TRUNCATE'
    else:
        write_disposition = 'WRITE_APPEND'
    default_args = {
        'owner': 'default_user',
        'start_date': datetime.datetime.now() - datetime.timedelta(days = 2),
        'depends_on_past': False,
        # With this set to true, the pipeline won't run if the previous day failed
        # 'email': ['demo@email.de'],
        # 'email_on_failure': True,
        # upon failure this pipeline will send an email to your email set above
        # 'email_on_retry': False,
        'retries': 5,
        'retry_delay': datetime.timedelta(minutes=1),
    }
    try:
        with DAG('tempDAG', default_args=default_args) as dag:
            if not truncate_load:
                load_data = gcs2bq.GCSToBigQueryOperator(
                        dag=dag,
                        task_id='load_source_data',
                        bucket=source_bucket,
                        source_objects=[ gcs_file_path ],
                        field_delimiter=field_delimiter,
                        destination_project_dataset_table=f'us_climate_normals.{table_id}',
                        skip_leading_rows=1,
                        schema_object=schema_filepath,
                        write_disposition=f'{write_disposition}',
                        schema_update_options=[ "ALLOW_FIELD_ADDITION" ]
                )
            else:
                load_data = gcs2bq.GCSToBigQueryOperator(
                        dag=dag,
                        task_id='load_source_data',
                        bucket=source_bucket,
                        source_objects=[ gcs_file_path ],
                        field_delimiter=field_delimiter,
                        destination_project_dataset_table=f'us_climate_normals.{table_id}',
                        skip_leading_rows=1,
                        schema_object=schema_filepath,
                        write_disposition=f'{write_disposition}'
                )
            # dummy_task = DummyOperator(task_id='dummy_task', retries=3)
            # load_data.set_downstream("load_source_data")
            # dag.Execute()
            load_data.execute("load_source_data")
    except google_api_exceptions.BadRequest:
            print("*** SCHEMA DIFFERENT FROM TABLE - CREATING ADDITIONAL TABLE ***")
            # ext = os.path.basename(file_path).replace(".csv", "")
            ext = "alternative_1"
            destination_table = f"{table_id}_{ext}"
            schema_filepath = schema_filepath.replace("_schema.json", f"_{ext}_schema.json")
            output_schema_file = output_schema_file.replace("_schema.json", f"_{ext}_schema.json")
            # import pdb; pdb.set_trace()
            if not table_exists(project_id, dataset_id, destination_table):
                create_schema_and_table(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    destination_table=destination_table,
                    target_gcs_bucket=source_bucket,
                    output_schema_file=output_schema_file,
                    file_path=local_file_path,
                    schema_filepath_gcs_path=schema_filepath
                )
            if not truncate_load:
                load_data = gcs2bq.GCSToBigQueryOperator(
                        dag=dag,
                        task_id='load_source_data_alt_1',
                        bucket=source_bucket,
                        source_objects=[ gcs_file_path ],
                        field_delimiter=field_delimiter,
                        destination_project_dataset_table=f'us_climate_normals.{destination_table}',
                        skip_leading_rows=1,
                        schema_object=schema_filepath,
                        write_disposition=f'{write_disposition}',
                        schema_update_options=[ "ALLOW_FIELD_ADDITION" ]
                )
            else:
                load_data = gcs2bq.GCSToBigQueryOperator(
                        dag=dag,
                        task_id='load_source_data_alt_1',
                        bucket=source_bucket,
                        source_objects=[ gcs_file_path ],
                        field_delimiter=field_delimiter,
                        destination_project_dataset_table=f'us_climate_normals.{table_id}',
                        skip_leading_rows=1,
                        schema_object=schema_filepath,
                        write_disposition=f'{write_disposition}'
                )
            load_data.execute("load_source_data_alt_1")


    logging.info(
        f"Loading data from {gcs_file_path} into {project_id}.{dataset_id}.{table_id} completed"
    )

def load_data_to_bq(
    project_id: str,
    dataset_id: str,
    table_id: str,
    file_path: str,
    field_delimiter: str,
    truncate_load: bool = False
) -> None:
    if truncate_load:
        logging.info(f"Loading data from {file_path} into {project_id}.{dataset_id}.{table_id} (with truncate table) started")
    else:
        logging.info(f"Loading data from {file_path} into {project_id}.{dataset_id}.{table_id} (with append data) started")
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = field_delimiter
    job_config.skip_leading_rows = 1  # ignore the header
    job_config.autodetect = False
    if truncate_load:
        job_config.write_disposition = 'WRITE_TRUNCATE'
    else:
        job_config.write_disposition = 'WRITE_APPEND'
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
) -> None:
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    logging.info(f"Attempting to create table {table_ref} if it doesn't already exist")
    client = bigquery.Client()
    try:
        table_exists_id = client.get_table(table_ref).table_id
        logging.info(f"Table {table_exists_id} currently exists.")
    except NotFound:
        logging.info(
            (
                f"Table {table_ref} currently does not exist.  Attempting to create table."
            )
        )
        schema = create_table_schema([], bucket_name, schema_filepath)
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Table {table_ref} was created".format(table_id))


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


def upload_file_to_gcs(
    file_path: pathlib.Path, target_gcs_bucket: str, target_gcs_path: str
) -> None:
    if os.path.exists(file_path):
        logging.info(
            f"Uploading output file {file_path} to gs://{target_gcs_bucket}/{target_gcs_path}"
        )
        storage_client = storage.Client()
        bucket = storage_client.bucket(target_gcs_bucket)
        blob = bucket.blob(target_gcs_path)
        blob.upload_from_filename(file_path)
    else:
        logging.info(
            f"Cannot upload file {file_path} to gs://{target_gcs_bucket}/{target_gcs_path} as it does not exist."
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    main(
        project_id=os.environ.get("PROJECT_ID", ""),
        dataset_id=os.environ.get("DATASET_ID", ""),
        # current_data_table_id=os.environ.get("CURRENT_DATA_TABLE_ID", ""),
        # historical_data_table_id=os.environ.get("HISTORICAL_DATA_TABLE_ID", ""),
        # data_file_surr_key_field=os.environ.get("DATA_FILE_SURR_KEY_FIELD", ""),
        # dest_folder=os.environ.get("DEST_FOLDER", ""),
        # schema_filepath=os.environ.get("SCHEMA_FILEPATH", ""),
        # source_bucket=os.environ.get("SOURCE_BUCKET", ""),
        target_gcs_bucket=os.environ.get("TARGET_GCS_BUCKET", ""),
        # dest_current_data_folder_name=os.environ.get(
        #     "DEST_CURRENT_DATA_FOLDER_NAME", ""
        # ),
        # dest_historical_data_folder_name=os.environ.get(
        #     "DEST_HISTORICAL_FOLDER_NAME", ""
        # ),
        # source_bucket_current_data_folder_name=os.environ.get(
        #     "SOURCE_BUCKET_CURRENT_DATA_FOLDER_NAME", ""
        # ),
        # current_data_target_gcs_path=os.environ.get("CURRENT_DATA_TARGET_GCS_PATH", ""),
        # historical_data_target_gcs_path=os.environ.get(
        #     "HISTORICAL_DATA_TARGET_GCS_PATH", ""
        # ),
        # data_root_folder=os.environ.get("DATA_ROOT_FOLDER", ""),
        # hist_folders_list=json.loads(os.environ.get("HIST_FOLDERS_LIST", r"[]")),
        # pipeline_name=os.environ.get("PIPELINE_NAME", ""),
        # chunksize=os.environ.get("CHUNKSIZE", ""),
        # input_headers=json.loads(os.environ.get("INPUT_CSV_HEADERS", r"[]")),
        # data_dtypes=json.loads(os.environ.get("DATA_DTYPES", r"{}")),
        # int_col_list=json.loads(os.environ.get("INT_COL_LIST", r"[]")),
    )
