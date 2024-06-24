# Copyright 2021 Google LLC
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

import datetime
import json
import logging
import os
from concurrent import futures

import pandas as pd
import requests
from google.api_core import exceptions
from google.cloud import bigquery, storage


def main(
  project_id: str,
  dataset_id: str,
  table_id: str,
  staging_dataset_id: str,
  staging_table_id: str,
  schema_filepath: str,
  source_page_url: str,
  start_year: str,
  file_name_prefix: str,
  stage_source_folder: str,
  target_gcs_bucket: str,
  target_gcs_path: str,
  rename_mappings: dict,
  non_na_columns: list,
  data_types: dict,
  date_cols: list,
) -> None:
  logging.info(
      f'Chicago Taxi Trips Dataset pipeline process started at {str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}'
  )
  # Collect the names and URLs of the files
  bq_client = bigquery.Client(project=project_id)
  df_bq_data = extract_data_count_by_year(
    bq_client=bq_client,
    dataset_id=dataset_id,
    table_id=table_id
  )
  df_source_files = list_source_files(
    source_page_url=source_page_url,
    file_name_prefix=f"{file_name_prefix}{start_year[0:2]}"
  )
  current_year = datetime.datetime.now().year
  for load_lp_yr in range((int(start_year) if start_year else 2013), current_year + 1):
    source_name = f"{file_name_prefix}{load_lp_yr}"
    source_id = str(df_source_files[ df_source_files["name"] == source_name ]["id"]).split()[1]
    source_url = f"{source_page_url}/{source_id}/rows.csv?accessType=DOWNLOAD&bom=true"
    source_file_name = str.lower(source_name).replace(" ", "_")
    source_file_path = f"{stage_source_folder}/{source_file_name}.csv"
    output_file_path = source_file_path.replace(".csv", "_out.csv")
    if not((df_bq_data[ df_bq_data['data_year'] == load_lp_yr ]).empty):
      data_row_count = int(df_bq_data[ df_bq_data['data_year'] == load_lp_yr ]['cnt'])
    else:
      data_row_count = 0
    if load_lp_yr in df_bq_data["data_year"].values and load_lp_yr < (current_year - 2):
      logging.info(f"... Data for year {load_lp_yr} is already loaded with {data_row_count:,d} rows.  Skipping...")
    else:
      stage_and_load_data_file(
        bq_client=bq_client,
        load_lp_yr=load_lp_yr,
        data_row_count=data_row_count,
        output_file_path=output_file_path,
        target_gcs_bucket=target_gcs_bucket,
        target_gcs_path=target_gcs_path,
        staging_dataset_id=staging_dataset_id,
        staging_table_id=staging_table_id,
        dataset_id=dataset_id,
        table_id=table_id,
        schema_filepath=schema_filepath,
        source_url=source_url,
        source_file_path=source_file_path,
        data_types=data_types,
        date_cols=date_cols,
        rename_mappings=rename_mappings,
        non_na_columns=non_na_columns
      )
  logging.info(
      f'Chicago Taxi Trips Dataset pipeline process completed at {str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}'
  )


def stage_and_load_data_file(
  bq_client: bigquery.Client,
  load_lp_yr: int,
  data_row_count: int,
  output_file_path: str,
  target_gcs_bucket: str,
  target_gcs_path: str,
  staging_dataset_id: str,
  staging_table_id: str,
  dataset_id: str,
  table_id: str,
  schema_filepath: str,
  source_url: str,
  source_file_path: str,
  rename_mappings: dict,
  non_na_columns: list,
  data_types: dict,
  date_cols: list,
) -> bool:
  logging.info(f"... Downloading the data for year {load_lp_yr}.")
  download_file(source_url, source_file_path)
  logging.info(f"... Executing transforms for year {load_lp_yr}.")
  chunk_process(
      target_file=source_file_path,
      data_types=data_types,
      date_cols=date_cols,
      rename_mappings=rename_mappings,
      non_na_columns=non_na_columns,
      output_file=output_file_path,
  )
  logging.info(f"... Staging load data for year {load_lp_yr}.")
  # If the target file exists
  if os.path.exists(output_file_path):
    # Upload the transformed file to GCS
    upload_file_to_gcs(
        file_path=output_file_path,
        target_gcs_bucket=target_gcs_bucket,
        target_gcs_path=f"{target_gcs_path}/{os.path.basename(output_file_path)}"
    )
    # Create the staging table if it does not exist
    table_exists = create_dest_table(
        project_id=bq_client.project,
        dataset_id=staging_dataset_id,
        table_id=staging_table_id,
        schema_filepath=schema_filepath,
        bucket_name=target_gcs_bucket,
        drop_table=False
    )
    if table_exists:
      # Load the data into the staging table
      load_data_to_bq(
          bq_client=bq_client,
          file_path=output_file_path,
          dataset_id=staging_dataset_id,
          table_id=staging_table_id,
          truncate_table=True,
          field_delimiter="|",
          skip_leading_rows=1,
          ignore_unknown_values=True,
          enable_logging=True
      )
      # if we have a different number of rows between main and stage then load the deltas into main from stage.
      logging.info(f"... Appending load data deltas for year {load_lp_yr} to main table.")
      stage_row_count = count_stage_rows(
          bq_client=bq_client,
          staging_dataset_id=staging_dataset_id,
          staging_table_id=staging_table_id
      )
      count_deltas = stage_row_count - data_row_count
      if count_deltas == 0:
        logging.info(f"... Number of rows in data file and destination table are the same ({data_row_count}). Skipping load process.")
      else:
        # add the delta rows from the staging table to the main table
        logging.info("... Deltas exist in the new data file.  Adding rows to main table.")
        add_delta_rows(
          bq_client=bq_client,
          staging_dataset_id=staging_dataset_id,
          staging_table_id=staging_table_id,
          main_dataset_id=dataset_id,
          main_table_id=table_id
        )
    else:
      error_msg = f"Error: Data was not loaded because the destination table {bq_client.project}.{staging_dataset_id}.{staging_table_id} does not exist and/or could not be created."
      return str(ValueError(error_msg))
  else:
    logging.info(
        f"Informational: The data file {output_file_path} was not generated because no data file was available.  Continuing."
    )
    return False
  return True


def extract_data_count_by_year(
    bq_client: bigquery.Client,
    dataset_id: str,
    table_id: str
) -> pd.DataFrame:
  project_id = bq_client.project
  query = f"""
      SELECT extract(year from trip_start_timestamp) as data_year,
              count(*) as cnt
        FROM `{project_id}.{dataset_id}.{table_id}`
        GROUP BY 1
        ORDER BY 1
  """
  # than retrieve the list
  query_job = bq_client.query(query)
  query_job.result()
  return query_job.result().to_dataframe()


def count_stage_rows(
    bq_client: bigquery.Client,
    staging_dataset_id: str,
    staging_table_id: str
) -> int:
  project_id = bq_client.project
  query = f"""
      SELECT count(*) as cnt
        FROM `{project_id}.{staging_dataset_id}.{staging_table_id}`
  """
  query_job = bq_client.query(query)
  query_job.result()
  return int(query_job.result().to_dataframe()["cnt"].loc[0])


def add_delta_rows(
    bq_client: bigquery.Client,
    staging_dataset_id: str,
    staging_table_id: str,
    main_dataset_id: str,
    main_table_id: str
) -> bool:
  project_id = bq_client.project
  query = f"""
    INSERT INTO `{project_id}.{main_dataset_id}.{main_table_id}`
    (
      unique_key,
      taxi_id,
      trip_start_timestamp,
      trip_end_timestamp,
      trip_seconds,
      trip_miles,
      pickup_census_tract,
      dropoff_census_tract,
      pickup_community_area,
      dropoff_community_area,
      fare,
      tips,
      tolls,
      extras,
      trip_total,
      payment_type,
      company,
      pickup_latitude,
      pickup_longitude,
      pickup_location,
      dropoff_latitude,
      dropoff_longitude,
      dropoff_location
    )
      SELECT  stg.unique_key,
              stg.taxi_id,
              stg.trip_start_timestamp,
              stg.trip_end_timestamp,
              stg.trip_seconds,
              stg.trip_miles,
              stg.pickup_census_tract,
              stg.dropoff_census_tract,
              stg.pickup_community_area,
              stg.dropoff_community_area,
              stg.fare,
              stg.tips,
              stg.tolls,
              stg.extras,
              stg.trip_total,
              stg.payment_type,
              stg.company,
              stg.pickup_latitude,
              stg.pickup_longitude,
              stg.pickup_location,
              stg.dropoff_latitude,
              stg.dropoff_longitude,
              stg.dropoff_location
        FROM `{project_id}.{staging_dataset_id}.{staging_table_id}` as stg
             LEFT OUTER JOIN `{project_id}.{main_dataset_id}.{main_table_id}` as main
          ON main.unique_key = stg.unique_key
      WHERE main.unique_key is null
  """
  query_job = bq_client.query(query)
  query_job.result()
  return True


def create_dest_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    schema_filepath: list,
    bucket_name: str,
    drop_table: bool = False,
    table_description="",
) -> bool:
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    logging.info(f"Attempting to create table {table_ref} if it doesn't already exist")
    client = bigquery.Client()
    table_exists = False
    try:
        table = client.get_table(table_ref)
        table_exists_id = table.table_id
        logging.info(f"Table {table_exists_id} currently exists.")
        if drop_table:
            logging.info("Dropping existing table")
            client.delete_table(table)
            table = None
    except exceptions.NotFound:
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
            table.description = table_description
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


def list_source_files(source_page_url: str, file_name_prefix: str) -> pd.DataFrame:
    df = pd.read_json(source_page_url)
    return df[ df['name'].str.contains(file_name_prefix) ].sort_values('name', ascending=True, ignore_index=True)[['id', 'name']]


def download_file(source_url: str, source_file: str) -> None:
    logging.info(f"Downloading data from {source_url} to {source_file} .")
    res = requests.get(source_url, stream=True)
    if res.status_code == 200:
        with open(source_file, "wb") as fb:
            for idx, chunk in enumerate(res):
                fb.write(chunk)
                if not idx % 10000000:
                    file_size = os.stat(source_file).st_size / (1024**3)
                    logging.info(
                        f"\t{idx} chunks of data downloaded & current file size is {file_size} GB"
                    )
    else:
        logging.info(f"Couldn't download {source_url}: {res.text}")
    logging.info(
        f"Successfully downloaded data from {source_url} into {source_file} - {os.stat(source_file).st_size / (1024 ** 3)} GB"
    )


def download_blob(gcs_bucket: str, source_gcs_object: str, target_file: str) -> None:
    """Downloads a blob from the bucket."""
    logging.info(
        f"Downloading data from gs://{gcs_bucket}/{source_gcs_object} to {target_file} ..."
    )
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(source_gcs_object)
    blob.download_to_filename(target_file, raw_download=True)
    logging.info("Downloading Completed.")


def chunk_process(
    target_file: str,
    data_types: dict,
    date_cols: list,
    rename_mappings: dict,
    non_na_columns: list,
    output_file: str,
) -> None:
    logging.info(f"Process started for {output_file} file")
    logging.info(f"Reading file {target_file} to pandas dataframe...")
    chunks = pd.read_csv(
        target_file,
        chunksize=500000,
        dtype=data_types,
        parse_dates=date_cols,
        dayfirst=False,
    )
    logging.info(f"Removing {target_file} file")
    os.remove(target_file)
    for idx, chunk in enumerate(chunks):
        # logging.info("\tRenaming headers")
        rename_headers(chunk, rename_mappings)
        # logging.info(
        #     f"\tDropping null rows from specified columns in list = {non_na_columns}"
        # )
        drop_null_rows(chunk, non_na_columns)
        if not idx:
            logging.info(f"\tWriting data to output file = {output_file}")
            chunk.to_csv(output_file, mode="w", index=False, header=True, sep="|")
        else:
            # logging.info(f"\tAppending data to output file = {output_file}")
            chunk.to_csv(output_file, mode="a", index=False, header=False, sep="|")
    logging.info(f"Process completed for {output_file} file")


def rename_headers(df: pd.DataFrame, rename_mappings: dict) -> None:
    df.rename(columns=rename_mappings, inplace=True)


def drop_null_rows(df: pd.DataFrame, null_columns: list) -> None:
    df.dropna(subset=null_columns, inplace=True)


def upload_file_to_gcs(
    file_path: str, target_gcs_bucket: str, target_gcs_path: str
) -> None:
    logging.info(f"Uploading output file to gs://{target_gcs_bucket}/{target_gcs_path}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(target_gcs_bucket)
    blob = bucket.blob(target_gcs_path)
    blob.upload_from_filename(file_path)
    logging.info("Successfully uploaded file to gcs bucket.")


def load_data_to_bq(
    bq_client: bigquery.Client,
    file_path: str,
    dataset_id: str,
    table_id: str,
    table_id_placeholder: str = "",
    table_id_placeholder_value: str = "",
    truncate_table: bool = True,
    allow_quoted_newlines: bool = True,
    create_disposition: str = "CREATE_IF_NEEDED",
    field_delimiter: str = "|",
    quote_character: str = '"',
    skip_leading_rows: int = 0,
    write_disposition: str = "WRITE_TRUNCATE",
    ignore_unknown_values: bool = False,
    enable_logging: bool = False,
) -> bool:
  """Load data to a bigquery table from a flat file.

  Args:
    bq_client: Reference to the Google cloud BQ client to use. # storage_client:
      GCS client to use to access GCP buckets.
    file_path: Full path to the local file to load data from.
    dataset_id: Name of the dataset containing the table to load data into.
    table_id: Name of the table to load data into.
    table_id_placeholder: String representing the value to replace in table_id
      variable with the placeholder value such as an actual year.  For example,
      "~YEAR~"
    table_id_placeholder_value: String value to replace the placeholder with in
      the table id.  For example, the actual year to replace the table_id's
      placeholder with.
    truncate_table: True if truncate-load, False if append data.
    schema_filepath: Full path in the GCS bucket to the JSON table schema file.
    allow_quoted_newlines: see "Additional Info/settings/allow_quoted_newlines".
    create_disposition: see "Additional Info/settings/create_disposition".
    field_delimiter: see "Additional Info/settings/field_delimiter".
    quote_character: see "Additional Info/settings/quote_character".
    skip_leading_rows: see "Additional Info/settings/skip_leading_rows".
    write_disposition: see "Additional Info/settings/write_disposition".
    ignore_unknown_values: see "Additional Info/settings/ignore_unknown_values".
    enable_logging: If True, logs accordingly.

  Returns:
    True if successful load, otherwise unsuccessful.
  """
  try:
    if not os.path.isfile(file_path):
      logging.info("Error: file %s doesn't exist.", file_path)
      return False
    if table_id_placeholder:
      table_id = table_id.replace(
          table_id_placeholder, table_id_placeholder_value
      )
    if enable_logging:
      logging.info(
          "Loading data from %s into %s.%s.%s started.",
          file_path,
          bq_client.project,
          dataset_id,
          table_id,
      )
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = field_delimiter
    if truncate_table:
      job_config.write_disposition = "WRITE_TRUNCATE"
    else:
      job_config.write_disposition = write_disposition
    job_config.skip_leading_rows = skip_leading_rows
    job_config.autodetect = False
    job_config.allow_quoted_newlines = True
    job_config.quote_character = quote_character
    job_config.create_disposition = create_disposition
    job_config.quote_character = quote_character
    job_config.allow_quoted_newlines = allow_quoted_newlines
    job_config.skip_leading_rows = skip_leading_rows
    job_config.ignore_unknown_values = ignore_unknown_values
    job_config.autodetect = False
    job = load_table_from_file(
        bq_client=bq_client,
        table_ref=table_ref,
        job_config=job_config,
        file_path=file_path,
        enable_logging=enable_logging,
    )
    if enable_logging:
      logging.info(
          "Loading data from %s into %s.%s.%s completed.",
          file_path,
          bq_client.project,
          dataset_id,
          table_id,
      )
    return job.state == "DONE"
  except (FileNotFoundError, PermissionError, OSError) as ex:
    logging.info(" ... Unable to access source data file !!!")
    logging.info(ex.strerror)
    return False


def get_schema_from_file(
    schema_filepath: str,
) -> ...:
  """Gets the schema from the file identified in file_path.

  Args:
    schema_filepath: Path to the local file to load data from.

  Returns:
    Schema object in the form of a list of dicts.
  """
  with open(schema_filepath, "r") as schema_file:
    schema_data = json.load(schema_file)
  return schema_data


def load_table_from_file(
    bq_client: bigquery.Client,
    table_ref: bigquery.TableReference,
    job_config: bigquery.LoadJobConfig,
    file_path: str = "",
    enable_logging: bool = False,
) -> bigquery.LoadJob:
  """Loads data into the specified BQ table from the file identified in file_path.

  Args:
    bq_client: The bigquery client used to communicate with BigQuery.
    table_ref: A reference to the table object to load.
    job_config: A reference to the bigquery load job object.
    file_path: Path to the local file to load to BQ.
    enable_logging: If True, logs accordingly.

  Returns:
    Either:
      "Error: A Google API error occurred"
        Indicates that a google API error occurred
      "Error: A timeout error occurred"
        Indicates that a timeout occurred when connecting to BQ
      "DONE":
        Success
  """
  with open(file_path, "rb") as source_file:
    job = bq_client.load_table_from_file(
        source_file, destination=table_ref, job_config=job_config
    )
    try:
      success = job.result()
      succeeded = job.state
      if succeeded == "DONE":
        logging.info(
            "Loading data from %s into %s:%s.%s completed successfully",
            file_path,
            bq_client.project,
            table_ref.dataset_id,
            table_ref.table_id,
        )
        return success
      else:
        logging.info(
            "Loading data from %s into %s:%s.%s did not complete successfully",
            file_path,
            bq_client.project,
            table_ref.dataset_id,
            table_ref.table_id,
        )
        return "ERROR: One or more load errors occurred"
    except exceptions.BadRequest:
      if enable_logging:
        for err in job.errors:
          logging.info("ERROR: %s", format(err["message"]))
      return job
    except exceptions.GoogleAPICallError as ex:
      logging.info(
          "Loading data from %s into %s:%s.%s raised a google API call"
          " error %s",
          file_path,
          bq_client.project,
          table_ref.dataset_id,
          table_ref.table_id,
          ex.message,
      )
      return job
    except futures.TimeoutError:
      logging.info(
          "Loading data from %s into %s:%s.%s raised a timeout error",
          file_path,
          bq_client.project,
          table_ref.dataset_id,
          table_ref.table_id,
      )
      return job


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main(
        project_id=os.environ.get("PROJECT_ID", ""),
        dataset_id=os.environ.get("DATASET_ID", ""),
        table_id=os.environ.get("TABLE_ID", ""),
        staging_dataset_id=os.environ.get("STAGING_DATASET_ID", ""),
        staging_table_id=os.environ.get("STAGING_TABLE_ID", ""),
        schema_filepath=os.environ.get("SCHEMA_FILEPATH", ""),
        source_page_url=os.environ.get("SOURCE_PAGE_URL", ""),
        start_year=os.environ.get("START_YEAR", ""),
        file_name_prefix=os.environ.get("FILE_NAME_PREFIX", ""),
        stage_source_folder=os.environ.get("STAGE_SOURCE_FOLDER", ""),
        target_gcs_bucket=os.environ.get("TARGET_GCS_BUCKET", ""),
        target_gcs_path=os.environ.get("TARGET_GCS_PATH", ""),
        rename_mappings=json.loads(os.environ.get("RENAME_MAPPINGS", "{}")),
        non_na_columns=json.loads(os.environ.get("NON_NA_COLUMNS", "[]")),
        data_types=json.loads(os.environ.get("DATA_TYPES", "{}")),
        date_cols=json.loads(os.environ.get("DATE_COLS", "[]")),
    )
