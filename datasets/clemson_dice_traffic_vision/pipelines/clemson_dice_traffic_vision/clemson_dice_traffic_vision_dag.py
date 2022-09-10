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


from airflow import DAG
from airflow.operators import bash
from airflow.providers.google.cloud.transfers import gcs_to_bigquery

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-03-01",
}


with DAG(
    dag_id="clemson_dice_traffic_vision.traffic_vision",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@once",
    catchup=False,
    default_view="graph",
) as dag:

    # Task to copy over to pod, the source data and structure from GCS
    transform_files = bash.BashOperator(
        task_id="transform_files",
        bash_command='mkdir -p $WORKING_DIR/load_files ;\nmkdir -p $WORKING_DIR/unpack ;\nfor f in $WORKING_DIR/files/*.tar.gz ;\n  do echo Decompressing $f ; \\\n     tar -xvzf "$f" -C "$WORKING_DIR/unpack" ; \\\n     ext="$(basename ${f/.tar.gz/})" ; \\\n     sedval=\u0027s/{\\"frame\\"/{"id": \\"\u0027$ext\u0027\\"\\, "frame"/\u0027 ; \\\n     echo Fixing $WORKING_DIR/unpack/$ext/out.log ; \\\n     sed -i "$sedval" $WORKING_DIR/unpack/$ext/out.log ; \\\n     cp $WORKING_DIR/unpack/$ext/out.log $WORKING_DIR/load_files/out"$ext".log ;\ndone\n',
        env={"WORKING_DIR": "/home/airflow/gcs/data/trafficvision"},
    )

    # Load JSON metadata files to BQ
    load_json_metadata_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_json_metadata_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/trafficvision/load_files/out*.log"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table="clemson_dice.traffic_vision",
        write_disposition="WRITE_TRUNCATE",
    )

    transform_files >> load_json_metadata_to_bq