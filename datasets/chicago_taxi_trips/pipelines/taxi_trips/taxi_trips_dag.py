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
from airflow.providers.google.cloud.operators import kubernetes_engine

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-03-01",
}


with DAG(
    dag_id="chicago_taxi_trips.taxi_trips",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@weekly",
    catchup=False,
    default_view="graph",
) as dag:
    create_cluster = kubernetes_engine.GKECreateClusterOperator(
        task_id="create_cluster",
        project_id="{{ var.value.gcp_project }}",
        location="us-central1-c",
        body={
            "name": "chicago-taxi-trips",
            "initial_node_count": 1,
            "network": "{{ var.value.vpc_network }}",
            "node_config": {
                "machine_type": "e2-standard-16",
                "oauth_scopes": [
                    "https://www.googleapis.com/auth/devstorage.read_write",
                    "https://www.googleapis.com/auth/cloud-platform",
                ],
            },
        },
    )

    # Download Taxi Trips dataset
    load_data = kubernetes_engine.GKEStartPodOperator(
        task_id="load_data",
        name="taxi_trips",
        project_id="{{ var.value.gcp_project }}",
        location="us-central1-c",
        cluster_name="chicago-taxi-trips",
        namespace="default",
        image="{{ var.json.chicago_taxi_trips.container_registry.run_csv_transform_kub }}",
        image_pull_policy="Always",
        env_vars={
            "PROJECT_ID": "{{ var.value.gcp_project }}",
            "DATASET_ID": "chicago_taxi_trips",
            "TABLE_ID": "taxi_trips",
            "STAGING_DATASET_ID": "chicago_stage",
            "STAGING_TABLE_ID": "taxi_trips_stage",
            "SCHEMA_FILEPATH": "data/chicago_taxi_trips/schema/chicago_taxi_trips_schema.json",
            "SOURCE_PAGE_URL": "https://data.cityofchicago.org/api/views",
            "START_YEAR": "2013",
            "FILE_NAME_PREFIX": "Taxi Trips - ",
            "STAGE_SOURCE_FOLDER": "",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "data/chicago_taxi_trips",
            "RENAME_MAPPINGS": '{\n  "Trip ID": "unique_key",\n  "Taxi ID": "taxi_id",\n  "Trip Start Timestamp": "trip_start_timestamp",\n  "Trip End Timestamp": "trip_end_timestamp",\n  "Trip Seconds": "trip_seconds",\n  "Trip Miles": "trip_miles",\n  "Pickup Census Tract": "pickup_census_tract",\n  "Dropoff Census Tract": "dropoff_census_tract",\n  "Pickup Community Area": "pickup_community_area",\n  "Dropoff Community Area": "dropoff_community_area",\n  "Fare": "fare",\n  "Tips": "tips",\n  "Tolls": "tolls",\n  "Extras": "extras",\n  "Trip Total": "trip_total",\n  "Payment Type": "payment_type",\n  "Company": "company",\n  "Pickup Centroid Latitude": "pickup_latitude",\n  "Pickup Centroid Longitude": "pickup_longitude",\n  "Pickup Centroid Location": "pickup_location",\n  "Dropoff Centroid Latitude": "dropoff_latitude",\n  "Dropoff Centroid Longitude": "dropoff_longitude",\n  "Dropoff Centroid  Location": "dropoff_location"\n}',
            "NON_NA_COLUMNS": '["unique_key","taxi_id"]',
            "OUTPUT_OBJECT_FOLDER": "data/chicago_taxi_trips/output",
            "DATA_TYPES": '{\n  "Trip Seconds": "Int64",\n  "Pickup Census Tract": "Int64",\n  "Dropoff Census Tract": "Int64",\n  "Pickup Community Area": "Int64",\n  "Dropoff Community Area": "Int64"\n}',
            "DATE_COLS": '["Trip Start Timestamp", "Trip End Timestamp"]',
        },
        retries=3,
        retry_delay=300,
        retry_exponential_backoff=True,
        startup_timeout_seconds=600,
    )
    delete_cluster = kubernetes_engine.GKEDeleteClusterOperator(
        task_id="delete_cluster",
        project_id="{{ var.value.gcp_project }}",
        location="us-central1-c",
        name="chicago-taxi-trips",
    )

    create_cluster >> load_data >> delete_cluster
