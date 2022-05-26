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


from airflow import DAG
from airflow.providers.cncf.kubernetes.operators import kubernetes_pod
from airflow.providers.google.cloud.operators import kubernetes_engine

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-03-01",
}


with DAG(
    dag_id="cloud_storage_geo_index.cloud_storage_geo_index",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
) as dag:
    create_cluster = kubernetes_engine.GKECreateClusterOperator(
        task_id="create_cluster",
        project_id="{{ var.value.gcp_project }}",
        location="us-central1-c",
        body={
            "name": "cloud-storage-geo-index",
            "initial_node_count": 2,
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

    # Run CSV transform within kubernetes pod
    landsat_index = kubernetes_pod.KubernetesPodOperator(
        task_id="landsat_index",
        startup_timeout_seconds=600,
        name="landsat_index",
        namespace="composer",
        service_account_name="datasets",
        image_pull_policy="Always",
        image="{{ var.json.cloud_storage_geo_index.container_registry.run_csv_transform_kub }}",
        env_vars={
            "PIPELINE_NAME": "{{ var.json.cloud_storage_geo_index.landsat_index.pipeline_name }}",
            "SOURCE_URL": "{{ var.json.cloud_storage_geo_index.landsat_index.source_url }}",
            "SOURCE_ZIPFILE": "{{ var.json.cloud_storage_geo_index.landsat_index.source_zipfile }}",
            "SOURCE_FILE": "{{ var.json.cloud_storage_geo_index.landsat_index.source_file }}",
            "TARGET_FILE": "{{ var.json.cloud_storage_geo_index.landsat_index.target_file }}",
            "CHUNKSIZE": "{{ var.json.cloud_storage_geo_index.landsat_index.chunksize }}",
            "PROJECT_ID": "{{ var.value.gcp_project }}",
            "DATASET_ID": "{{ var.json.cloud_storage_geo_index.dataset_id }}",
            "TABLE_ID": "{{ var.json.cloud_storage_geo_index.landsat_index.table_id }}",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "{{ var.json.cloud_storage_geo_index.landsat_index.target_gcs_path }}",
            "SCHEMA_PATH": "{{ var.json.cloud_storage_geo_index.landsat_index.schema_path }}",
            "DROP_DEST_TABLE": "{{ var.json.cloud_storage_geo_index.landsat_index.drop_dest_table }}",
            "INPUT_FIELD_DELIMITER": "{{ var.json.cloud_storage_geo_index.landsat_index.input_field_delimiter }}",
            "REMOVE_SOURCE_FILE": "{{ var.json.cloud_storage_geo_index.landsat_index.remove_source_file }}",
            "DELETE_TARGET_FILE": "{{ var.json.cloud_storage_geo_index.landsat_index.delete_target_file }}",
            "INPUT_CSV_HEADERS": '[\n  "SCENE_ID",\n  "PRODUCT_ID",\n  "SPACECRAFT_ID",\n  "SENSOR_ID",\n  "DATE_ACQUIRED",\n  "COLLECTION_NUMBER",\n  "COLLECTION_CATEGORY",\n  "SENSING_TIME",\n  "DATA_TYPE",\n  "WRS_PATH",\n  "WRS_ROW",\n  "CLOUD_COVER",\n  "NORTH_LAT",\n  "SOUTH_LAT",\n  "WEST_LON",\n  "EAST_LON",\n  "TOTAL_SIZE",\n  "BASE_URL"\n]',
            "DATA_DTYPES": '{\n  "SCENE_ID": "str",\n  "PRODUCT_ID": "str",\n  "SPACECRAFT_ID": "str",\n  "SENSOR_ID": "str",\n  "DATE_ACQUIRED": "str",\n  "COLLECTION_NUMBER": "str",\n  "COLLECTION_CATEGORY": "str",\n  "SENSING_TIME": "str",\n  "DATA_TYPE": "str",\n  "WRS_PATH": "str",\n  "WRS_ROW": "str",\n  "CLOUD_COVER": "str",\n  "NORTH_LAT": "str",\n  "SOUTH_LAT": "str",\n  "WEST_LON": "str",\n  "EAST_LON": "str",\n  "TOTAL_SIZE": "str",\n  "BASE_URL": "str"\n}',
            "RENAME_HEADERS_LIST": '{\n  "SCENE_ID": "scene_id",\n  "PRODUCT_ID": "product_id",\n  "SPACECRAFT_ID": "spacecraft_id",\n  "SENSOR_ID": "sensor_id",\n  "DATE_ACQUIRED": "date_acquired",\n  "COLLECTION_NUMBER": "collection_number",\n  "COLLECTION_CATEGORY": "collection_category",\n  "SENSING_TIME": "sensing_time",\n  "DATA_TYPE": "data_type",\n  "WRS_PATH": "wrs_path",\n  "WRS_ROW": "wrs_row",\n  "CLOUD_COVER": "cloud_cover",\n  "NORTH_LAT": "north_lat",\n  "SOUTH_LAT": "south_lat",\n  "WEST_LON": "west_lon",\n  "EAST_LON": "east_lon",\n  "TOTAL_SIZE": "total_size",\n  "BASE_URL": "base_url"\n}',
            "REORDER_HEADERS_LIST": '[\n  "scene_id",\n  "product_id",\n  "spacecraft_id",\n  "sensor_id",\n  "date_acquired",\n  "sensing_time",\n  "collection_number",\n  "collection_category",\n  "data_type",\n  "wrs_path",\n  "wrs_row",\n  "cloud_cover",\n  "north_lat",\n  "south_lat",\n  "west_lon",\n  "east_lon",\n  "total_size",\n  "base_url"\n]',
            "TABLE_DESCRIPTION": "Landsat index table",
            "TABLE_CLUSTERING_FIELD_LIST": '[\n  "spacecraft_id",\n  "sensor_id",\n  "wrs_path",\n  "wrs_row"\n]',
            "TABLE_PARTITION_FIELD": "sensing_time",
            "TABLE_PARTITION_FIELD_TYPE": "MONTH",
        },
        resources={"request_ephemeral_storage": "32G", "request_cpu": "4"},
    )

    # Run CSV transform within kubernetes pod
    sentinel_2 = kubernetes_pod.KubernetesPodOperator(
        task_id="sentinel_2",
        startup_timeout_seconds=600,
        name="sentinel_2",
        namespace="composer",
        service_account_name="datasets",
        image_pull_policy="Always",
        image="{{ var.json.cloud_storage_geo_index.container_registry.run_csv_transform_kub }}",
        env_vars={
            "PIPELINE_NAME": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.pipeline_name }}",
            "SOURCE_URL": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.source_url }}",
            "SOURCE_FILE": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.source_file }}",
            "TARGET_FILE": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.target_file }}",
            "CHUNKSIZE": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.chunksize }}",
            "PROJECT_ID": "{{ var.value.gcp_project }}",
            "DATASET_ID": "{{ var.json.cloud_storage_geo_index.dataset_id }}",
            "TABLE_ID": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.table_id }}",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.target_gcs_path }}",
            "SCHEMA_PATH": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.schema_path }}",
            "DROP_DEST_TABLE": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.drop_dest_table }}",
            "INPUT_FIELD_DELIMITER": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.input_field_delimiter }}",
            "REMOVE_SOURCE_FILE": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.remove_source_file }}",
            "DELETE_TARGET_FILE": "{{ var.json.cloud_storage_geo_index.sentinel_2_index.delete_target_file }}",
            "INPUT_CSV_HEADERS": '[\n  "GRANULE_ID",\n  "PRODUCT_ID",\n  "DATATAKE_IDENTIFIER",\n  "MGRS_TILE",\n  "SENSING_TIME",\n  "TOTAL_SIZE",\n  "CLOUD_COVER",\n  "GEOMETRIC_QUALITY_FLAG",\n  "GENERATION_TIME",\n  "NORTH_LAT",\n  "SOUTH_LAT",\n  "WEST_LON",\n  "EAST_LON",\n  "BASE_URL"\n]',
            "DATA_DTYPES": '{\n  "GRANULE_ID": "str",\n  "PRODUCT_ID": "str",\n  "DATATAKE_IDENTIFIER": "str",\n  "MGRS_TILE": "str",\n  "SENSING_TIME": "str",\n  "TOTAL_SIZE": "str",\n  "CLOUD_COVER": "str",\n  "GEOMETRIC_QUALITY_FLAG": "str",\n  "GENERATION_TIME": "str",\n  "NORTH_LAT": "str",\n  "SOUTH_LAT": "str",\n  "WEST_LON": "str",\n  "EAST_LON": "str",\n  "BASE_URL": "str"\n}',
            "RENAME_HEADERS_LIST": '{\n  "GRANULE_ID": "granule_id",\n  "PRODUCT_ID": "product_id",\n  "DATATAKE_IDENTIFIER": "datatake_identifier",\n  "MGRS_TILE": "mgrs_tile",\n  "SENSING_TIME": "sensing_time",\n  "TOTAL_SIZE": "total_size",\n  "CLOUD_COVER": "cloud_cover",\n  "GEOMETRIC_QUALITY_FLAG": "geometric_quality_flag",\n  "GENERATION_TIME": "generation_time",\n  "NORTH_LAT": "north_lat",\n  "SOUTH_LAT": "south_lat",\n  "WEST_LON": "west_lon",\n  "EAST_LON": "east_lon",\n  "BASE_URL": "base_url"\n}',
            "REORDER_HEADERS_LIST": '[\n  "granule_id",\n  "product_id",\n  "datatake_identifier",\n  "mgrs_tile",\n  "sensing_time",\n  "geometric_quality_flag",\n  "generation_time",\n  "north_lat",\n  "south_lat",\n  "west_lon",\n  "east_lon",\n  "base_url",\n  "total_size",\n  "cloud_cover"\n]',
            "TABLE_DESCRIPTION": "Sentinel 2 table",
            "TABLE_CLUSTERING_FIELD_LIST": '[\n  "product_id",\n  "mgrs_tile",\n  "generation_time",\n  "datatake_identifier"\n]',
            "TABLE_PARTITION_FIELD": "sensing_time",
            "TABLE_PARTITION_FIELD_TYPE": "MONTH",
        },
        resources={"request_ephemeral_storage": "48G", "request_cpu": "4"},
    )
    delete_cluster = kubernetes_engine.GKEDeleteClusterOperator(
        task_id="delete_cluster",
        project_id="{{ var.value.gcp_project }}",
        location="us-central1-c",
        name="cloud-storage-geo-index",
    )

    create_cluster >> [landsat_index, sentinel_2] >> delete_cluster
