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
from airflow.providers.google.cloud.operators import bigquery, kubernetes_engine
from airflow.providers.google.cloud.transfers import gcs_to_bigquery

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-02-09",
}


with DAG(
    dag_id="thelook_ecommerce.thelook_ecommerce",
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
            "name": "pdp-thelook-ecommerce",
            "initial_node_count": 1,
            "network": "{{ var.value.vpc_network }}",
            "node_config": {
                "machine_type": "e2-highmem-16",
                "oauth_scopes": [
                    "https://www.googleapis.com/auth/devstorage.read_write",
                    "https://www.googleapis.com/auth/cloud-platform",
                ],
            },
        },
    )

    # Run CSV transform within kubernetes pod
    generate_thelook = kubernetes_engine.GKEStartPodOperator(
        task_id="generate_thelook",
        is_delete_operator_pod=False,
        name="generate_thelook",
        project_id="{{ var.value.gcp_project }}",
        location="us-central1-c",
        cluster_name="pdp-thelook-ecommerce",
        namespace="default",
        image_pull_policy="Always",
        image="{{ var.json.thelook_ecommerce.docker_image }}",
        env_vars={
            "NUM_OF_USERS": "100000",
            "NUM_OF_GHOST_EVENTS": "5",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PREFIX": "data/thelook_ecommerce",
            "SOURCE_DIR": "data",
            "EXTRANEOUS_HEADERS": '["event_type", "ip_address", "browser", "traffic_source", "session_id", "sequence_number", "uri", "is_sold"]',
        },
    )
    delete_cluster = kubernetes_engine.GKEDeleteClusterOperator(
        task_id="delete_cluster",
        project_id="{{ var.value.gcp_project }}",
        location="us-central1-c",
        name="pdp-thelook-ecommerce",
    )

    # Task to load Products data to a BigQuery table
    load_products_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_products_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/thelook_ecommerce/products.csv"],
        source_format="CSV",
        destination_project_dataset_table="thelook_ecommerce.products",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "brand", "type": "STRING", "mode": "NULLABLE"},
            {"name": "retail_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "department", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sku", "type": "STRING", "mode": "NULLABLE"},
            {"name": "distribution_center_id", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    # Task to load Events data to a BigQuery table
    load_events_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_events_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/thelook_ecommerce/events.csv"],
        source_format="CSV",
        destination_project_dataset_table="thelook_ecommerce.events",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "user_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sequence_number", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "session_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "ip_address", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "postal_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "browser", "type": "STRING", "mode": "NULLABLE"},
            {"name": "traffic_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "uri", "type": "STRING", "mode": "NULLABLE"},
            {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    # Task to load Iventory Items data to a BigQuery table
    load_inventory_items_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_inventory_items_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/thelook_ecommerce/inventory_items.csv"],
        source_format="CSV",
        destination_project_dataset_table="thelook_ecommerce.inventory_items",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "product_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "sold_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "product_category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_brand", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_retail_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "product_department", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product_sku", "type": "STRING", "mode": "NULLABLE"},
            {
                "name": "product_distribution_center_id",
                "type": "INTEGER",
                "mode": "NULLABLE",
            },
        ],
    )

    # Task to load Order Items data to a BigQuery table
    load_order_items_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_order_items_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/thelook_ecommerce/order_items.csv"],
        source_format="CSV",
        destination_project_dataset_table="thelook_ecommerce.order_items",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "order_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "user_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "product_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "inventory_item_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "shipped_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "delivered_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "returned_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "sale_price", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    # Task to load Orders data to a BigQuery table
    load_orders_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_orders_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/thelook_ecommerce/orders.csv"],
        source_format="CSV",
        destination_project_dataset_table="thelook_ecommerce.orders",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "order_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "user_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "returned_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "shipped_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "delivered_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "num_of_item", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    # Task to load Users data to a BigQuery table
    load_users_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_users_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/thelook_ecommerce/users.csv"],
        source_format="CSV",
        destination_project_dataset_table="thelook_ecommerce.users",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "age", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
            {"name": "state", "type": "STRING", "mode": "NULLABLE"},
            {"name": "street_address", "type": "STRING", "mode": "NULLABLE"},
            {"name": "postal_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "traffic_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ],
    )

    # Task to load Distribution Centers data to a BigQuery table
    load_distribution_centers_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_distribution_centers_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/thelook_ecommerce/distribution_centers.csv"],
        source_format="CSV",
        destination_project_dataset_table="thelook_ecommerce.distribution_centers",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    # Task to create the user geom column from the latitude and longitude columns
    create_user_geom_column = bigquery.BigQueryInsertJobOperator(
        task_id="create_user_geom_column",
        configuration={
            "query": {
                "query": "ALTER TABLE `{{ var.value.gcp_project }}.thelook_ecommerce.users` ADD COLUMN IF NOT EXISTS user_geom GEOGRAPHY;\nUPDATE `{{ var.value.gcp_project }}.thelook_ecommerce.users`\n   SET user_geom = SAFE.ST_GeogFromText(CONCAT('POINT(',CAST(longitude AS STRING), ' ', CAST(latitude as STRING), ')'))\n WHERE longitude IS NOT NULL AND latitude IS NOT NULL;",
                "useLegacySql": False,
            }
        },
    )

    # Task to create the distribution center geom column from the latitude and longitude columns
    create_distribution_center_geom_column = bigquery.BigQueryInsertJobOperator(
        task_id="create_distribution_center_geom_column",
        configuration={
            "query": "ALTER TABLE `{{ var.value.gcp_project }}.thelook_ecommerce.distribution_centers`\n  ADD COLUMN IF NOT EXISTS distribution_center_geom GEOGRAPHY;\nUPDATE `{{ var.value.gcp_project }}.thelook_ecommerce.distribution_centers`\n   SET distribution_center_geom = SAFE.ST_GeogFromText(CONCAT('POINT(',CAST(longitude AS STRING), ' ', CAST(latitude as STRING), ')'))\n WHERE longitude IS NOT NULL\n   AND latitude IS NOT NULL;\n# Use Legacy SQL should be false for any query that uses a DML statement",
            "useLegacySql": False,
        },
    )

    (
        create_cluster
        >> generate_thelook
        >> delete_cluster
        >> [
            load_products_to_bq,
            load_events_to_bq,
            load_inventory_items_to_bq,
            load_order_items_to_bq,
            load_orders_to_bq,
            load_users_to_bq,
            load_distribution_centers_to_bq,
        ]
        >> create_user_geom_column
        >> create_distribution_center_geom_column
    )
