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
    dag_id="nppes.nppes",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    dagrun_timeout=3600,
    catchup=False,
    default_view="graph",
) as dag:
    create_cluster = kubernetes_engine.GKECreateClusterOperator(
        task_id="create_cluster",
        project_id="{{ var.value.gcp_project }}",
        location="us-central1-c",
        body={
            "name": "pubds-nppes",
            "initial_node_count": 2,
            "network": "{{ var.value.vpc_network }}",
            "node_config": {
                "machine_type": "e2-highmem-4",
                "oauth_scopes": [
                    "https://www.googleapis.com/auth/devstorage.read_write",
                    "https://www.googleapis.com/auth/cloud-platform",
                ],
            },
        },
    )

    # Run CSV transform within kubernetes pod
    load_npi_raw = kubernetes_engine.GKEStartPodOperator(
        task_id="load_npi_raw",
        startup_timeout_seconds=3600,
        name="load_npi_raw",
        project_id="{{ var.value.gcp_project }}",
        location="us-central1-c",
        cluster_name="pubds-nppes",
        namespace="default",
        image_pull_policy="Always",
        image="{{ var.json.nppes.container_registry.run_csv_transform_kub }}",
        env_vars={
            "SOURCE_URL": "https://download.cms.gov/nppes/NPPES_Data_Dissemination_MM_YYYY.zip",
            "SOURCE_FILE": "files/data.csv",
            "TARGET_FILE": "files/data_output.csv",
            "SOURCE_NPI_DATA_FILE_REGEXP": "npidata_pfile_[0-9]+-[0-9]+.csv",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "data/nppes/npi_data_output.csv",
            "PROJECT_ID": "{{ var.value.gcp_project }}",
            "DATASET_ID": "nppes",
            "TABLE_ID": "npi_raw",
            "SCHEMA_PATH": "data/nppes/schema/npi_raw_schema.json",
            "CSV_HEADERS": '[\n   "npi",\n   "entity_type_code",\n   "replacement_npi",\n   "employer_identification_number_ein",\n   "provider_organization_name_legal_business_name",\n   "provider_last_name_legal_name",\n   "provider_first_name",\n   "provider_middle_name",\n   "provider_name_prefix_text",\n   "provider_name_suffix_text",\n   "provider_credential_text",\n   "provider_other_organization_name",\n   "provider_other_organization_name_type_code",\n   "provider_other_last_name",\n   "provider_other_first_name",\n   "provider_other_middle_name",\n   "provider_other_name_prefix_text",\n   "provider_other_name_suffix_text",\n   "provider_other_credential_text",\n   "provider_other_last_name_type_code",\n   "provider_first_line_business_mailing_address",\n   "provider_second_line_business_mailing_address",\n   "provider_business_mailing_address_city_name",\n   "provider_business_mailing_address_state_name",\n   "provider_business_mailing_address_postal_code",\n   "provider_business_mailing_address_country_code_if_outside_us",\n   "provider_business_mailing_address_telephone_number",\n   "provider_business_mailing_address_fax_number",\n   "provider_first_line_business_practice_location_address",\n   "provider_second_line_business_practice_location_address",\n   "provider_business_practice_location_address_city_name",\n   "provider_business_practice_location_address_state_name",\n   "provider_business_practice_location_address_postal_code",\n   "provider_business_practice_location_address_country_code_if_outside_us",\n   "provider_business_practice_location_address_telephone_number",\n   "provider_business_practice_location_address_fax_number",\n   "provider_enumeration_date",\n   "last_update_date",\n   "npi_deactivation_reason_code",\n   "npi_deactivation_date",\n   "npi_reactivation_date",\n   "provider_gender_code",\n   "authorized_official_last_name",\n   "authorized_official_first_name",\n   "authorized_official_middle_name",\n   "authorized_official_title_or_position",\n   "authorized_official_telephone_number",\n   "healthcare_provider_taxonomy_code_1",\n   "provider_license_number_1",\n   "provider_license_number_state_code_1",\n   "healthcare_provider_primary_taxonomy_switch_1",\n   "healthcare_provider_taxonomy_code_2",\n   "provider_license_number_2",\n   "provider_license_number_state_code_2",\n   "healthcare_provider_primary_taxonomy_switch_2",\n   "healthcare_provider_taxonomy_code_3",\n   "provider_license_number_3",\n   "provider_license_number_state_code_3",\n   "healthcare_provider_primary_taxonomy_switch_3",\n   "healthcare_provider_taxonomy_code_4",\n   "provider_license_number_4",\n   "provider_license_number_state_code_4",\n   "healthcare_provider_primary_taxonomy_switch_4",\n   "healthcare_provider_taxonomy_code_5",\n   "provider_license_number_5",\n   "provider_license_number_state_code_5",\n   "healthcare_provider_primary_taxonomy_switch_5",\n   "healthcare_provider_taxonomy_code_6",\n   "provider_license_number_6",\n   "provider_license_number_state_code_6",\n   "healthcare_provider_primary_taxonomy_switch_6",\n   "healthcare_provider_taxonomy_code_7",\n   "provider_license_number_7",\n   "provider_license_number_state_code_7",\n   "healthcare_provider_primary_taxonomy_switch_7",\n   "healthcare_provider_taxonomy_code_8",\n   "provider_license_number_8",\n   "provider_license_number_state_code_8",\n   "healthcare_provider_primary_taxonomy_switch_8",\n   "healthcare_provider_taxonomy_code_9",\n   "provider_license_number_9",\n   "provider_license_number_state_code_9",\n   "healthcare_provider_primary_taxonomy_switch_9",\n   "healthcare_provider_taxonomy_code_10",\n   "provider_license_number_10",\n   "provider_license_number_state_code_10",\n   "healthcare_provider_primary_taxonomy_switch_10",\n   "healthcare_provider_taxonomy_code_11",\n   "provider_license_number_11",\n   "provider_license_number_state_code_11",\n   "healthcare_provider_primary_taxonomy_switch_11",\n   "healthcare_provider_taxonomy_code_12",\n   "provider_license_number_12",\n   "provider_license_number_state_code_12",\n   "healthcare_provider_primary_taxonomy_switch_12",\n   "healthcare_provider_taxonomy_code_13",\n   "provider_license_number_13",\n   "provider_license_number_state_code_13",\n   "healthcare_provider_primary_taxonomy_switch_13",\n   "healthcare_provider_taxonomy_code_14",\n   "provider_license_number_14",\n   "provider_license_number_state_code_14",\n   "healthcare_provider_primary_taxonomy_switch_14",\n   "healthcare_provider_taxonomy_code_15",\n   "provider_license_number_15",\n   "provider_license_number_state_code_15",\n   "healthcare_provider_primary_taxonomy_switch_15",\n   "other_provider_identifier_1",\n   "other_provider_identifier_type_code_1",\n   "other_provider_identifier_state_1",\n   "other_provider_identifier_issuer_1",\n   "other_provider_identifier_2",\n   "other_provider_identifier_type_code_2",\n   "other_provider_identifier_state_2",\n   "other_provider_identifier_issuer_2",\n   "other_provider_identifier_3",\n   "other_provider_identifier_type_code_3",\n   "other_provider_identifier_state_3",\n   "other_provider_identifier_issuer_3",\n   "other_provider_identifier_4",\n   "other_provider_identifier_type_code_4",\n   "other_provider_identifier_state_4",\n   "other_provider_identifier_issuer_4",\n   "other_provider_identifier_5",\n   "other_provider_identifier_type_code_5",\n   "other_provider_identifier_state_5",\n   "other_provider_identifier_issuer_5",\n   "other_provider_identifier_6",\n   "other_provider_identifier_type_code_6",\n   "other_provider_identifier_state_6",\n   "other_provider_identifier_issuer_6",\n   "other_provider_identifier_7",\n   "other_provider_identifier_type_code_7",\n   "other_provider_identifier_state_7",\n   "other_provider_identifier_issuer_7",\n   "other_provider_identifier_8",\n   "other_provider_identifier_type_code_8",\n   "other_provider_identifier_state_8",\n   "other_provider_identifier_issuer_8",\n   "other_provider_identifier_9",\n   "other_provider_identifier_type_code_9",\n   "other_provider_identifier_state_9",\n   "other_provider_identifier_issuer_9",\n   "other_provider_identifier_10",\n   "other_provider_identifier_type_code_10",\n   "other_provider_identifier_state_10",\n   "other_provider_identifier_issuer_10",\n   "other_provider_identifier_11",\n   "other_provider_identifier_type_code_11",\n   "other_provider_identifier_state_11",\n   "other_provider_identifier_issuer_11",\n   "other_provider_identifier_12",\n   "other_provider_identifier_type_code_12",\n   "other_provider_identifier_state_12",\n   "other_provider_identifier_issuer_12",\n   "other_provider_identifier_13",\n   "other_provider_identifier_type_code_13",\n   "other_provider_identifier_state_13",\n   "other_provider_identifier_issuer_13",\n   "other_provider_identifier_14",\n   "other_provider_identifier_type_code_14",\n   "other_provider_identifier_state_14",\n   "other_provider_identifier_issuer_14",\n   "other_provider_identifier_15",\n   "other_provider_identifier_type_code_15",\n   "other_provider_identifier_state_15",\n   "other_provider_identifier_issuer_15",\n   "other_provider_identifier_16",\n   "other_provider_identifier_type_code_16",\n   "other_provider_identifier_state_16",\n   "other_provider_identifier_issuer_16",\n   "other_provider_identifier_17",\n   "other_provider_identifier_type_code_17",\n   "other_provider_identifier_state_17",\n   "other_provider_identifier_issuer_17",\n   "other_provider_identifier_18",\n   "other_provider_identifier_type_code_18",\n   "other_provider_identifier_state_18",\n   "other_provider_identifier_issuer_18",\n   "other_provider_identifier_19",\n   "other_provider_identifier_type_code_19",\n   "other_provider_identifier_state_19",\n   "other_provider_identifier_issuer_19",\n   "other_provider_identifier_20",\n   "other_provider_identifier_type_code_20",\n   "other_provider_identifier_state_20",\n   "other_provider_identifier_issuer_20",\n   "other_provider_identifier_21",\n   "other_provider_identifier_type_code_21",\n   "other_provider_identifier_state_21",\n   "other_provider_identifier_issuer_21",\n   "other_provider_identifier_22",\n   "other_provider_identifier_type_code_22",\n   "other_provider_identifier_state_22",\n   "other_provider_identifier_issuer_22",\n   "other_provider_identifier_23",\n   "other_provider_identifier_type_code_23",\n   "other_provider_identifier_state_23",\n   "other_provider_identifier_issuer_23",\n   "other_provider_identifier_24",\n   "other_provider_identifier_type_code_24",\n   "other_provider_identifier_state_24",\n   "other_provider_identifier_issuer_24",\n   "other_provider_identifier_25",\n   "other_provider_identifier_type_code_25",\n   "other_provider_identifier_state_25",\n   "other_provider_identifier_issuer_25",\n   "other_provider_identifier_26",\n   "other_provider_identifier_type_code_26",\n   "other_provider_identifier_state_26",\n   "other_provider_identifier_issuer_26",\n   "other_provider_identifier_27",\n   "other_provider_identifier_type_code_27",\n   "other_provider_identifier_state_27",\n   "other_provider_identifier_issuer_27",\n   "other_provider_identifier_28",\n   "other_provider_identifier_type_code_28",\n   "other_provider_identifier_state_28",\n   "other_provider_identifier_issuer_28",\n   "other_provider_identifier_29",\n   "other_provider_identifier_type_code_29",\n   "other_provider_identifier_state_29",\n   "other_provider_identifier_issuer_29",\n   "other_provider_identifier_30",\n   "other_provider_identifier_type_code_30",\n   "other_provider_identifier_state_30",\n   "other_provider_identifier_issuer_30",\n   "other_provider_identifier_31",\n   "other_provider_identifier_type_code_31",\n   "other_provider_identifier_state_31",\n   "other_provider_identifier_issuer_31",\n   "other_provider_identifier_32",\n   "other_provider_identifier_type_code_32",\n   "other_provider_identifier_state_32",\n   "other_provider_identifier_issuer_32",\n   "other_provider_identifier_33",\n   "other_provider_identifier_type_code_33",\n   "other_provider_identifier_state_33",\n   "other_provider_identifier_issuer_33",\n   "other_provider_identifier_34",\n   "other_provider_identifier_type_code_34",\n   "other_provider_identifier_state_34",\n   "other_provider_identifier_issuer_34",\n   "other_provider_identifier_35",\n   "other_provider_identifier_type_code_35",\n   "other_provider_identifier_state_35",\n   "other_provider_identifier_issuer_35",\n   "other_provider_identifier_36",\n   "other_provider_identifier_type_code_36",\n   "other_provider_identifier_state_36",\n   "other_provider_identifier_issuer_36",\n   "other_provider_identifier_37",\n   "other_provider_identifier_type_code_37",\n   "other_provider_identifier_state_37",\n   "other_provider_identifier_issuer_37",\n   "other_provider_identifier_38",\n   "other_provider_identifier_type_code_38",\n   "other_provider_identifier_state_38",\n   "other_provider_identifier_issuer_38",\n   "other_provider_identifier_39",\n   "other_provider_identifier_type_code_39",\n   "other_provider_identifier_state_39",\n   "other_provider_identifier_issuer_39",\n   "other_provider_identifier_40",\n   "other_provider_identifier_type_code_40",\n   "other_provider_identifier_state_40",\n   "other_provider_identifier_issuer_40",\n   "other_provider_identifier_41",\n   "other_provider_identifier_type_code_41",\n   "other_provider_identifier_state_41",\n   "other_provider_identifier_issuer_41",\n   "other_provider_identifier_42",\n   "other_provider_identifier_type_code_42",\n   "other_provider_identifier_state_42",\n   "other_provider_identifier_issuer_42",\n   "other_provider_identifier_43",\n   "other_provider_identifier_type_code_43",\n   "other_provider_identifier_state_43",\n   "other_provider_identifier_issuer_43",\n   "other_provider_identifier_44",\n   "other_provider_identifier_type_code_44",\n   "other_provider_identifier_state_44",\n   "other_provider_identifier_issuer_44",\n   "other_provider_identifier_45",\n   "other_provider_identifier_type_code_45",\n   "other_provider_identifier_state_45",\n   "other_provider_identifier_issuer_45",\n   "other_provider_identifier_46",\n   "other_provider_identifier_type_code_46",\n   "other_provider_identifier_state_46",\n   "other_provider_identifier_issuer_46",\n   "other_provider_identifier_47",\n   "other_provider_identifier_type_code_47",\n   "other_provider_identifier_state_47",\n   "other_provider_identifier_issuer_47",\n   "other_provider_identifier_48",\n   "other_provider_identifier_type_code_48",\n   "other_provider_identifier_state_48",\n   "other_provider_identifier_issuer_48",\n   "other_provider_identifier_49",\n   "other_provider_identifier_type_code_49",\n   "other_provider_identifier_state_49",\n   "other_provider_identifier_issuer_49",\n   "other_provider_identifier_50",\n   "other_provider_identifier_type_code_50",\n   "other_provider_identifier_state_50",\n   "other_provider_identifier_issuer_50",\n   "is_sole_proprietor",\n   "is_organization_subpart",\n   "parent_organization_lbn",\n   "parent_organization_tin",\n   "authorized_official_name_prefix_text",\n   "authorized_official_name_suffix_text",\n   "authorized_official_credential_text",\n   "healthcare_provider_taxonomy_group_1",\n   "healthcare_provider_taxonomy_group_2",\n   "healthcare_provider_taxonomy_group_3",\n   "healthcare_provider_taxonomy_group_4",\n   "healthcare_provider_taxonomy_group_5",\n   "healthcare_provider_taxonomy_group_6",\n   "healthcare_provider_taxonomy_group_7",\n   "healthcare_provider_taxonomy_group_8",\n   "healthcare_provider_taxonomy_group_9",\n   "healthcare_provider_taxonomy_group_10",\n   "healthcare_provider_taxonomy_group_11",\n   "healthcare_provider_taxonomy_group_12",\n   "healthcare_provider_taxonomy_group_13",\n   "healthcare_provider_taxonomy_group_14",\n   "healthcare_provider_taxonomy_group_15",\n   "certification_date"\n]',
            "PIPELINE_NAME": "NPPES",
        },
        resources={
            "request_ephemeral_storage": "10G",
            "limit_memory": "48G",
            "limit_cpu": "2",
        },
    )
    delete_cluster = kubernetes_engine.GKEDeleteClusterOperator(
        task_id="delete_cluster",
        project_id="{{ var.value.gcp_project }}",
        location="us-central1-c",
        name="pubds-nppes",
    )

    create_cluster >> load_npi_raw >> delete_cluster
