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
from airflow.providers.cncf.kubernetes.operators import kubernetes_pod

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-11-23",
}


with DAG(
    dag_id="cfe_calculator.copy_cfe_data",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@once",
    catchup=False,
    default_view="graph",
) as dag:

    # Transfer CFE Databases
    copy_bq_dataset = kubernetes_pod.KubernetesPodOperator(
        task_id="copy_bq_dataset",
        name="copy_bq_dataset",
        namespace="composer-user-workloads",
        service_account_name="default",
        config_file="/home/airflow/composer_kube_config",
        image_pull_policy="Always",
        image="{{ var.json.cfe_calculator.container_registry.copy_bq_datasets }}",
        env_vars={
            "SOURCE_PROJECT_ID": "{{ var.json.cfe_calculator.source_project_id }}",
            "TARGET_PROJECT_ID": "{{ var.json.cfe_calculator.target_project_id }}",
            "DATASET_NAME": "{{ var.json.cfe_calculator.dataset_name }}",
            "DATASET_VERSIONS": "{{ var.json.cfe_calculator.dataset_versions }}",
            "SERVICE_ACCOUNT": "{{ var.json.cfe_calculator.service_account }}",
        },
    )

    copy_bq_dataset
