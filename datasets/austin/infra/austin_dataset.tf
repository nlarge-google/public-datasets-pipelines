/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


resource "google_bigquery_dataset" "austin_311" {
  dataset_id  = "austin_311"
  project     = var.project_id
  description = "Austin 311 Service Requests"
}

output "bigquery_dataset-austin_311-dataset_id" {
  value = google_bigquery_dataset.austin_311.dataset_id
}

resource "google_bigquery_dataset" "austin_bikeshare_trips" {
  dataset_id  = "austin_bikeshare_trips"
  project     = var.project_id
  description = "Austin Bikeshare Trips"
}

output "bigquery_dataset-austin_bikeshare_trips-dataset_id" {
  value = google_bigquery_dataset.austin_bikeshare_trips.dataset_id
}
