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


resource "google_bigquery_dataset" "chicago_taxi_trips" {
  dataset_id  = "chicago_taxi_trips"
  project     = var.project_id
  description = "This dataset consistes of Taxi trips reported to the City of Chicago in its role as a regulatory agency."
}

output "bigquery_dataset-chicago_taxi_trips-dataset_id" {
  value = google_bigquery_dataset.chicago_taxi_trips.dataset_id
}
