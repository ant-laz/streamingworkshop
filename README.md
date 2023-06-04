# Apache Beam streaming pipeline workshop
Step by step development of a streaming pipeline in Python using Apace Beam.

In this workshop we will develop a streaming pipeline, showing how to get data in JSON 
format and parse it (using Beam schemas), how to aggregate it and write out to files.

We will apply complex analytics to the stream, calculating properties of a session for 
different users, grouping together events of the same session by using windowing.

As a bonus point, we will explore how to use the parameters of a DoFn to inspect the 
properties of the window, adding information about the window and the trigger, to 
understand the concepts of windowing, triggering and accumulation mode.

## Choosing the correct code branch

The "main" branch has the complete code for the streaming pipeline.

The "workshop" branch has code with gaps that are to be completed live in the workshop.

Checkout the "workshop" branch and challenge yourself ! 

```
git checkout workshop
```

## Setting up your environment

### Python 3.10

This is a hands on workshop, please come ready with Python 3.10 installed: 
 * use the Python official [downloads](https://www.python.org/downloads/)
 * use pyenv
 * follow the instructions for Google [Cloud](https://cloud.google.com/python/docs/setup)
 * ...or whatever you prefer ! 

### Development Environment

This is a hands on workshop, please come ready with a development environment:
 * VSCode
 * PyCharm
 * GCP cloud shell 
 * GCP cloud shell editor
 * GCP workstation
 * ...or whatever you prefer ! 


### Python Virtual Env

With *python* pointing at python 3.10 run the following.

Create a virtual environment
```
python -m venv venv
```
Activate the virtual environment.
```
source venv/bin/activate
```

While activated, your `python` and `pip` commands will point to the virtual environment,
so any changes or install dependencies are self-contained.

### Initialize pipeline code

Execute form the root of this repo to initialze the pipeline code.

First, update pip before installing dependencies, it's always a good idea to do this.
```sh
pip install -U pip
```

Install the project as a local package, this installs all the dependencies as well.
```
pip install -e .
```

## The taxi ride dataset being used in this workshop

The taxi tycoon dataset was created for this codelab:

https://github.com/googlecodelabs/cloud-dataflow-nyc-taxi-tycoon

It is a public pub/sub topic which is continuously updated. It is found here:

projects/pubsub-public-data/topics/taxirides-realtime

To save time, samples of this pub/sub topic have already been downloaded for you.

The following samples have been taking:

  * 01_complete_ride*.txt are messages from rides with pickup,enroute & dropoff events
  * All other files differ according to how long we spent downlading the pubs/sub msgs
    * 05min_of_rides.txt
    * 10min_of_rides.txt
    * 20min_of_rides.txt

## Unzip the taxi ride datasets before executing the beam pipeline

To get around GitHub 100MB upload limit the samples of ride data messages were zip.

Unzip them before proceeding.

Execute form the root of this repo:

```sh
unzip data/input/05min_of_rides.zip -d data/input/
unzip data/input/10min_of_rides.zip -d data/input/
unzip data/input/20min_of_rides.zip -d data/input/
```

## TESTS - Running the test suite for the pipeline

To run the all the tests
```
python -m unittest -v
```

To run just a single test
```
python -m unittest test.test_pipeline.TestTaxiPointCreation.test_task_1_taxi_point_creation
```
```
python -m unittest test.test_pipeline.TestAddTimestampDoFn.test_task_2_add_timestamp_dofn
```
```
python -m unittest test.test_pipeline.TestAddingKeysToTaxiRides.test_task_3_adding_keys_to_taxi_rides
```
```
python -m unittest test.test_pipeline.TestTaxiRideSessions.test_task_4_taxi_ride_sessions
```
```
python -m unittest test.test_pipeline.TestStatsCalculation.test_task_5_stats_calculation
```
```
python -m unittest test.test_pipeline.TestEndToEndPipeline.test_task_6_end_to_end_pipeline
```

## DirectRunner - BATCH MODE - Executing beam pipeline to process taxi ride data

It is possible to run the pipeline using the DirectRunner in batch mode.

```sh
# batch mode
python main.py  \
  --runner DirectRunner \
  --save_main_session \
  --setup_file ./setup.py \
  --input_filename=data/input/01_complete_ride_277278c9-9e5e-4aa9-b1b9-3e38e2133e5f.txt \
  --output_filename=data/output/01_complete_ride_ride_analysis
```


## DirectRunner - STREAMING MODE - Executing beam pipeline to process taxi ride data

It is possible to run the pipeline using the DirectRunner in streaming mode. 

```sh
# streaming mode
python main.py  \
  --runner DirectRunner \
  --streaming \
  --save_main_session \
  --setup_file ./setup.py \
  --input_filename=data/input/01_complete_ride_277278c9-9e5e-4aa9-b1b9-3e38e2133e5f.txt \
  --output_filename=data/output/01_complete_ride_ride_analysis
```

However, because of this bug: 

https://github.com/apache/beam/issues/19070

Certain code will fail when using the DirectRunner in STREAMING mode. 

For example, printing to stdout locally when trying to debug the pipeline.

## DirectRunner - Details of each command line flag used: 

| Flag | Description |
| --- | --- |
| runner | Apache Beam execution engine or "runner", e.g. DirectRunner or DataflowRunner |
| streaming | If present, pipeline executes in streaming mode otherwise in batch mode |
| save_main_session | Make global imports availabe to all dataflow workers [details](https://cloud.google.com/dataflow/docs/guides/common-errors#name-error) |
| setup_file | To hanle Multiple File Dependencies [details](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/) |
| input-filename | Custom. The input file to the pipeline. |
| output-filename | Custom. The output file of the pipeline |

## DataflowRunner - Executing beam pipeline to process taxi ride data

It is possible to run the pipeline using the Dataflow runner. 

First create some evironmental variables as follows:

```sh
export GCP_PROJECT_ID=$(gcloud config list core/project --format="value(core.project)")

export GCP_PROJECT_NUM=$(gcloud projects describe $GCP_PROJECT_ID --format="value(projectNumber)")

export GCP_BUCKET_REGION="US"

export GCP_DATAFLOW_REGION="us-east4"

export GCS_BUCKET=gs://${GCP_PROJECT_ID}-beamsummit2023

export GCS_BUCKET_TMP=${GCS_BUCKET}/tmp/

export GCS_BUCKET_INPUT=${GCS_BUCKET}/input

export GCS_BUCKET_OUTPUT=${GCS_BUCKET}/output

export EMAIL_ADDRESS=your_email@domain.com
```

Next create a GCS bucket for temp, input & output data.

```sh
gcloud storage buckets create ${GCS_BUCKET} \
  --project=${GCP_PROJECT_ID} \
  --location=${GCP_BUCKET_REGION} \
  --uniform-bucket-level-access

gcloud storage buckets add-iam-policy-binding ${GCS_BUCKET} \
--member=allUsers \
--role=roles/storage.objectViewer
```

Next upload our taxi data into the newly created bucket

```sh
gcloud storage cp 01_complete_ride_277278c9-9e5e-4aa9-b1b9-3e38e2133e5f.txt ${GCS_BUCKET_INPUT}
gcloud storage cp 05min_of_rides.txt ${GCS_BUCKET_INPUT}
gcloud storage cp 10min_of_rides.txt ${GCS_BUCKET_INPUT}
gcloud storage cp 20min_of_rides.txt ${GCS_BUCKET_INPUT}
```


Next create authentication details for your Google account

```sh
gcloud auth application-default login
```

To successfully run Dataflow jobs, your user must have permissions to do so.

Run the following commands to update the permissions on your user.

```sh
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} \
--member="user:${EMAIL_ADDRESS}" \
--role=roles/iam.serviceAccountUser
```

To successfully run Dataflow jobs, Dataflow service account must have resource access.

The Dataflow service account is the default GCP Project Compute Engine service account:

 * it executes dataflow, e.g. launching jobs
 * it accesses resources from dataflow workers, e.g. GCS buckets

Run the following commands to update permissions on this service account for dataflow:

```sh
gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} \
 --member="serviceAccount:${GCP_PROJECT_NUM}-compute@developer.gserviceaccount.com" \
--role=roles/dataflow.admin

gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} \
 --member="serviceAccount:${GCP_PROJECT_NUM}-compute@developer.gserviceaccount.com" \
--role=roles/dataflow.worker

gcloud projects add-iam-policy-binding ${GCP_PROJECT_ID} \
 --member="serviceAccount:${GCP_PROJECT_NUM}-compute@developer.gserviceaccount.com" \
--role=roles/storage.objectAdmin

```

Using these environmental variables, the pipeline can be executed as follows:

```sh
# batch mode
python main.py  \
  --runner DataflowRunner \
  --save_main_session \
  --setup_file ./setup.py \
  --project ${GCP_PROJECT_ID} \
  --region ${GCP_DATAFLOW_REGION} \
  --temp_location ${GCS_BUCKET_TMP} \
  --input_filename=${GCS_BUCKET_INPUT}/01_complete_ride_277278c9-9e5e-4aa9-b1b9-3e38e2133e5f.txt \
  --output_filename=${GCS_BUCKET_OUTPUT}/01_complete_ride_analysis
```

```sh
# streaming mode
python main.py  \
  --runner DataflowRunner \
  --streaming \
  --save_main_session \
  --setup_file ./setup.py \
  --project ${GCP_PROJECT_ID} \
  --region ${GCP_DATAFLOW_REGION} \
  --temp_location ${GCS_BUCKET_TMP} \
  --input_filename=${GCS_BUCKET_INPUT}/01_complete_ride_277278c9-9e5e-4aa9-b1b9-3e38e2133e5f.txt \
  --output_filename=${GCS_BUCKET_OUTPUT}/01_complete_ride_analysis
```



Details of each flag used: 

| Flag | Description |
| --- | --- |
| runner | Apache Beam execution engine or "runner", e.g. DirectRunner or DataflowRunner |
| streaming | If present, pipeline executes in streaming mode otherwise in batch mode |
| save_main_session | Make global imports availabe to all dataflow workers [details](https://cloud.google.com/dataflow/docs/guides/common-errors#name-error) |
| setup_file | To hanle Multiple File Dependencies [details](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/) |
| project | The GCP project where dataflow will execute |
| region | The GCP region where dataflow will execute |
| temp_location | A Temporary location for dataflow to use during execution |
| input_filename | Custom. The input file to the pipeline. |
| output_filename | Custom. The output file of the pipeline |

## Debugging the beam pipeline

Use the AnalyzeElement DoFn for info on PCollection elements, timestamps & windows.

You can configure the minimum & maximum expected timestamps for your pipeline.

This DoFn will use these to flag suspicious timestamps & windows as per this example:

```sh
####### BEGIN details of this element #####

>>>> element
{'ride_id': '277278c9-9e5e-4aa9-b1b9-3e38e2133e5f', 'point_idx': 278, 'latitude': 40.74541000000001, 'longitude': -73.98677, 'timestamp': '2023-04-21T16:37:24.7316-04:00', 'meter_reading': 17.25, 'meter_increment': 0.06205036, 'ride_status': 'dropoff', 'passenger_count': 2}
<class 'dict'>

>>>> timestamp
type(timestamp) -> <class 'apache_beam.utils.timestamp.Timestamp'>
timestamp.micros -> -9223372036854775000
timestamps on elements look incorrect, did you assign?

>>>>window
type(window) -> <class 'apache_beam.transforms.window.GlobalWindow'>
window.start -> Timestamp(-9223372036854.775000)
window.end -> Timestamp(9223371950454.775000)
windows on elements look incorrect, did you assign?


####### END details of this element #####
```

If the timestamp & windows values look fine you will a full output as per this example:


```sh
####### BEGIN details of this element #####

>>>> element
('277278c9-9e5e-4aa9-b1b9-3e38e2133e5f', {'ride_id': '277278c9-9e5e-4aa9-b1b9-3e38e2133e5f', 'point_idx': 278, 'latitude': 40.74541000000001, 'longitude': -73.98677, 'timestamp': '2023-04-21T16:37:24.7316-04:00', 'meter_reading': 17.25, 'meter_increment': 0.06205036, 'ride_status': 'dropoff', 'passenger_count': 2})
<class 'tuple'>

>>>> timestamp
type(timestamp) -> <class 'apache_beam.utils.timestamp.Timestamp'>
timestamp.micros -> 1682109444731600
timestamp.to_rfc3339() -> '2023-04-21T20:37:24.731600Z'
timestamp.to_utc_datetime() -> datetime.datetime(2023, 4, 21, 20, 37, 24, 731600)

>>>>window
type(window) -> <class 'apache_beam.transforms.window.IntervalWindow'>
window.start -> Timestamp(1682109444.731600)
window.start (utc datetime) -> 2023-04-21 20:37:24.731600 
window.end -> Timestamp(1682109474.731600)
window.end (utc datetime) -> 2023-04-21 20:37:54.731600 
window.max_timestamp() -> Timestamp(1682109474.731599) 
window.max_timestamp() (utc datetime) -> 2023-04-21 20:37:54.731599 


####### END details of this element #####
```
