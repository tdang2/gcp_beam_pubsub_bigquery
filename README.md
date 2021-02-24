# GCP with Dataflow - PubSub - BigQuery

## Description
This Python project set up the following working components on Google Cloud Platform (GCP)  

- Publishing messages name, action, and created_at timestamp to a GCP topic
- Subscribing the messages to from the topic's subscription and print out the msg 
- An apache beam dataflow that perform the following tasks

    - accepts input arguments to listen to a GCP topic or subscription
    - accepts input arguments to save the event data to big query with some data transformation
    - perform streaming analytic with a 10-second window and 30 minute allowed lateness watermark
    - the streaming analytic aggregate the number of time a name is mentioned within the window and print it out


## Prerequisites

In order to run this code, you will need the following set up

- A working GCP project with BigQuery, PubSub, and Dataflow enabled
- Install and initialize GCP Cloud SDK: https://cloud.google.com/sdk/docs/
- A service account that have the appropriate permission to read and write to the above services
- Without the service account, you can use the default google credentials as well
- In BigQuery console window, create a dataset named "CoreTest" and a table named "dataflow_mvp"
- The table schema can be referred inside mvp.py file toward the end of the source code
- Create a .env file with the following keys
  - GCP_DEFAULT_CREDENTIALS=the absolute path of your credential json file
  - TOPIC_ID=GCP topic id name only
  - SUBSCRIPTION_ID=GCP subscription id name only


## Structure

- pubSubMessage folder contain two python files
  - publish.py is used to publish events to a topic
  - subscribe.py is used to subscribe events from the topic's subscription 

- mvp.py is an apache beam code which retrieves data from the subscription or a topic

  - it allows watermark and 30 minute lateness
  - it processes created_at format so that big query can accept as streaming insert
  - it saves the output to bigquery to CoreTest.dataflow_mvp
  - it aggregates name and number of action per name per window batch and print it out
  
- Dataflow getting started example provided from [Apache Beam](https://beam.apache.org/get-started/beam-overview/)

## Usages  

This section assumes you already complete all the prerequisites mentioned above
From this point, we will simulate all the components using Terminal interface. 
The dataflow will also run via DirectRunner instead of deploying to GCP dataflow.
You can deploy the apache beam code to dataflow runner if you prefer. Make sure to 
provide the appropriate credentials. Refer to [Dataflow Python Quickstart](https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python)


- After turning on virtualenv and install the dependencies, turn on the component to publish events

      cd pubSubMessages
      python publish.py

- Optional: In a different window, turn on the component to subscribe to the events

      cd pubSubMessgaes
      python subscribe.py

- To turn on dataflow runner, first provide default GCP credentials to your terminal window

      export GOOGLE_APPLICATION_CREDENTIALS="<absolute path of the credential json file>"

- then run the dataflow with DirectRunner 

      python -m mvp --output_table <project-id>:<dataset-id>.<table-id> --input_subscription projects/<project-id>/subscriptions/<subscription-id> 

## Deploy on GCP
- More detail from [GCP Flex Template example](https://github.com/GoogleCloudPlatform/python-docs-samples/tree/master/dataflow/flex-templates/streaming_beam)

- Set shell environment variables

      export REGION="us-central1"
      export SUBSCRIPTION="<your_subscription_name>"
      export PROJECT="<your_project_name>"
      export DATASET="<your_dataset_name>"
      export TABLE="<your_table_name"
      export BUCKET="<you_gcp_storage_bucket_name>"

- Docker Build the template. In this case, the image name is tri-beam
  
      export TEMPLATE_IMAGE="gcr.io/$PROJECT/dataflow/tri-beam:latest"
      gcloud builds submit --tag "$TEMPLATE_IMAGE"
  
- Create the template json. In this case, the template json file name is tri-beam

      export TEMPLATE_PATH="gs://$BUCKET/test/templates/tri-beam.json"
      gcloud dataflow flex-template build $TEMPLATE_PATH \
        --image "$TEMPLATE_IMAGE" \
        --sdk-language "PYTHON" \
        --metadata-file "metadata.json"

- Running Dataflow flex template. In this case, the job name is tri-beam-<YYMMDD-HHMMSS>
 
      gcloud dataflow flex-template run "tri-beam-`date +%Y%m%d-%H%M%S`" \
        --template-file-gcs-location "$TEMPLATE_PATH" \
        --parameters input_topic="projects/$PROJECT/topics/$TOPIC" \
        --parameters output_table="$PROJECT:$DATASET.$TABLE" \
        --region "$REGION"
  