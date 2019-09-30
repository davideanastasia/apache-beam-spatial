#!/usr/bin/env bash

java -cp ../target/beam-spatial-examples-bundled-0.0.1.jar com.davideanastasia.beam.spatial.Batch \
  --runner=DataflowRunner \
  --project=${GCP_PROJECT} \
  --region=${GCP_REGION} \
  --maxNumWorkers=4 \
  --tempLocation=gs://da-apache-beam-geo-data/temp/ \
  --inputFile=gs://da-apache-beam-geo-data/raw-data/*.txt \
  --outputFile=gs://da-apache-beam-geo-data/sessions-4/out


