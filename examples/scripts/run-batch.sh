#!/usr/bin/env bash

java -cp ../target/beam-spatial-examples-bundled-0.0.1.jar com.davideanastasia.beam.spatial.Batch \
  --runner=DataflowRunner \
  --project=da-apache-beam-geo \
  --maxNumWorkers=6 \
  --region=europe-west1 \
  --tempLocation=gs://da-apache-beam-geo-data/temp/ \
  --inputFile=gs://da-apache-beam-geo-data/raw-data/*.txt \
  --outputFile=gs://da-apache-beam-geo-data/sessions-3/out


