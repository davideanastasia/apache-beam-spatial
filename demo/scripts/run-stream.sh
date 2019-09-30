#!/usr/bin/env bash

java -cp ../examples/target/beam-spatial-examples-bundled-0.0.1.jar com.davideanastasia.beam.spatial.Stream \
  --runner=DataflowRunner \
  --project=${GCP_PROJECT} \
  --region=${GCP_REGION} \
  --autoscalingAlgorithm=THROUGHPUT_BASED \
  --maxNumWorkers=4 \
  --enableStreamingEngine \
  --diskSizeGb=30 \
  --profilingAgentConfiguration="{\"APICurated\": true}" \
  --tempLocation=gs://${GCP_PROJECT}-data/temp/ \
  --inputTopicName=projects/${GCP_PROJECT}/topics/t-drive-data \
  --outputTopicName=projects/${GCP_PROJECT}/topics/t-drive-session
