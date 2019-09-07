# Apache Beam Spatial

Example on how to use a tile system (Uber H3) to generate 2D-aware sessions in Apache Beam

Full explanation for the code in this repo is available in the article [Time/Space Sessions in Apache Beam](https://medium.com/@davide.anastasia/time-space-sessions-in-apache-beam-b402cdf8470) on Medium

## Examples

### Reference dataset

Dataset used in the examples/ folder is the [Microsoft T-Drive](https://www.microsoft.com/en-us/research/publication/t-drive-trajectory-data-sample/).

### How to run?

```bash
export PROJECT_ID=...
export REGION=...
export GOOGLE_APPLICATION_CREDENTIALS=...

java -cp ../target/beam-spatial-examples-bundled-0.0.1.jar com.davideanastasia.beam.spatial.Batch \
  --runner=DataflowRunner \
  --project=${PROJECT_ID} \
  --maxNumWorkers=6 \
  --region=${REGION} \
  --tempLocation=gs://${PROJECT_ID}-data/temp/ \
  --inputFile=gs://${PROJECT_ID}-data/raw-data/*.txt \
  --outputFile=gs://${PROJECT_ID}-data/sessions-3/out
```