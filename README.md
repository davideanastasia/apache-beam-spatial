# Apache Beam Spatial

Example on how to use a tile (Uber H3) system to generate 2D-aware sessions in Apache Beam

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