# Apache Beam Spatial

This project contains a modified session algorithm for the calculation of time/space sessions using Apache Beam. The algorithm leverages
Uber H3 (an hex-based tiling system), and works in either batch or streaming mode. With little modification, it can be extended to work using Google S2 or the classic Geohash algorithm.

The code is support material for the article [Time/Space Sessions in Apache Beam](https://medium.com/@davide.anastasia/time-space-sessions-in-apache-beam-b402cdf8470) on Medium.

## Examples

### Reference dataset

Most of the code in the project (including the demo app) is tested with the [Microsoft T-Drive](https://www.microsoft.com/en-us/research/publication/t-drive-trajectory-data-sample/) dataset.

### On Google Dataflow

Execution of the batch job on Google Dataflow can be performed using:

```bash
export GCP_PROJECT=...
export GCP_REGION=...
export GOOGLE_APPLICATION_CREDENTIALS=...

java -cp ../target/beam-spatial-examples-bundled-0.0.1.jar com.davideanastasia.beam.spatial.Batch \
  --runner=DataflowRunner \
  --project=${GCP_PROJECT} \
  --region=${GCP_REGION} \
  --maxNumWorkers=6 \
  --tempLocation=gs://${GCP_PROJECT}-data/temp/ \
  --inputFile=gs://${GCP_PROJECT}-data/raw-data/*.txt \
  --outputFile=gs://${GCP_PROJECT}-data/sessions-3/out
```

# Demo

The folder `demo/` contains a combination of dataflow + python to create a real-time mapping visualization of the taxi data. For further info, please refer to the README.md file in the same folder.

