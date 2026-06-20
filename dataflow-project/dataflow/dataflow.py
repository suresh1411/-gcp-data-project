import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

class ParseData(beam.DoFn):
    def process(self, element):
        record = json.loads(element)
        record["total_amount"] = record["price"] * record["quantity"]
        record["discount"] = record["total_amount"] * 0.1
        record["final_amount"] = record["total_amount"] - record["discount"]
        yield record

options = PipelineOptions(
    streaming=True,
    project="your-project-id",
    region="us-central1",
    temp_location="gs://your-bucket/temp"
)

with beam.Pipeline(options=options) as p:

    (p
     | "Read PubSub" >> beam.io.ReadFromPubSub(
            topic="projects/your-project-id/topics/orders-topic"
        )
     | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
     | "Transform" >> beam.ParDo(ParseData())
     | "Write BQ" >> beam.io.WriteToBigQuery(
            table="your-project-id:dataset.orders",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )