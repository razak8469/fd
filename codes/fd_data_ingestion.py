import logging

import apache_beam as beam
import apache_beam.io.fileio as fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.runners import DirectRunner

PROJECT_ID = 'hardy-unison-369003'
REGION = 'us-west4'
FDH_BUCKET = 'gs://hardy-unison-369003-fraud-detection-handbook'
RAWDATA_BUCKET = f'{FDH_BUCKET}/simulated-data-raw/data'
FD_DATASET_RAW = 'fd_raw'
FD_TRANSACTIONS_TABLE_RAW = 'transactions'


SCHEMA = ','.join([
    "TRANSACTION_ID:INT64",
    "TX_DATETIME:TIMESTAMP",
    "CUSTOMER_ID:INT64",
    "TERMINAL_ID:INT64",
    "TX_AMOUNT:FLOAT64",
    "TX_TIME_SECONDS:INT64",
    "TX_TIME_DAYS:INT64",
    "TX_FRAUD:INT64",
    "TX_FRAUD_SCENARIO:INT64"
])

class ReadGcsBlobs(beam.DoFn):
    def process(self, element):
        from apache_beam.io.gcp import gcsio
        import pickle, json
        gcs = gcsio.GcsIO()
        result = pickle.load(gcs.open(element), encoding='utf-8')
        records = result.to_dict(orient='records')
        for elem in records:
            yield elem

def run(argv=None, save_main_session=True):

    dataflow_options = PipelineOptions(
        runner='DataflowRunner',
        project=PROJECT_ID,
        region=REGION,
        job_name='fd-data-ingestion',
        temp_location=f'{FDH_BUCKET}/temp',
    )

    dataflow_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=dataflow_options) as pipeline:
        pickle_files = (
            pipeline
            | fileio.MatchFiles(f'{RAWDATA_BUCKET}/*.pkl')
            | fileio.ReadMatches()
            | beam.Reshuffle()
        

        file_records = (
            pickle_files
            | "Get Pickle File Paths" >> beam.Map(lambda x: x.metadata.path)
            | "Transform To Json Dictionary" >> beam.ParDo(ReadGcsBlobs())
        )

        (
            file_records
            | "Write To BigQuery" >> beam.io.WriteToBigQuery(
                f'{PROJECT_ID}:{FD_DATASET_RAW}.{FD_TRANSACTIONS_TABLE_RAW}',
                schema=SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()