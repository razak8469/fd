import logging, json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.pvalue import TaggedOutput

PROJECT_ID = 'hardy-unison-369003'
LOCATION = 'us-west4'
ENDPOINT_ID = '877990821104713728'

TRANSACTIONS_TOPIC = 'new_transactions'

OPERATIONS_DATASET = 'fd_operations'
NEW_TRANSACTIONS_TABLE = 'new_transactions'

FDH_BUCKET = 'gs://hardy-unison-369003-fraud-detection-handbook'
NORMAL_TXNS_TABLE = 'normal_transactions'
FRAUD_NOTIFICATION_TOPIC = 'fraudulent_transactions'
ERROR_NOTIFICATION_TOPIC = 'error_notifications'

FEATURES = [
	"TX_AMOUNT",
	"TX_DURING_WEEKEND",
	"TX_DURING_NIGHT",
	"CUSTOMER_ID_NB_TX_1DAY_WINDOW",
	"CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW",
	"CUSTOMER_ID_NB_TX_7DAY_WINDOW",
	"CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW",
	"CUSTOMER_ID_NB_TX_30DAY_WINDOW",
	"CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW",
	"TERMINAL_ID_NB_TX_1DAY_WINDOW",
	"TERMINAL_ID_RISK_1DAY_WINDOW",
	"TERMINAL_ID_NB_TX_7DAY_WINDOW",
	"TERMINAL_ID_RISK_7DAY_WINDOW",
	"TERMINAL_ID_NB_TX_30DAY_WINDOW",
	"TERMINAL_ID_RISK_30DAY_WINDOW"
]

class PredictFraud(beam.DoFn):

    TAG_FRAUDULENT_TRANSACTIONS = 'tag_fraudulent_transactions'
    TAG_NORMAL_TRANSACTIONS = 'tag_normal_transactions'
    TAG_ENDPOINT_ERRORS = 'tag_endpoint_errors'

    def __init__(self, project_id, location, endpoint_id, feature_names):
        self.location = location
        self.project_id = project_id
        self.endpoint_id = endpoint_id
        self.feature_names = feature_names

    def start_bundle(self):
        from google.cloud import aiplatform 
        aiplatform.init(project=self.project_id, location=self.location)
        self.endpoint = aiplatform.Endpoint(endpoint_name=self.endpoint_id)
        
    def process(self, elem):
        instance = [elem[feature] for feature in self.feature_names]
        prediction = self.endpoint.predict(instances=[instance])

        # associate elem to different collections based on prediction result
        if not prediction or len(prediction.predictions) == 0:
            yield TaggedOutput(self.TAG_ENDPOINT_ERRORS, elem)
        prediction_result = prediction.predictions[0]
        if prediction_result[0] >= 0.5:
            yield TaggedOutput(self.TAG_FRAUDULENT_TRANSACTIONS, elem)
        else:
            yield TaggedOutput(self.TAG_NORMAL_TRANSACTIONS, elem)


def run(argv=None, save_main_session=True):

    dataflow_options = PipelineOptions(
        runner='DataflowRunner',
        streaming=True,
        project=PROJECT_ID,
        region=LOCATION,
        job_name='ml-inference',
        temp_location=f'{FDH_BUCKET}/temp',
    )
    # local_options = PipelineOptions(
    #     runner='DirectRunner',
    #     streaming=True,
    #     project=PROJECT_ID,
    #     region=LOCATION,
    #     job_name='ml-inference',
    # )

    transactions_topic = f"projects/{PROJECT_ID}/topics/{TRANSACTIONS_TOPIC}"
    fraud_notification_topic = f"projects/{PROJECT_ID}/topics/{FRAUD_NOTIFICATION_TOPIC}"
    error_notification_topic = f"projects/{PROJECT_ID}/topics/{ERROR_NOTIFICATION_TOPIC}"

    dataflow_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=dataflow_options) as pipeline:
    # with beam.Pipeline(options=local_options) as pipeline:
        ml_invocation = (
            pipeline
            | "Read Transactions" >> beam.io.ReadFromPubSub(topic=transactions_topic)
            | "Convert to Json" >> beam.Map(json.loads)
            | "Classify Transactions" >> beam.ParDo(
                    PredictFraud(project_id=PROJECT_ID, location=LOCATION, endpoint_id=ENDPOINT_ID, feature_names=FEATURES)
                ).with_outputs(
                    PredictFraud.TAG_ENDPOINT_ERRORS,
                    PredictFraud.TAG_FRAUDULENT_TRANSACTIONS,
                    PredictFraud.TAG_NORMAL_TRANSACTIONS
                )
        )
        fraudulent_txns = ml_invocation.tag_fraudulent_transactions
        normal_txns = ml_invocation.tag_normal_transactions
        errors = ml_invocation.tag_endpoint_errors

        # publish fraudulent transactions to pubsub
        (
            fraudulent_txns
            | "Notify Frauds" >> beam.io.WriteToPubSub(topic=fraud_notification_topic)
        )

        # publish inference errors to pubsub
        (
            errors
            | "Notify Errors" >> beam.io.WriteToPubSub(topic=error_notification_topic)
        )

        # write non-fraudulent transactions to bigquery
        (
            normal_txns
            | "Write Non-fraudulent Transactions to BigQuery" >> beam.io.WriteToBigQuery(
                table=f'{PROJECT_ID}:{OPERATIONS_DATASET}.{NEW_TRANSACTIONS_TABLE}',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method='STREAMING_INSERTS'
            )
        )
        

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()