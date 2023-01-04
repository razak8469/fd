import json
from google.cloud import bigquery, pubsub_v1
from concurrent import futures

def publish_to_pubsub(**kwargs):
    PROJECT_ID = kwargs.get('PROJECT_ID', None)
    DATASET = kwargs.get('DATASET', None)
    TABLE = kwargs.get('TABLE', None)
    TOPIC = kwargs.get('TOPIC', None)

    query = f"""
        SELECT 
            * EXCEPT (TX_DATETIME),
            UNIX_SECONDS(TX_DATETIME) as TX_DATETIME
        FROM 
            {DATASET}.{TABLE}
        ORDER BY
            TX_DATETIME
    """

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC)
    publish_futures = []

    client = bigquery.Client()
    txns_query = client.query(query)
    for txn in txns_query:
        message = json.dumps(dict(txn)).encode('utf-8')
        future = publisher.publish(topic_path, message)
        publish_futures.append(future)

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

