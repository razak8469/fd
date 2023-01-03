import json
from google.cloud import bigquery, pubsub_v1
from concurrent import futures

def publish_simulated_transactions(**kwargs):
    project_id = kwargs.get('project_id', None)
    dataset = kwargs.get('dataset', None)
    table = kwargs.get('table', None)
    topic = kwargs.get('topic', None)

    query = f"""
        SELECT 
            * EXCEPT (TX_DATETIME),
            UNIX_SECONDS(TX_DATETIME) as TX_DATETIME
        FROM 
            {dataset}.{table}
        ORDER BY
            TX_DATETIME
    """

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic)
    publish_futures = []

    client = bigquery.Client()
    txns_query = client.query(query)
    for txn in txns_query:
        message = json.dumps(dict(txn)).encode('utf-8')
        future = publisher.publish(topic_path, message)
        publish_futures.append(future)

    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

