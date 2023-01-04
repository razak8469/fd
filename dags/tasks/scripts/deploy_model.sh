#!/bin/bash

# extract bigquey trained model to cloud storage
bq extract --destination_format ML_XGBOOST_BOOSTER -m {{ params.PROJECT_ID }}:{{ params.MODEL_DATASET_ID }}.{{ params.MODEL_NAME }} \
    {{ params.MODEL_BUCKET }}/{{ params.MODEL_NAME }}/{{ params.MODEL_VERSION}} &

# upload model to model registry
gcloud ai models upload \
  --region={{ params.REGION }} \
   --model-id={{ params.MODEL_ID }} \
  --display-name={{ params.MODEL_NAME }} \
  --container-image-uri={{ params.IMAGE_URI }} \
  --artifact-uri={{ params.MODEL_BUCKET  }}/{{ params.MODEL_NAME }}/{{ params.MODEL_VERSION }} &

# create end point 
gcloud ai endpoints create --project={{ params.PROJECT_ID }} --region={{ params.REGION }} \
    --endpoint-id={{ params.ENDPOINT_ID }} --display-name={{ params.ENDPOINT_NAME }} &
   
# deploy model to endpoint
gcloud ai endpoints deploy-model {{ params.ENDPOINT_ID }} \
    --project={{ params.PROJECT_ID }} \
    --region={{ params.REGION }} \
    --model={{ params.MODEL_ID }} \
    --display-name=f'{{ params.MODEL_NAME }}_{{ params.MODEL_VERSION }}'
