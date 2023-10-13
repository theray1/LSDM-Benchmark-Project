#!/bin/bash

## En local ->
## pig -x local -

## en dataproc...


## create the cluster
gcloud dataproc clusters create cluster-a35a --enable-component-gateway --region europe-west1 --zone europe-west1-c --master-machine-type n1-standard-4 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 --image-version 2.0-debian10 --project lsdm-project-401907

## copy data
#gsutil cp gs://public_lddm_data/small_page_links.nt gs://myown_bucket/

## copy pig code
gsutil cp dataproc.py gs://kick_the/

## Clean out directory
gsutil rm -rf gs://kick_the/out


## run
## (suppose that out directory is empty !!)
gcloud dataproc jobs submit pig --region europe-west1 --cluster cluster-a35a -f gs://kick_the/dataproc.py

## access results
gsutil cat gs://kick_the/out/pagerank_data_10/part-r-00000

## delete cluster...
gcloud dataproc clusters delete cluster-a35a --region europe-west1

