#!/bin/bash

kubectl delete secret dmm-certs
kubectl delete secret dmm-config
kubectl delete secret sense-config
kubectl delete secret rucio-client-config

kubectl create secret generic dmm-certs --from-file=$HOME/private/certs/rucio-sense/cert.pem --from-file=$HOME/private/certs/rucio-sense/key.pem
kubectl create secret generic dmm-config --from-file=$HOME/private/dmm.cfg
kubectl create secret generic rucio-client-config --from-file=$HOME/private/rucio.cfg
kubectl create secret generic sense-config --from-file $HOME/.sense-o-auth.yaml 