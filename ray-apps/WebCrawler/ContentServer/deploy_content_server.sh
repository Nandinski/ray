#!/bin/bash

eval $(minikube -p minikube docker-env)
docker build . -t resourceless/content-server

kubectl delete -f deployment.yaml
kubectl create -f deployment.yaml

kubectl delete -f service.yaml
kubectl create -f service.yaml

echo "Content server successfully deployed"