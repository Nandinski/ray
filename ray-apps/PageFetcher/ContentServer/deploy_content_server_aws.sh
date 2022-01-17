#!/bin/bash

docker build . -t resourceless:content-server

aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin 152490780313.dkr.ecr.eu-central-1.amazonaws.com
docker tag resourceless:content-server 152490780313.dkr.ecr.eu-central-1.amazonaws.com/resourceless:content-server
docker push 152490780313.dkr.ecr.eu-central-1.amazonaws.com/resourceless:content-server

kubectl delete -f deployment.yaml
kubectl create -f deployment.yaml

kubectl delete -f service.yaml
kubectl create -f service.yaml

echo "Content server successfully deployed"

