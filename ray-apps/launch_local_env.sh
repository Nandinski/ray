#!/bin/bash

if [ "$1" == "reboot" ]; then
    minikube stop; minikube delete
    minikube start --cpus 6 --memory 6138

    helm -n ray install example-cluster --create-namespace ../deploy/charts/ray
    sleep 120
    kubectl port-forward services/example-cluster-ray-head 10001:10001 -n ray
else
    minikube start --cpus 6 --memory 6138
    kubectl port-forward services/example-cluster-ray-head 10001:10001 -n ray
fi