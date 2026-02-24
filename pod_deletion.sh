#!/bin/bash

kubectl get pods --no-headers | grep -v "group-8-ubuntu-volume" | awk '{print $1}' | xargs kubectl delete pod
