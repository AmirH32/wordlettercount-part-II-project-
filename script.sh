#!/bin/bash
MASTER_URL=${1:-"k8s://https://128.232.80.18:6443"}
NAMESPACE=${2:-"cc-group8"}
SERVICE_ACCOUNT=${3:-"spark-cc-group8"}
IMAGE=${4:-"andylamp/spark:v3.5.4-amd64"}
PVC_NAME=${5:-"nfs-cc-group8"}
MOUNT_PATH=${6:-"/test-data"}
EXECUTOR_INSTANCES=${7:-1}

SPARK_CMD="./spark-3.5.4-bin-hadoop3/bin/spark-submit \
--master ${MASTER_URL} \
--deploy-mode cluster \
--name word-letter-counter \
--class com.ccgroup8.spark.WordLetterCount \
--conf spark.executor.instances=${EXECUTOR_INSTANCES} \
--conf spark.kubernetes.namespace=${NAMESPACE} \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=${SERVICE_ACCOUNT} \
--conf spark.kubernetes.container.image=${IMAGE} \
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.${PVC_NAME}.mount.path=${MOUNT_PATH} \
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.${PVC_NAME}.mount.readOnly=false \
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.${PVC_NAME}.options.claimName=${PVC_NAME} \
--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.${PVC_NAME}.mount.path=${MOUNT_PATH} \
--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.${PVC_NAME}.mount.readOnly=false \
--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.${PVC_NAME}.options.claimName=${PVC_NAME} \
local:///test-data/WordLetterCount-1.0.jar $@"

echo "Executing command:"
echo "${SPARK_CMD}"

eval ${SPARK_CMD}
