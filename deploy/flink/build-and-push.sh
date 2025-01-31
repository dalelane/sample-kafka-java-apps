#!/bin/sh

export REGISTRY=`oc get route default-route -n openshift-image-registry --template='{{ .spec.host }}'`
docker login -u `oc whoami` -p `oc whoami --show-token` ${REGISTRY}

docker build -t ${REGISTRY}/event-automation/flink-opentelemetry:1 .
docker push ${REGISTRY}/event-automation/flink-opentelemetry:1
