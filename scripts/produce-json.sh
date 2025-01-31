#!/bin/sh

if [ ! -f opentelemetry-javaagent.jar ]; then
    curl -LO https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.12.0/opentelemetry-javaagent.jar
fi

export OTEL_SERVICE_NAME=demo-producer

export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=none
export OTEL_LOGS_EXPORTER=none

export OTEL_EXPORTER_OTLP_ENDPOINT=http://$(oc get route -nmonitoring tempo-otel-http -ojsonpath='{.spec.host}')

java \
    -javaagent:$(pwd)/opentelemetry-javaagent.jar \
    -cp ./target/sample-kafka-apps-0.0.5-jar-with-dependencies.jar \
    com.ibm.eventautomation.demos.producers.JsonProducer
