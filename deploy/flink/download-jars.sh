#!/bin/sh

echo "Downloading OpenTelemetry java agent"
curl -LO https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.12.0/opentelemetry-javaagent.jar

echo "Downloading OpenTelemetry Kafka interceptor"
mvn dependency:copy-dependencies -DoutputDirectory=target/dependencies
