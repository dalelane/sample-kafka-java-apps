FROM --platform=linux/amd64 icr.io/cpopen/ibm-eventautomation-flink/ibm-eventautomation-flink:1.2.4

COPY opentelemetry-javaagent.jar /opt/flink/lib/
COPY target/dependencies/*.jar   /opt/flink/lib/
