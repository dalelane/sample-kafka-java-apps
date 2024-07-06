# Simple Kafka client apps

Sample Java applications for sending and receiving messages to a Kafka topic. These require Java and Maven to build and run, and a Kafka cluster to connect to.

Three variations are included:
- [Text](#text)
- [JSON](#json)
- [Avro](#avro)

## Text

### Sending
- [`TextProducer`](./src/main/java/com/ibm/eventautomation/demos/producers/TextProducer.java)

This sends the contents of each txt file in the [`testdata/text`](./testdata/text/) folder to a Kafka topic - each file contents a separate Kafka message.

It exits once it has sent all of the files in the test data folder.

**To modify what this sends**, create text files in the test data folder. Ensure that your files have a `.txt` file extension.

**To modify the Kafka topic it sends to**, modify the [`producer.properties`](./testdata/producer.properties) properties file.

### Receiving
- [`TextConsumer`](./src/main/java/com/ibm/eventautomation/demos/consumers/TextConsumer.java)

This receives messages from a Kafka topic and prints each to stdout.

It will keep doing this until the app is killed.

**To modify the Kafka topic it receives from**, modify the [`consumer.properties`](./testdata/consumer.properties) properties file.

### Scripts

- **To compile**: [`./scripts/compile.sh`](./scripts/compile.sh)
- **To run**:
    - [`./scripts/produce-text.sh`](./scripts/produce-text.sh)
    - [`./scripts/consume-text.sh`](./scripts/consume-text.sh)



## JSON

### Sending
- [`JsonProducer`](./src/main/java/com/ibm/eventautomation/demos/producers/JsonProducer.java)

This sends the contents of each json file in the [`testdata/json`](./testdata/json//) folder to a Kafka topic - each file contents a separate Kafka message.

It exits once it has sent all of the files in the test data folder.

**To modify what this sends**, create JSON files in the test data folder. Ensure that your files hava a `.json` file extension, and contain a valid JSON object.

**To modify the Kafka topic it sends to**, modify the [`producer.properties`](./testdata/producer.properties) properties file.

### Receiving
- [`JsonConsumer`](./src/main/java/com/ibm/eventautomation/demos/consumers/JsonConsumer.java)

This receives messages from a Kafka topic and prints each to stdout.

It will keep doing this until the app is killed.

**To modify the Kafka topic it receives from**, modify the [`consumer.properties`](./testdata/consumer.properties) properties file.

### Scripts

- **To compile**: [`./scripts/compile.sh`](./scripts/compile.sh)
- **To run**:
    - [`./scripts/produce-json.sh`](./scripts/produce-json.sh)
    - [`./scripts/consume-json.sh`](./scripts/consume-json.sh)

## Avro

### Sending

- [`AvroProducer`](./src/main/java/com/ibm/eventautomation/demos/producers/AvroProducer.java)

This sends hard-coded messages to a Kafka topic, using the Avro schema [`testdata/avro/schema.avsc`](./testdata/avro/schema.avsc).

It exits once it has sent all of the messages in the app.

**To modify what this sends**, modify [the Java source code](./src/main/java/com/ibm/eventautomation/demos/producers/AvroProducer.java#L60-L87). To change the message format, you will also need to modify the [the Avro schema](./testdata/avro/schema.avsc) to match.

**To modify the Kafka topic it sends to**, modify the [`producer.properties`](./testdata/producer.properties) properties file.

### Receiving

- [`AvroConsumer`](./src/main/java/com/ibm/eventautomation/demos/consumers/AvroConsumer.java)

This receives messages from a Kafka topic, deserializes them using the Avro schema [`testdata/avro/schema.avsc`](./testdata/avro/schema.avsc), and prints properties from each to stdout.

It will keep doing this until the app is killed.

**To modify what it prints**, modify [the Java source code](./src/main/java/com/ibm/eventautomation/demos/consumers/AvroConsumer.java#L60-L64).

**To modify the Kafka topic it receives from**, modify the [`consumer.properties`](./testdata/consumer.properties) properties file.

### Scripts

- **To compile**: [`./scripts/compile.sh`](./scripts/compile.sh)
- **To run**:
    - [`./scripts/produce-avro.sh`](./scripts/produce-avro.sh)
    - [`./scripts/consume-avro.sh`](./scripts/consume-avro.sh)
