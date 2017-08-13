# Kafka Connect source connector for IBM MQ
kafka-connect-mqsource is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect) source connector for copying data from IBM MQ into Apache Kafka.

The connector is supplied as source code which you can easily build into a JAR file.


## Building the connector
To build the connector, you must have the following installed:
* [git](https://git-scm.com/)
* [Maven](https://maven.apache.org)
* Java 7 or later

Download the MQ client JAR by following the instructions in [Getting the IBM MQ classes for Java and JMS](https://www-01.ibm.com/support/docview.wss?uid=swg21683398). Once you've accepted the license, download the *IBM MQ JMS and Java redistributable client* file (currently called `9.0.0.1-IBM-MQC-Redist-Java.zip`). Unpack the ZIP file.

Clone the repository with the following command:
```shell
git clone https://github.com/ibm-messaging/kafka-connect-mq-source.git
```

Change directory into the `kafka-connect-mq-source` directory:
```shell
cd kafka-connect-mq-source
```

Copy the JAR file `com.ibm.mq.allclient.jar` that you unpacked from the ZIP file earlier into the `kafka-connect-mq-source` directory.

Run the following command to create a local Maven repository containing just this file so that it can be used to build the connector:
```shell
mvn deploy:deploy-file -Durl=file://local-maven-repo -Dfile=com.ibm.mq.allclient.jar -DgroupId=com.ibm.mq -DartifactId=allclient -Dpackaging=jar -Dversion=9.0.0.1
```

Build the connector using Maven:
```shell
mvn clean package
```

Once built, the output is a single JAR called `target/kafka-connect-mq-source-0.1-SNAPSHOT-jar-with-dependencies.jar` which contains all of the required dependencies.


## Running the connector
To run the connector, you must have:
* The JAR from building the connector
* A properties file containing the configuration for the connector
* Apache Kafka
* IBM MQ v8.0 or later

The connector can be run in a Kafka Connect worker in either standalone (single process) or distributed mode. It's a good idea to start in standalone mode.

You need two configuration files, one for the configuration that applies to all of the connectors such as the Kafka bootstrap servers, and another for the configuration specific to the MQ source connector such as the connection information for your queue manager. For the former, the Kafka distribution includes a file called `connect-standalone.properties` that you can use as a starting point. For the latter, you can use `config/mq-source.properties` in this repository.

The connector connects to MQ using a client connection. You must provide the name of the queue manager, the connection name (one or more host/port pairs) and the channel name. In addition, you can provide a user name and password if the queue manager is configured to require them for client connections. If you look at the supplied `config/mq-sink.properties`, you'll see how to specify the configuration required.

To run the connector in standalone mode from the directory into which you installed Apache Kafka, you use a command like this:

``` shell
bin/connect-standalone.sh connect-standalone.properties mq-source.properties
```


## Data formats
Kafka Connect is very flexible but it's important to understand the way that it processes messages to end up with a reliable system. When the connector encounters a message that it cannot process, it stops rather than throwing the message away. Therefore, you need to make sure that the configuration you use can handle the messages the connector will process.

Each message in Kafka Connect is associated with a representation of the message format known as a *schema*. Each Kafka message actually has two parts, key and value, and each part has its own schema. The MQ source connector does not currently use message keys, but some of the configuration options use the word *Value* because they refer to the Kafka message value.

When the MQ source connector reads a message from MQ, it chooses a schema to represent the message format and creates a Java object containing the message value. Each message is then processed using a *converter* which creates the message that's published on a Kafka topic. You need to choose a converter appropriate to the format of messages that will pass through the connector.

There's no single configuration that will always be right, but here are some high-level suggestions.

* Pass unchanged binary data as the Kafka message value
```
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
```
* Messages are JMS BytesMessage, pass byte array as the Kafka message value
```
mq.message.body.jms=true
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
```
* Messages are JMS TextMessage, pass string as the Kafka message value
```
mq.message.body.jms=true
value.converter=org.apache.kafka.connect.storage.StringConverter
```

### The gory detail
The MQ source connector has a configuration option *mq.message.body.jms* that controls whether it interprets the MQ messages as JMS messages or regular MQ messages. By default, *mq.message.body.jms=false* which gives the following behaviour.

| Incoming message format | Value schema   | Value class |
| ----------------------- | -------------- | ----------- |
| Any                     | OPTIONAL_BYTES | byte[]      |

This means that all messages are treated as arrays of bytes, and the converter must be able to handle arrays of bytes.

When you set *mq.message.body.jms=true*, the MQ messages are interpreted as JMS messages. This is appropriate if the applications sending the messages are themselves using JMS. This gives the following behaviour.

| Incoming message format | Value schema | Value class      |
| ----------------------- | ------------ | ---------------- |
| JMS BytesMessage        | null         | byte[]           |
| JMS TextMessage         | null         | java.lang.String |
| Anything else           | *EXCEPTION*  | *EXCEPTION*      |

There are three basic converters built into Apache Kafka, with the likely useful combinations in **bold**.

| Converter class                                        | byte[]              | java.lang.String |
| ------------------------------------------------------ | ------------------- | ---------------- |
| org.apache.kafka.connect.converters.ByteArrayConverter | **Binary data**     | *EXCEPTION*      |
| org.apache.kafka.connect.storage.StringConverter       | Works, not useful   | **String data**  |
| org.apache.kafka.connect.json.JsonConverter            | Base-64 JSON String | JSON String      |

In addition, there is another converter for the Avro format that is part of the Confluent Platform. This has not been tested with the MQ source connector at this time.


## Security
The connector supports authentication with user name and password and also connections secured with TLS using a server-side certificate. It does not currently support TLS mutual authentication with client-side certificates.

### Setting up TLS using a server-side certificate
To enable use of TLS, set the configuration 'mq.ssl.cipher.suite' to the name of the cipher suite which matches the CipherSpec in the SSLCIPH attribute of the MQ server-connection channel. Use the table of supported cipher suites for MQ 9.0.x [here](https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.0.0/com.ibm.mq.dev.doc/q113220_.htm) as a reference. Note that the names of the CipherSpecs as used in the MQ configuration are not necessarily the same as the cipher suite names that the connector uses. The connector uses the JMS interface so it follows the Java conventions.

You will need to put the public part of the queue manager's certificate in a JSSE truststore and then provide the location of the truststore as a property to the Kafka Connect worker that you're using to run the connector.


## Configuration
The configuration options for the MQ Source Connector are as follows:

| Name                    | Description                                                 | Type    | Default       | Valid values                |
| ----------------------- | ----------------------------------------------------------- | ------- | ------------- | --------------------------- |
| mq.queue.manager        | The name of the MQ queue manager                            | string  |               | MQ queue manager name       |
| mq.connection.name.list | List of connection names for queue manager                  | string  |               | host(port)[,host(port),...] |
| mq.channel.name         | The name of the server-connection channel                   | string  |               | MQ channel name             |
| mq.queue                | The name of the source MQ queue                             | string  |               | MQ queue name               |
| mq.user.name            | The user name for authenticating with the queue manager     | string  |               | User name                   |
| mq.password             | The password for authenticating with the queue manager      | string  |               | Password                    |
| mq.message.body.jms     | Whether to interpret the message body as a JMS message type | boolean | false         |                             |
| mq.ssl.cipher.suite     | The name of the cipher suite for TLS connection             | string  |               | Blank or valid cipher suite |
| topic                   | The name of the target Kafka topic                          | string  |               | Topic name                  |


## Future enhancements
The connector is intentionally basic. The idea is to enhance it over time with additional features to make it more capable. Some possible future enhancements are:
* TLS mutual authentication
* Message key support
* Configurable schema for MQ messages
* JMX metrics
* JSON parsing so that the JSON type information is supplied to the converter
* Testing with the Confluent Platform Avro converter and Schema Registry


## Issues and contributions
For issues relating specifically to this connect, please use the [GitHub issue tracker](https://github.com/ibm-messaging/kafka-connect-mq-source/issues). If you do submit a Pull Request related to this connector, please indicate in the Pull Request that you accept and agree to be bound by the terms of the [IBM Contributor License Agreement](CLA.md).


## License
Copyright 2017 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    (http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.The project is licensed under the Apache 2 license.