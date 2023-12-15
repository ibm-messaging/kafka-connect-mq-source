# Kafka Connect source connector for IBM MQ

kafka-connect-mq-source is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect) source connector for copying data from IBM MQ into Apache Kafka.

The connector is supplied as source code which you can easily build into a JAR file.

**Note**: A sink connector for IBM MQ is also available on [GitHub](https://github.com/ibm-messaging/kafka-connect-mq-sink).

## Contents

- [Building the connector](#building-the-connector)
- [Running the connector](#running-the-connector)
- [Running the connector with Docker](#running-with-docker)
- [Deploying the connector to Kubernetes](#deploying-to-kubernetes)
- [Data formats](#data-formats)
- [Security](#security)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Support](#support)
- [Issues and contributions](#issues-and-contributions)
- [License](#license)

## Building the connector

To build the connector, you must have the following installed:

- [git](https://git-scm.com/)
- [Maven 3.0 or later](https://maven.apache.org)
- Java 8 or later

Clone the repository with the following command:

```shell
git clone https://github.com/ibm-messaging/kafka-connect-mq-source.git
```

Change directory into the `kafka-connect-mq-source` directory:

```shell
cd kafka-connect-mq-source
```

Build the connector using Maven:

```shell
mvn clean package
```

Once built, the output is a single JAR called `target/kafka-connect-mq-source-<VERSION>-jar-with-dependencies.jar` which contains all of the required dependencies.

## Running the connector

For step-by-step instructions, see the following guides for running the connector:

- connecting to Apache Kafka [running locally](UsingMQwithKafkaConnect.md)
- connecting to an installation of [IBM Event Streams](https://ibm.github.io/event-streams/connecting/mq/source)

To run the connector, you must have:

- The JAR from building the connector
- A properties file containing the configuration for the connector
- Apache Kafka 2.6.2 or later, either standalone or included as part of an offering such as IBM Event Streams
- IBM MQ v9 or later, or the IBM MQ on Cloud service

The connector can be run in a Kafka Connect worker in either standalone (single process) or distributed mode. It's a good idea to start in standalone mode.

### Running in standalone mode

You need two configuration files, one for the configuration that applies to all of the connectors such as the Kafka bootstrap servers, and another for the configuration specific to the MQ source connector such as the connection information for your queue manager. For the former, the Kafka distribution includes a file called `connect-standalone.properties` that you can use as a starting point. For the latter, you can use `config/mq-source.properties` in this repository.

The connector connects to MQ using either a client or a bindings connection. For a client connection, you must provide the name of the queue manager, the connection name (one or more host/port pairs) and the channel name. In addition, you can provide a user name and password if the queue manager is configured to require them for client connections. If you look at the supplied `config/mq-source.properties`, you'll see how to specify the configuration required. For a bindings connection, you must provide provide the name of the queue manager and also run the Kafka Connect worker on the same system as the queue manager.

To run the connector in standalone mode from the directory into which you installed Apache Kafka, you use a command like this:

``` shell
bin/connect-standalone.sh connect-standalone.properties mq-source.properties
```

### Running in distributed mode

You need an instance of Kafka Connect running in distributed mode. The Kafka distribution includes a file called `connect-distributed.properties` that you can use as a starting point, or follow [Running with Docker](#running-with-docker) or [Deploying to Kubernetes](#deploying-to-kubernetes)

To start the MQ connector, you can use `config/mq-source.json` in this repository after replacing all placeholders and use a command like this:

``` shell
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data "@./config/mq-source.json"
```

## Running with Docker

This repository includes an example Dockerfile to run Kafka Connect in distributed mode. It also adds in the MQ source connector as an available connector plugin. It uses the default `connect-distributed.properties` and `connect-log4j.properties` files.

1. `mvn clean package`
1. `docker build -t kafkaconnect-with-mq-source:<TAG> .`
1. `docker run -p 8083:8083 kafkaconnect-with-mq-source:<TAG>`

Substitute `<TAG>` with the version of the connector or `latest` to use the latest version.

**NOTE:** To provide custom properties files create a folder called `config` containing the `connect-distributed.properties` and `connect-log4j.properties` files and use a Docker volume to make them available when running the container like this:

``` shell
docker run -v $(pwd)/config:/opt/kafka/config -p 8083:8083 kafkaconnect-with-mq-source:<TAG>
```

To start the MQ connector, you can use `config/mq-source.json` in this repository after replacing all placeholders and use a command like this:

``` shell
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data "@./config/mq-source.json"
```

## Deploying to Kubernetes

This repository includes a Kubernetes yaml file called `kafka-connect.yaml`. This will create a deployment to run Kafka Connect in distributed mode and a service to access the deployment.

The deployment assumes the existence of a Secret called `connect-distributed-config` and a ConfigMap called `connect-log4j-config`. These can be created using the default files in your Kafka install, however it is easier to edit them later if comments and whitespaces are trimmed before creation.

### Creating Kafka Connect configuration Secret and ConfigMap

Create Secret for Kafka Connect configuration:

1. `cp kafka/config/connect-distributed.properties connect-distributed.properties.orig`
1. `sed '/^#/d;/^[[:space:]]*$/d' < connect-distributed.properties.orig > connect-distributed.properties`
1. `kubectl -n <namespace> create secret generic connect-distributed-config --from-file=connect-distributed.properties`

Create ConfigMap for Kafka Connect Log4j configuration:

1. `cp kafka/config/connect-log4j.properties connect-log4j.properties.orig`
1. `sed '/^#/d;/^[[:space:]]*$/d' < connect-log4j.properties.orig > connect-log4j.properties`
1. `kubectl -n <namespace> create configmap connect-log4j-config --from-file=connect-log4j.properties`

### Creating Kafka Connect deployment and service in Kubernetes

**NOTE:** You will need to [build the Docker image](#running-with-docker) and push it to your Kubernetes image repository. Remember that the supplied Dockerfile is just an example and you will have to modify it for your needs. You might need to update the image name in the `kafka-connect.yaml` file.

1. Update the namespace in `kafka-connect.yaml`
1. `kubectl -n <namespace> apply -f kafka-connect.yaml`
1. `curl <serviceIP>:<servicePort>/connector-plugins` to see whether the MQ source connector is available to use

### Deploying to OpenShift using Strimzi

This repository includes a Kubernetes yaml file called `strimzi.kafkaconnector.yaml` for use with the [Strimzi](https://strimzi.io) operator. Strimzi provides a simplified way of running the Kafka Connect distributed worker, by defining either a KafkaConnect resource or a KafkaConnectS2I resource.

The KafkaConnectS2I resource provides a nice way to have OpenShift do all the work of building the Docker images for you. This works particularly nicely combined with the KafkaConnector resource that represents an individual connector.

The following instructions assume you are running on OpenShift and have Strimzi 0.16 or later installed.

#### Start a Kafka Connect cluster using KafkaConnectS2I

1. Create a file called `kafka-connect-s2i.yaml` containing the definition of a KafkaConnectS2I resource. You can use the examples in the Strimzi project to get started.
1. Configure it with the information it needs to connect to your Kafka cluster. You must include the annotation `strimzi.io/use-connector-resources: "true"` to configure it to use KafkaConnector resources so you can avoid needing to call the Kafka Connect REST API directly.
1. `oc apply -f kafka-connect-s2i.yaml` to create the cluster, which usually takes several minutes.

#### Add the MQ source connector to the cluster

1. `mvn clean package` to build the connector JAR.
1. `mkdir my-plugins`
1. `cp target/kafka-connect-mq-source-*-jar-with-dependencies.jar my-plugins`
1. `oc start-build <kafkaconnectClusterName>-connect --from-dir ./my-plugins` to add the MQ source connector to the Kafka Connect distributed worker cluster. Wait for the build to complete, which usually takes a few minutes.
1. `oc describe kafkaconnects2i <kafkaConnectClusterName>` to check that the MQ source connector is in the list of available connector plugins.

#### Start an instance of the MQ source connector using KafkaConnector

1. `cp deploy/strimzi.kafkaconnector.yaml kafkaconnector.yaml`
1. Update the `kafkaconnector.yaml` file to replace all of the values in `<>`, adding any additional configuration properties.
1. `oc apply -f kafkaconnector.yaml` to start the connector.
1. `oc get kafkaconnector` to list the connectors. You can use `oc describe` to get more details on the connector, such as its status.

## Data formats

Kafka Connect is very flexible but it's important to understand the way that it processes messages to end up with a reliable system. When the connector encounters a message that it cannot process, it stops rather than throwing the message away. Therefore, you need to make sure that the configuration you use can handle the messages the connector will process.

This is rather complicated and it's likely that a future update of the connector will simplify matters.

Each message in Kafka Connect is associated with a representation of the message format known as a *schema*. Each Kafka message actually has two parts, key and value, and each part has its own schema. The MQ source connector does not currently make much use of message keys, but some of the configuration options use the word *Value* because they refer to the Kafka message value.

When the MQ source connector reads a message from MQ, it chooses a schema to represent the message format and creates an internal object called a *record* containing the message value. This conversion is performed using a *record builder*.  Each record is then processed using a *converter* which creates the message that's published on a Kafka topic.

There are two record builders supplied with the connector, although you can write your own. The basic rule is that if you just want the message to be passed along to Kafka unchanged, the default record builder is probably the best choice. If the incoming data is in JSON format and you want to use a schema based on its structure, use the JSON record builder.

There are three converters built into Apache Kafka. You need to make sure that the incoming message format, the setting of the `mq.message.body.jms` configuration, the record builder and converter are all compatible. By default, everything is just treated as bytes but if you want the connector to understand the message format and apply more sophisticated processing such as single-message transforms, you'll need a more complex configuration. The following table shows the basic options that work.

| Record builder class                                                | Incoming MQ message    | mq.message.body.jms | Converter class                                        | Outgoing Kafka message  |
| ------------------------------------------------------------------- | ---------------------- | ------------------- | ------------------------------------------------------ | ----------------------- |
| com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder | Any                    | false (default)     | org.apache.kafka.connect.converters.ByteArrayConverter | **Binary data**         |
| com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder | JMS BytesMessage       | true                | org.apache.kafka.connect.converters.ByteArrayConverter | **Binary data**         |
| com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder | JMS TextMessage        | true                | org.apache.kafka.connect.storage.StringConverter       | **String data**         |
| com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder    | JSON, may have schema  | Not used            | org.apache.kafka.connect.json.JsonConverter            | **JSON, no schema**     |

There's no single configuration that will always be right, but here are some high-level suggestions.

- Pass unchanged binary (or string) data as the Kafka message value

```java
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
```

- Message format is MQSTR, pass string data as the Kafka message value

```java
mq.message.body.jms=true
value.converter=org.apache.kafka.connect.storage.StringConverter
```

- Messages are JMS BytesMessage, pass byte array as the Kafka message value

```java
mq.message.body.jms=true
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
```

- Messages are JMS TextMessage, pass string data as the Kafka message value

```java
mq.message.body.jms=true
value.converter=org.apache.kafka.connect.storage.StringConverter
```

### The gory detail

The messages received from MQ are processed by a record builder which builds a Kafka Connect record to represent the message. There are two record builders supplied with the MQ source connector. The connector has a configuration option *mq.message.body.jms* that controls whether it interprets the MQ messages as JMS messages or regular MQ messages.

| Record builder class                                                | mq.message.body.jms | Incoming message body | Value schema       | Value class        |
| ------------------------------------------------------------------- | ------------------- | --------------------- | ------------------ | ------------------ |
| com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder | false (default)     | Any                   | OPTIONAL_BYTES     | byte[]             |
| com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder | true                | JMS BytesMessage      | null               | byte[]             |
| com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder | true                | JMS TextMessage       | null               | String             |
| com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder | true                | Everything else       | *EXCEPTION*        | *EXCEPTION*        |
| com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder    | Not used            | JSON                  | Depends on message | Depends on message |

You must then choose a converter than can handle the value schema and class. There are three basic converters built into Apache Kafka, with the likely useful combinations in **bold**.

| Converter class                                        | Output for byte[]   | Output for String | Output for compound schema |
| ------------------------------------------------------ | ------------------- | ----------------- | -------------------------- |
| org.apache.kafka.connect.converters.ByteArrayConverter | **Binary data**     | *EXCEPTION*       | *EXCEPTION*                |
| org.apache.kafka.connect.storage.StringConverter       | Works, not useful   | **String data**   | Works, not useful          |
| org.apache.kafka.connect.json.JsonConverter            | Base-64 JSON String | JSON String       | **JSON data**              |

### Key support and partitioning

By default, the connector does not use keys for the Kafka messages it publishes. It can be configured to use the JMS message headers to set the key of the Kafka records. You could use this, for example, to use the MQMD correlation identifier as the partitioning key when the messages are published to Kafka. There are four valid values for the `mq.record.builder.key.header` that controls this behavior.

| mq.record.builder.key.header | Key schema      | Key class | Recommended value for key.converter                    |
| ---------------------------- |---------------- | --------- | ------------------------------------------------------ |
| JMSMessageID                 | OPTIONAL_STRING | String    | org.apache.kafka.connect.storage.StringConverter       |
| JMSCorrelationID             | OPTIONAL_STRING | String    | org.apache.kafka.connect.storage.StringConverter       |
| JMSCorrelationIDAsBytes      | OPTIONAL_BYTES  | byte[]    | org.apache.kafka.connect.converters.ByteArrayConverter |
| JMSDestination               | OPTIONAL_STRING | String    | org.apache.kafka.connect.storage.StringConverter       |

In MQ, the message ID and correlation ID are both 24-byte arrays. As strings, the connector represents them using a sequence of 48 hexadecimal characters.

### Accessing MQMD fields

If you write your own RecordBuilder, you can access the MQMD fields of the MQ messages as JMS message properties. By default, only a subset of the MQMD fields are available, but you can get access to all of them by setting the configuration `mq.message.mqmd.read`. For more information, see [JMS message object properties](https://www.ibm.com/support/knowledgecenter/SSFKSJ_9.1.0/com.ibm.mq.dev.doc/q032350_.htm) in the MQ documentation.

## Security

The connector supports authentication with user name and password and also connections secured with TLS using a server-side certificate and mutual authentication with client-side certificates. You can also choose whether to use connection security parameters (MQCSP) depending on the security settings you're using in MQ.

### Setting up MQ connectivity using TLS with a server-side certificate

To enable use of TLS, set the configuration `mq.ssl.cipher.suite` to the name of the cipher suite which matches the CipherSpec in the SSLCIPH attribute of the MQ server-connection channel. Use the table of supported cipher suites for MQ 9.1 [here](https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.1.0/com.ibm.mq.dev.doc/q113220_.htm) as a reference. Note that the names of the CipherSpecs as used in the MQ configuration are not necessarily the same as the cipher suite names that the connector uses. The connector uses the JMS interface so it follows the Java conventions.

You will need to put the public part of the queue manager's certificate in the JSSE truststore used by the Kafka Connect worker that you're using to run the connector. If you need to specify extra arguments to the worker's JVM, you can use the EXTRA_ARGS environment variable.

### Setting up MQ connectivity using TLS for mutual authentication

You will need to put the public part of the client's certificate in the queue manager's key repository. You will also need to configure the worker's JVM with the location and password for the keystore containing the client's certificate. Alternatively, you can configure a separate keystore and truststore for the connector.

### Security troubleshooting

For troubleshooting, or to better understand the handshake performed by the IBM MQ Java client application in combination with your specific JSSE provider, you can enable debugging by setting `javax.net.debug=ssl` in the JVM environment.

## Configuration

The configuration options for the Kafka Connect source connector for IBM MQ are as follows:

| Name                                    | Description                                                            | Type    | Default        | Valid values                                            |
| --------------------------------------- | ---------------------------------------------------------------------- | ------- | -------------- | ------------------------------------------------------- |
| topic                                   | The name of the target Kafka topic                                     | string  |                | Topic name                                              |
| mq.queue.manager                        | The name of the MQ queue manager                                       | string  |                | MQ queue manager name                                   |
| mq.connection.mode                      | The connection mode - bindings or client                               | string  | client         | client, bindings                                        |
| mq.connection.name.list                 | List of connection names for queue manager                             | string  |                | host(port)[,host(port),...]                             |
| mq.channel.name                         | The name of the server-connection channel                              | string  |                | MQ channel name                                         |
| mq.queue                                | The name of the source MQ queue                                        | string  |                | MQ queue name                                           |
| mq.user.name                            | The user name for authenticating with the queue manager                | string  |                | User name                                               |
| mq.password                             | The password for authenticating with the queue manager                 | string  |                | Password                                                |
| mq.user.authentication.mqcsp            | Whether to use MQ connection security parameters (MQCSP)               | boolean | true           |                                                         |
| mq.ccdt.url                             | The URL for the CCDT file containing MQ connection details             | string  |                | URL for obtaining a CCDT file                           |
| mq.record.builder                       | The class used to build the Kafka Connect record                       | string  |                | Class implementing RecordBuilder                        |
| mq.message.body.jms                     | Whether to interpret the message body as a JMS message type            | boolean | false          |                                                         |
| mq.record.builder.key.header            | The JMS message header to use as the Kafka record key                  | string  |                | JMSMessageID, JMSCorrelationID, JMSCorrelationIDAsBytes, JMSDestination |
| mq.jms.properties.copy.to.kafka.headers | Whether to copy JMS message properties to Kafka headers                | boolean | false          |                                                         |
| mq.ssl.cipher.suite                     | The name of the cipher suite for TLS (SSL) connection                  | string  |                | Blank or valid cipher suite                             |
| mq.ssl.peer.name                        | The distinguished name pattern of the TLS (SSL) peer                   | string  |                | Blank or DN pattern                                     |
| mq.ssl.keystore.location                | The path to the JKS keystore to use for SSL (TLS) connections          | string  | JVM keystore   | Local path to a JKS file                                |
| mq.ssl.keystore.password                | The password of the JKS keystore to use for SSL (TLS) connections      | string  |                |                                                         |
| mq.ssl.truststore.location              | The path to the JKS truststore to use for SSL (TLS) connections        | string  | JVM truststore | Local path to a JKS file                                |
| mq.ssl.truststore.password              | The password of the JKS truststore to use for SSL (TLS) connections    | string  |                |                                                         |
| mq.ssl.use.ibm.cipher.mappings          | Whether to set system property to control use of IBM cipher mappings   | boolean |                |                                                         |
| mq.batch.size                           | The maximum number of messages in a batch (unit of work)               | integer | 250            | 1 or greater                                            |
| mq.message.mqmd.read                    | Whether to enable reading of all MQMD fields                           | boolean | false          |                                                         |
| mq.ccsid                                | The coded character set identifier to use when for encoding strings    | integer |                | Valid 16-bit CCSID ([IBM CCSID reference information](https://www.ibm.com/docs/en/i/7.5?topic=information-ccsid-reference)) |

### Using a CCDT file

Some of the connection details for MQ can be provided in a [CCDT file](https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.1.0/com.ibm.mq.con.doc/q016730_.htm) by setting `mq.ccdt.url` in the MQ source connector configuration file. If using a CCDT file the `mq.connection.name.list` and `mq.channel.name` configuration options are not required.

### Externalizing secrets

[KIP 297](https://cwiki.apache.org/confluence/display/KAFKA/KIP-297%3A+Externalizing+Secrets+for+Connect+Configurations) introduced a mechanism to externalize secrets to be used as configuration for Kafka connectors.

#### Example: externalizing secrets with FileConfigProvider

Given a file `mq-secrets.properties` with the contents:

```java
secret-key=password
```

Update the worker configuration file to specify the FileConfigProvider which is included by default:

```java
# Additional properties for the worker configuration to enable use of ConfigProviders
# multiple comma-separated provider types can be specified here
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

Update the connector configuration file to reference `secret-key` in the file:

```java
mq.password=${file:mq-secret.properties:secret-key}
```

##### Using FileConfigProvider in Kubernetes

To use a file for the `mq.password` in Kubernetes, you create a Secret using the file as described in [the Kubernetes docs](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod).

## Troubleshooting

### Unable to connect to Kafka

You may receive an `org.apache.kafka.common.errors.SslAuthenticationException: SSL handshake failed` error when trying to run the MQ source connector using SSL to connect to your Kafka cluster. In the case that the error is caused by the following exception: `Caused by: java.security.cert.CertificateException: No subject alternative DNS name matching XXXXX found.`, Java may be replacing the IP address of your cluster with the corresponding hostname in your `/etc/hosts` file. For example, to push Docker images to a custom Docker repository, you may add an entry in this file which corresponds to the IP of your repository e.g. `123.456.78.90    mycluster.icp`. To fix this, you can comment out this line in your `/etc/hosts` file.

### Unsupported cipher suite

When configuring TLS connection to MQ, you may find that the queue manager rejects the cipher suite, in spite of the name looking correct. There are two different naming conventions for cipher suites (<https://www.ibm.com/support/knowledgecenter/SSFKSJ_9.1.0/com.ibm.mq.dev.doc/q113220_.htm>). Setting the configuration option `mq.ssl.use.ibm.cipher.mappings=false` often resolves cipher suite problems.

## Support

Commercial support for this connector is available for customers with a support entitlement for [IBM Event Automation](https://www.ibm.com/products/event-automation) or [IBM Cloud Pak for Integration](https://www.ibm.com/cloud/cloud-pak-for-integration).

## Issues and contributions

For issues relating specifically to this connector, please use the [GitHub issue tracker](https://github.com/ibm-messaging/kafka-connect-mq-source/issues). If you do want to submit a Pull Request related to this connector, please read the [contributing guide](CONTRIBUTING.md) first to understand how to sign your commits.

## License

Copyright 2017, 2020, 2023 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.The project is licensed under the Apache 2 license.
