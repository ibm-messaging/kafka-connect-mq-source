# Kafka Connect source connector for IBM MQ
kafka-connect-mq-source is a [Kafka Connect](http://kafka.apache.org/documentation.html#connect) source connector for copying data from IBM MQ into Apache Kafka.

The connector is supplied as source code which you can easily build into a JAR file.


## Building the connector
To build the connector, you must have the following installed:
* [git](https://git-scm.com/)
* [Maven](https://maven.apache.org)
* Java 8 or later

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

Once built, the output is a single JAR called `target/kafka-connect-mq-source-1.0-SNAPSHOT-jar-with-dependencies.jar` which contains all of the required dependencies.


## Running the connector
To run the connector, you must have:
* The JAR from building the connector
* A properties file containing the configuration for the connector
* Apache Kafka 1.0 or later, either standalone or included as part of an offering such as IBM Event Streams
* IBM MQ v8 or later, or the IBM MQ on Cloud service

The connector can be run in a Kafka Connect worker in either standalone (single process) or distributed mode. It's a good idea to start in standalone mode.

You need two configuration files, one for the configuration that applies to all of the connectors such as the Kafka bootstrap servers, and another for the configuration specific to the MQ source connector such as the connection information for your queue manager. For the former, the Kafka distribution includes a file called `connect-standalone.properties` that you can use as a starting point. For the latter, you can use `config/mq-source.properties` in this repository.

The connector connects to MQ using a client connection. You must provide the name of the queue manager, the connection name (one or more host/port pairs) and the channel name. In addition, you can provide a user name and password if the queue manager is configured to require them for client connections. If you look at the supplied `config/mq-sink.properties`, you'll see how to specify the configuration required.

To run the connector in standalone mode from the directory into which you installed Apache Kafka, you use a command like this:

``` shell
bin/connect-standalone.sh connect-standalone.properties mq-source.properties
```


## Data formats
Kafka Connect is very flexible but it's important to understand the way that it processes messages to end up with a reliable system. When the connector encounters a message that it cannot process, it stops rather than throwing the message away. Therefore, you need to make sure that the configuration you use can handle the messages the connector will process.

This is rather complicated and it's likely that a future update of the connector will simplify matters.

Each message in Kafka Connect is associated with a representation of the message format known as a *schema*. Each Kafka message actually has two parts, key and value, and each part has its own schema. The MQ source connector does not currently make much use of message keys, but some of the configuration options use the word *Value* because they refer to the Kafka message value.

When the MQ source connector reads a message from MQ, it chooses a schema to represent the message format and creates an internal object called a *record* containing the message value. This conversion is performed using a *record builder*.  Each record is then processed using a *converter* which creates the message that's published on a Kafka topic.

There are two record builders supplied with the connector, although you can write your own. The basic rule is that if you just want the message to be passed along to Kafka unchanged, the default record builder is probably the best choice. If the incoming data is in JSON format and you want to use a schema based on its structure, use the JSON record builder.

There are three converters built into Apache Kafka. You need to make sure that the incoming message format, the setting of the *mq.message.body.jms* configuration, the record builder and converter are all compatible. By default, everything is just treated as bytes but if you want the connector to understand the message format and apply more sophisticated processing such as single-message transforms, you'll need a more complex configuration. The following table shows the basic options that work.

| Record builder class                                                | Incoming MQ message    | mq.message.body.jms | Converter class                                        | Outgoing Kafka message  |
| ------------------------------------------------------------------- | ---------------------- | ------------------- | ------------------------------------------------------ | ----------------------- |
| com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder | Any                    | false (default)     | org.apache.kafka.connect.converters.ByteArrayConverter | **Binary data**         |
| com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder | JMS BytesMessage       | true                | org.apache.kafka.connect.converters.ByteArrayConverter | **Binary data**         |
| com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder | JMS TextMessage        | true                | org.apache.kafka.connect.storage.StringConverter       | **String data**         |
| com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder    | JSON, may have schema  | Not used            | org.apache.kafka.connect.json.JsonConverter            | **JSON, no schema**     |

There's no single configuration that will always be right, but here are some high-level suggestions.

* Pass unchanged binary (or string) data as the Kafka message value
```
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
```
* Message format is MQSTR, pass string data as the Kafka message value
```
mq.message.body.jms=true
value.converter=org.apache.kafka.connect.converters.StringConverter
```
* Messages are JMS BytesMessage, pass byte array as the Kafka message value
```
mq.message.body.jms=true
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
```
* Messages are JMS TextMessage, pass string data as the Kafka message value
```
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
By default, the connector does not use keys for the Kafka messages it publishes. It can be configured to use the JMS message headers to set the key of the Kafka records. You could use this, for example, to use the MQMD correlation identifier as the partitioning key when the messages are published to Kafka. There are three valid values for the `mq.record.builder.key.header` that controls this behavior.

| mq.record.builder.key.header | Key schema      | Key class | Recommended value for key.converter                    |
| ---------------------------- |---------------- | --------- | ------------------------------------------------------ |
| JMSMessageID                 | OPTIONAL_STRING | String    | org.apache.kafka.connect.storage.StringConverter       |
| JMSCorrelationID             | OPTIONAL_STRING | String    | org.apache.kafka.connect.storage.StringConverter       |
| JMSCorrelationIDAsBytes      | OPTIONAL_BYTES  | byte[]    | org.apache.kafka.connect.converters.ByteArrayConverter |

In MQ, the message ID and correlation ID are both 24-byte arrays. As strings, the connector represents them using a sequence of 48 hexadecimal characters.


## Security
The connector supports authentication with user name and password and also connections secured with TLS using a server-side certificate and mutual authentication with client-side certificates.

### Setting up MQ connectivity using TLS with a server-side certificate
To enable use of TLS, set the configuration `mq.ssl.cipher.suite` to the name of the cipher suite which matches the CipherSpec in the SSLCIPH attribute of the MQ server-connection channel. Use the table of supported cipher suites for MQ 9.1 [here] ((https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.1.0/com.ibm.mq.dev.doc/q113220_.htm) as a reference. Note that the names of the CipherSpecs as used in the MQ configuration are not necessarily the same as the cipher suite names that the connector uses. The connector uses the JMS interface so it follows the Java conventions.

You will need to put the public part of the queue manager's certificate in the JSSE truststore used by the Kafka Connect worker that you're using to run the connector. If you need to specify extra arguments to the worker's JVM, you can use the EXTRA_ARGS environment variable.

### Setting up MQ connectivity using TLS for mutual authentication
You will need to put the public part of the client's certificate in the queue manager's key repository. You will also need to configure the worker's JVM with the location and password for the keystore containing the client's certificate.

### Troubleshooting
For troubleshooting, or to better understand the handshake performed by the IBM MQ Java client application in combination with your specific JSSE provider, you can enable debugging by setting `javax.net.debug=ssl` in the JVM environment.


## Performance and syncpoint limit
The connector uses a transacted JMS session to receive messages from MQ in syncpoint and periodically commits the in-flight transaction. This has the effect of batching messages together for improved efficiency. However, the frequency of committing transactions is controlled by the Kafka Connect framework rather than the connector. The connector is only able to receive up to the queue manager's maximum uncommitted message limit (typically 10000 messages) before committing.

By default, Kafka Connect only commits every 60 seconds (10 seconds for the standalone worker), meaning that each task is limited to a rate of about 166 messages per second. You can increase the frequency of committing by using the `offset.flush.interval.ms` configuration in the worker configuration file. For example, if you set `offset.flush.interval.ms=5000`, the connector commits every 5 seconds increasing the maximum rate per task to about 2000 messages per second.

If messages are being received faster than they can be committed, the connector prints a message `Uncommitted message limit reached` and sleeps for a short delay. You should use this as an indication to set the `offset.flush.interval.ms` to a lower value, or increase the number of tasks.


## Configuration
The configuration options for the Kafka Connect source connector for IBM MQ are as follows:

| Name                         | Description                                                 | Type    | Default       | Valid values                                            |
| ---------------------------- | ----------------------------------------------------------- | ------- | ------------- | ------------------------------------------------------- |
| mq.queue.manager             | The name of the MQ queue manager                            | string  |               | MQ queue manager name                                   |
| mq.connection.name.list      | List of connection names for queue manager                  | string  |               | host(port)[,host(port),...]                             |
| mq.channel.name              | The name of the server-connection channel                   | string  |               | MQ channel name                                         |
| mq.queue                     | The name of the source MQ queue                             | string  |               | MQ queue name                                           |
| mq.user.name                 | The user name for authenticating with the queue manager     | string  |               | User name                                               |
| mq.password                  | The password for authenticating with the queue manager      | string  |               | Password                                                |
| mq.ccdt.url                  | The URL for the CCDT file containing MQ connection details  | string  |               | URL for obtaining a CCDT file                           |
| mq.record.builder            | The class used to build the Kafka Connect record            | string  |               | Class implementing RecordBuilder                        |
| mq.message.body.jms          | Whether to interpret the message body as a JMS message type | boolean | false         |                                                         |
| mq.record.builder.key.header | The JMS message header to use as the Kafka record key       | string  |               | JMSMessageID, JMSCorrelationID, JMSCorrelationIDAsBytes |
| mq.ssl.cipher.suite          | The name of the cipher suite for TLS (SSL) connection       | string  |               | Blank or valid cipher suite                             |
| mq.ssl.peer.name             | The distinguished name pattern of the TLS (SSL) peer        | string  |               | Blank or DN pattern                                     |
| topic                        | The name of the target Kafka topic                          | string  |               | Topic name                                              |

### Using a CCDT file
Some of the connection details for MQ can be provided in a [CCDT file](https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_9.0.0/com.ibm.mq.con.doc/q016730_.htm) by setting `mq.ccdt.url` in the Kafka Connect source connector configuration file. If using a CCDT file the `mq.connection.name.list` and `mq.channel.name` configuration options are not required.

### Externalizing secrets
[KIP 297](https://cwiki.apache.org/confluence/display/KAFKA/KIP-297%3A+Externalizing+Secrets+for+Connect+Configurations) introduced a mechanism to externalize secrets to be used as configuration for Kafka connectors.

#### Example: externalizing secrets with FileConfigProvider

Given a file `secrets.properties` with the contents:
```
secret-key=password
```

Update the worker configuration file to specify the FileConfigProvider which is included by default:

```
# Additional properties for the worker configuration to enable use of ConfigProviders
# multiple comma-separated provider types can be specified here
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

Update the connector configuration file to reference `secret-key` in the file:

```
mq.password=${file:mq-secret.properties:secret-key}
```

#### Using custom Config Providers

Custom config providers can also be enabled in the worker configuration file:
```
# Additional properties for the worker configuration to enable use of ConfigProviders
# multiple comma-separated provider types can be specified here
config.providers=file,other-provider
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
# Other ConfigProvider implementations might require parameters passed in to configure() as follows:
config.providers.other-provider.param.foo=value1
config.providers.other-provider.param.bar=value2
```

## Troubleshooting

### Unable to connect to Kafka

You may receive an `org.apache.kafka.common.errors.SslAuthenticationException: SSL handshake failed` error when trying to run the MQ Source Connector using SSL to connect to your Kafka cluster. In the case that the error is caused by the following exception: `Caused by: java.security.cert.CertificateException: No subject alternative DNS name matching XXXXX found.`, Java may be replacing the IP address of your cluster with the corresponding hostname in your `/etc/hosts` file. For example, to push Docker images to your ICP cluster, you may add an entry in this file which corresponds to the IP of your cluster e.g. `123.456.78.90    mycluster.icp`. To fix this, you can comment out this line in your `/etc/hosts` file.


## Support
A commercially supported version of this connector is available for customers with a support entitlement for [IBM Event Streams](https://developer.ibm.com/messaging/event-streams/).


## Issues and contributions
For issues relating specifically to this connector, please use the [GitHub issue tracker](https://github.com/ibm-messaging/kafka-connect-mq-source/issues). If you do submit a Pull Request related to this connector, please indicate in the Pull Request that you accept and agree to be bound by the terms of the [IBM Contributor License Agreement](CLA.md).


## License
Copyright 2017, 2018 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    (http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.The project is licensed under the Apache 2 license.