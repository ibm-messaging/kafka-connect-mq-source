# Using IBM MQ with Kafka Connect
Many organizations use both IBM MQ and Apache Kafka for their messaging needs. Although they're often used to solve different kinds of messaging problems, people often want to connect them together. When connecting Apache Kafka to other systems, the technology of choice is the Kafka Connect framework.

A pair of basic connectors for IBM MQ are available as source code available on GitHub. The source connector (https://github.com/ibm-messaging/kafka-connect-mq-source) is used to take messages from an MQ queue and transfer them to a Kafka topic, while the sink connector (https://github.com/ibm-messaging/kafka-connect-mq-sink) goes the other way. The GitHub projects have instructions for building the connectors, but once you've successfully built the JAR files, what next?

These instructions tell you how to set up MQ and Apache Kafka from scratch and use the connectors to transfer messages between them. The instructions are for MQ v9 running on Linux, so if you're using a different version or platform, you might have to adjust them slightly. The instructions also expect Apache Kafka 0.10.2.0 or later.


## Kafka Connect concepts
The best place to read about Kafka Connect is of course the Apache Kafka [documentation](https://kafka.apache.org/documentation/).

Kafka Connect connectors run inside a Java process called a *worker*. Kafka Connect can run in either standalone or distributed mode. Standalone mode is intended for testing and temporary connections between systems. Distributed mode is more appropriate for production use. These instructions focus on standalone mode because it's easier to see what's going on.

When you run Kafka Connect with a standalone worker, there are two configuration files. The *worker configuration file* contains the properties needed to connect to Kafka. The *connector configuration file* contains the properties needed for the connector. So, configuration to connect to Kafka goes into the worker configuration file, while the MQ configuration goes into the connector configuration file.

It's simplest to run just one connector in each standalone worker. Kafka Connect workers spew out a lot of messages and it's much simpler to read them if the messages from multiple connectors are not interleaved.


## Setting it up from scratch

### Create a queue manager
These instructions set up a queue manager that uses the local operating system to authenticate the user ID and password, and the user ID is called `alice` and the password is `passw0rd`.

It is assumed that you have installed MQ, you're logged in as a user authorized to administer MQ and the MQ commands are on the path.

Create a queue manager with a TCP/IP listener (on port 1414 in this example):
``` shell
crtmqm -p 1414 MYQM
```

Start the queue manager:
``` shell
strmqm MYQM
```

Start the `runmqsc` tool to configure the queue manager:
``` shell
runmqsc MYQM
```

In `runmqsc`, create a server-connection channel:
```
DEFINE CHANNEL(MYSVRCONN) CHLTYPE(SVRCONN)
```

Set the channel authentication rules to accept connections requiring userid and password:
```
SET CHLAUTH(MYSVRCONN) TYPE(BLOCKUSER) USERLIST('nobody')
SET CHLAUTH('*') TYPE(ADDRESSMAP) ADDRESS('*') USERSRC(NOACCESS)
SET CHLAUTH(MYSVRCONN) TYPE(ADDRESSMAP) ADDRESS('*') USERSRC(CHANNEL) CHCKCLNT(REQUIRED)
```

Set the identity of the client connections based on the supplied context, the user ID:
```
ALTER AUTHINFO(SYSTEM.DEFAULT.AUTHINFO.IDPWOS) AUTHTYPE(IDPWOS) ADOPTCTX(YES)
```

And refresh the connection authentication information:
```
REFRESH SECURITY TYPE(CONNAUTH)
```

Create an pair of queues for the Kafka Connect connectors to use:
```
DEFINE QLOCAL(MYQSOURCE)
DEFINE QLOCAL(MYQSINK)
```

Authorize `alice` to connect to and inquire the queue manager:
```
SET AUTHREC OBJTYPE(QMGR) PRINCIPAL('alice') AUTHADD(CONNECT,INQ)
```

And finally authorize `alice` to use the queues:
```
SET AUTHREC PROFILE(MYQSOURCE) OBJTYPE(QUEUE) PRINCIPAL('alice') AUTHADD(ALLMQI)
SET AUTHREC PROFILE(MYQSINK) OBJTYPE(QUEUE) PRINCIPAL('alice') AUTHADD(ALLMQI)
```

End `runmqsc`:
```
END
```

The queue manager is now ready to accept connection from Kafka Connect connectors.


### Download and set up Apache Kafka
If you do not already have Apache Kafka, you can download it from here: http://kafka.apache.org/downloads. Make sure you have the prerequisites installed, such as Java.

Download the latest .tgz file (called something like `kafka_2.11-1.0.0.tgz`) and unpack it. The top-level directory of the unpacked .tgz file is referred to as the *Kafka root directory*. It contains several directories including `bin` for the Kafka executables and `config` for the configuration files.

There are several components required to run a minimal Kafka cluster. It's easiest to run them each in a separate terminal window, starting in the Kafka root directory.

First, start a ZooKeeper server:
``` shell
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Wait while it starts up and then prints a message like this:
```
INFO binding to port 0.0.0.0/0.0.0.0:2181
```

In another terminal, start a Kafka server:
``` shell
bin/kafka-server-start.sh config/server.properties
```
Wait while it starts up and then prints a message like this:
```
INFO [KafkaServer id=0] started
```

Now create a pair of topics `TSOURCE` and `TSINK`:
``` shell
bin/kafka-topics.sh --zookeeper localhost:2181  --create --topic TSOURCE --partitions 1 --replication-factor 1
bin/kafka-topics.sh --zookeeper localhost:2181  --create --topic TSINK --partitions 1 --replication-factor 1
```

You can see which topics exist like this:
``` shell
bin/kafka-topics.sh --zookeeper localhost:2181 --describe
```

You now have a Kafka cluster consisting of a single node. This configuration is just a toy, but it works fine for a little testing.

The configuration is as follows:
* Kafka bootstrap server - `localhost:9092`
* ZooKeeper server - `localhost:2181`
* Topic name - `TSOURCE` and `TSINK`

Note that this configuration of Kafka puts its data in `/tmp/kafka-logs`, while ZooKeeper uses `/tmp/zookeeper` and Kafka Connect uses `/tmp/connect.offsets`. You can clear out these directories to reset to an empty state, making sure beforehand that they're not being used for something else.


## Running the MQ source connector
The MQ source connector takes messages from an MQ queue and transfers them to a Kafka topic.

Follow the [instructions](https://github.com/ibm-messaging/kafka-connect-mq-source) and build the connector JAR. The top-level directory that you used to build the connector is referred to as the *connector root directory*.

In a terminal window, change directory into the connector root directory and copy the sample connector configuration file into your home directory so you can edit it safely:
``` shell
cp config/mq-source.properties ~
```

Edit the following properties in the `~/mq-source.properties` file to match the configuration so far:
```
mq.queue.manager=MYQM
mq.connection.name.list=localhost:1414
mq.channel.name=MYSVRCONN
mq.queue=MYQSOURCE
mq.user.name=alice
mq.password=passw0rd
topic=TSOURCE
```

Change directory to the Kafka root directory. Start the connector worker replacing `<connector-root-directory>`:
``` shell
CLASSPATH=<connector-root-directory>/target/kafka-connect-mq-source-0.2-SNAPSHOT-jar-with-dependencies.jar bin/connect-standalone.sh config/connect-standalone.properties ~/mq-source.properties
```

Wait while the worker starts and then prints:
```
INFO Created connector mq-source
```

If something goes wrong, you'll see familiar MQ reason codes in the error messages to help you diagnose the problems, such as:
```
ERROR MQ error: CompCode 2, Reason 2538 MQRC_HOST_NOT_AVAILABLE
```

You can just kill the worker, fix the problem and start it up again.

Once it's started successfully and connected to MQ, you'll see this message:
```
INFO Connection to MQ established
```

In another terminal window, use the Kafka console consumer to start consuming messages from your topic and print them to the console:
``` shell
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic TSOURCE
```

Now, run the `amqsput` sample and type in some messages to put on the MQ queue:
``` shell
/opt/mqm/samp/bin/amqsput MYQSOURCE MYQM
```

After a short delay, you should see the messages printed by the Kafka console consumer.

Congratulations! The messages were transferred from the MQ queue `MYQSOURCE` onto the Kafka topic `TSOURCE`.


## Running the MQ sink connector
The MQ sink connector takes messages from a Kafka topic and transfers them to an MQ queue. Running it is very similar to the source connector.

Follow the [instructions](https://github.com/ibm-messaging/kafka-connect-mq-sink) and build the connector JAR. The top-level directory that you used to build the connector is referred to as the *connector root directory*.

In a terminal window, change directory into the connector root directory and copy the sample connector configuration file into your home directory so you can edit it safely:
``` shell
cp config/mq-sink.properties ~
```

Edit the following properties in the `~/mq-sink.properties` to match the configuration so far:
```
topics=TSINK
mq.queue.manager=MYQM
mq.connection.name.list=localhost:1414
mq.channel.name=MYSVRCONN
mq.queue=MYQSINK
mq.user.name=alice
mq.password=passw0rd
```

Change directory to the Kafka root directory. Start the connector worker replacing `<connector-root-directory>`:
``` shell
CLASSPATH=<connector-root-directory>/target/kafka-connect-mq-sink-0.2-SNAPSHOT-jar-with-dependencies.jar bin/kafka-connect-standalone config/connect-standalone.properties ~/mq-sink.properties
```

Wait while the worker starts and then prints:
```
INFO Created connector mq-sink
```

If something goes wrong, you'll see familiar MQ reason codes in the error messages to help you diagnose the problems, such as:
```
ERROR MQ error: CompCode 2, Reason 2538 MQRC_HOST_NOT_AVAILABLE
```

You can just kill the worker, fix the problem and start it up again.

Once it's started successfully and connected to MQ, you'll see this message:
```
INFO Connection to MQ established
```

In another terminal window, use the Kafka console producer to type in some messages and publish them on the Kafka topic:
``` shell
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic TSINK
```

Now, use the `amqsget` sample to get the messages from the MQ queue:
``` shell
/opt/mqm/samp/bin/amqsget MYQSINK MYQM
```

After a short delay, you should see the messages printed.

Congratulations! The messages were transferred from the Kafka topic `TSINK` onto the MQ queue `MYQSINK`.


## Stopping Apache Kafka
After you have finished experimenting with this, you will probably want to stop Apache Kafka. You start by stopping any Kafka Connect workers and console producers and consumers that you may have left running.

Then, in the Kafka root directory, stop Kafka:
``` shell
bin/kafka-server-stop.sh
```

And finally, stop ZooKeeper:
``` shell
bin/zookeeper-server-stop.sh
```


## Using existing MQ or Kafka installations
You can use an existing MQ or Kafka installation, either locally or on the cloud. For performance reasons, it is recommended to run the Kafka Connect worker close to the queue manager to minimise the effect of network latency. So, if you have a queue manager in your datacenter and Kafka in the cloud, it's best to run the Kafka Connect worker in your datacenter.

To use an existing queue manager, you'll need to specify the configuration information in the connector configuration file.  You will need:ng configuration information:
* The hostname (or IP address) and port number for the queue manager
* The server-connection channel name
* If using user ID/password authentication, the user ID and password for client connection
* If using SSL/TLS, the name of the cipher suite to use
* The queue manager name
* The queue name

To use an existing Kafka cluster, you specify the connection information in the worker configuration file. You will need:
* A list of one or more servers for bootstrapping connections
* Whether the cluster requires connections to use SSL/TLS
* Authentication credentials if the cluster requires clients to authenticate

You will also need to run the Kafka Connect worker. If you already have access to the Kafka installation, you probably have the Kafka Connect executables. Otherwise, follow the instructions earlier to download Kafka.

Alternatively, IBM Message Hub is a fully managed cloud deployment of Apache Kafka running in IBM Cloud. In this case, the cluster is running in IBM Cloud but you will still need to run the Kafka Connect worker. If the queue manager is also in the cloud, you could also run the worker in the cloud. If the network latency between the queue manager and Kafka is high, you should run the worker near the queue manager. For information about how to configure Kafka Connect to work with IBM Message Hub read [this](https://console.bluemix.net/docs/services/MessageHub/messagehub113.html#kafka_connect).