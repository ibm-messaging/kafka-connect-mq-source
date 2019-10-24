# Using IBM MQ with Kafka Connect
Many organizations use both IBM MQ and Apache Kafka for their messaging needs. Although they're typically used to solve different kinds of messaging problems, people often want to connect them together. When connecting Apache Kafka to other systems, the technology of choice is the Kafka Connect framework.

These instructions tell you how to set up MQ and Apache Kafka from scratch and use the connectors to transfer messages between them using a client connection to MQ. The instructions are for MQ v9 running on Linux, so if you're using a different version or platform, you might have to adjust them slightly. The instructions also expect Apache Kafka 2.0.0 or later.

## Kafka Connect concepts
The best place to read about Kafka Connect is of course the Apache Kafka [documentation](https://kafka.apache.org/documentation/).

Kafka Connect connectors run inside a Java process called a *worker*. Kafka Connect can run in either standalone or distributed mode. Standalone mode is intended for testing and temporary connections between systems. Distributed mode is more appropriate for production use. These instructions focus on standalone mode because it's easier to see what's going on.

When you run Kafka Connect with a standalone worker, there are two configuration files. The *worker configuration file* contains the properties needed to connect to Kafka. The *connector configuration file* contains the properties needed for the connector. So, configuration to connect to Kafka goes into the worker configuration file, while the MQ configuration goes into the connector configuration file.

It's simplest to run just one connector in each standalone worker. Kafka Connect workers spew out a lot of messages and it's much simpler to read them if the messages from multiple connectors are not interleaved.


## Setting it up from scratch

The following steps show how to run the connector in standalone mode for development and test purposes.

### Prerequisites

- [IBM MQ](https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_8.0.0/com.ibm.mq.helphome.v80.doc/WelcomePagev8r0.htm) v8 or later installed.
   **Note:** These instructions are for IBM MQ v9 running on Linux. If you're using a different version or platform, you might have to adjust some steps slightly.
- An [Apache Kafka](http://kafka.apache.org/downloads) distribution which includes the Kafka Connect runtime environment. These instructions are for Apache Kafka 2.0.0 or later.

### Create a queue manager
These sample instructions set up an IBM MQ queue manager that uses its local operating system to authenticate the user ID and password. The example uses the user ID `alice` and the password `passw0rd`. The user ID and password you provide must already be created on the operating system where IBM MQ is running.

It is assumed that you have installed MQ, you're logged in as a user authorized to administer MQ and the MQ commands are on the path.

1. Create a queue manager with a TCP/IP listener (on port 1414 in this example):
    ``` shell
    crtmqm -p 1414 MYQM
    ```

2. Start the queue manager:
    ``` shell
    strmqm MYQM
    ```

3. Start the `runmqsc` tool to configure the queue manager:
    ``` shell
    runmqsc MYQM
    ```

4. In `runmqsc`, create a server-connection channel:
    ```
    DEFINE CHANNEL(MYSVRCONN) CHLTYPE(SVRCONN)
    ```

5. Set the channel authentication rules to accept connections requiring userid and password:
    ```
    SET CHLAUTH(MYSVRCONN) TYPE(BLOCKUSER) USERLIST('nobody')
    SET CHLAUTH('*') TYPE(ADDRESSMAP) ADDRESS('*') USERSRC(NOACCESS)
    SET CHLAUTH(MYSVRCONN) TYPE(ADDRESSMAP) ADDRESS('*') USERSRC(CHANNEL) CHCKCLNT(REQUIRED)
    ```

6. Set the identity of the client connections based on the supplied context, the user ID:
    ```
    ALTER AUTHINFO(SYSTEM.DEFAULT.AUTHINFO.IDPWOS) AUTHTYPE(IDPWOS) ADOPTCTX(YES)
    ```

7. Refresh the connection authentication information:
    ```
    REFRESH SECURITY TYPE(CONNAUTH)
    ```

8. Create a queue for the connector to use:
    ```
    DEFINE QLOCAL(MYQSOURCE)
    ```

9. Authorize `alice` to connect to and inquire the queue manager:
    ```
    SET AUTHREC OBJTYPE(QMGR) PRINCIPAL('alice') AUTHADD(CONNECT,INQ)
    ```

10. Finally authorize `alice` to use the queue:
    ```
    SET AUTHREC PROFILE(MYQSOURCE) OBJTYPE(QUEUE) PRINCIPAL('alice') AUTHADD(ALLMQI)
    ```

11. End `runmqsc`:
    ```
    END
    ```

The queue manager is now ready to accept connection from Kafka Connect connectors.

### Set up Apache Kafka
These instructions assume you have Apache Kafka downloaded and running locally. See the [Apache Kafka quickstart guide](https://kafka.apache.org/quickstart) for more details.

1. Start a ZooKeeper server:
    ``` shell
    bin/zookeeper-server-start.sh config/zookeeper.properties
    ```

2. In another terminal, start a Kafka server:
    ``` shell
    bin/kafka-server-start.sh config/server.properties
    ```
3. Create a topic called `TSOURCE` for the connector to send events to:
    ``` shell
    bin/kafka-topics.sh --zookeeper localhost:2181  --create --topic TSOURCE --partitions 1 --replication-factor 1
    ```

You now have a Kafka cluster consisting of a single node. This configuration is just a toy, but it works fine for a little testing.

The configuration is as follows:
* Kafka bootstrap server - `localhost:9092`
* ZooKeeper server - `localhost:2181`
* Topic name - `TSOURCE`

**Note:** This configuration of Kafka puts its data in `/tmp/kafka-logs`, while ZooKeeper uses `/tmp/zookeeper` and Kafka Connect uses `/tmp/connect.offsets`. You can clear out these directories to reset to an empty state, making sure beforehand that they're not being used for something else.


### Running the MQ source connector
The MQ source connector takes messages from an MQ queue and transfers them to a Kafka topic.

If you haven't already, clone and build the connector:
```shell
git clone https://github.com/ibm-messaging/kafka-connect-mq-source.git
cd kafka-connect-mq-source
mvn clean package
```

The top-level directory that you used to build the connector is referred to as the *connector root directory*.

Configure and run the connector:
1. In a terminal window, change directory into the connector root directory and copy the sample connector configuration file into your home directory so you can edit it safely:
    ``` shell
    cp config/mq-source.properties ~
    ```

2. Edit the following properties in the `~/mq-source.properties` file to match the configuration so far:
   ```
   topic=TSOURCE
   mq.queue.manager=MYQM
   mq.connection.name.list=localhost(1414)
   mq.channel.name=MYSVRCONN
   mq.queue=MYQSOURCE
   mq.user.name=alice
   mq.password=passw0rd
   ```

3. Change directory to the Kafka root directory. Start the connector worker replacing `<connector-root-directory>` and `<version>` with your directory and the connector version:
    ``` shell
    CLASSPATH=<connector-root-directory>/target/kafka-connect-mq-source-<version>-jar-with-dependencies.jar bin/connect-standalone.sh config/connect-standalone.properties ~/mq-source.properties
    ```
    The log output will include the following messages that indicate the connector worker has started and successfully connected to IBM MQ:
    ```
    INFO Created connector mq-source
    INFO Connection to MQ established
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

### Send messages from MQ to Kafka
In a new terminal window, use the Kafka console consumer to start consuming messages from your topic and print them to the console:
``` shell
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic TSOURCE
```

Now, run the `amqsput` sample and type in some messages to put on the MQ queue:
``` shell
/opt/mqm/samp/bin/amqsput MYQSOURCE MYQM
```

After a short delay, you should see the messages printed by the Kafka console consumer.

Congratulations! The messages were transferred from the MQ queue `MYQSOURCE` onto the Kafka topic `TSOURCE`.

### Stopping Apache Kafka
After you have finished experimenting with this, you will probably want to stop Apache Kafka. You start by stopping any Kafka Connect workers and console producers and consumers that you may have left running.

Then, in the Kafka root directory, stop Kafka and ZooKeeper:
``` shell
bin/kafka-server-stop.sh
bin/zookeeper-server-stop.sh
```

**Note:** Make sure Kafka is fully stopped before stopping ZooKeeper.

## Using existing MQ or Kafka installations
You can use an existing MQ or Kafka installation, either locally or on the cloud. For performance reasons, it is recommended to run the Kafka Connect worker close to the queue manager to minimise the effect of network latency. So, if you have a queue manager in your datacenter and Kafka in the cloud, it's best to run the Kafka Connect worker in your datacenter.

To use an existing queue manager, you'll need to specify the configuration information in the connector configuration file. For a client connection, you will need the following configuration information:
* The hostname (or IP address) and port number for the queue manager
* The server-connection channel name
* If using user ID/password authentication, the user ID and password for client connection
* If using SSL/TLS, the name of the cipher suite to use
* The queue manager name
* The queue name

For a bindings connection, you must run the worker on the same system as the queue manager. You will also have to make sure that the Kafka Connect worker has access to the native JNI library used by JMS to establish the bindings connection. You will need the following configuration information:
* The queue manager name
* The queue name
* Set the connection mode to "bindings"

To use an existing Kafka cluster, you specify the connection information in the worker configuration file. You will need:
* A list of one or more servers for bootstrapping connections
* Whether the cluster requires connections to use SSL/TLS
* Authentication credentials if the cluster requires clients to authenticate

You will also need to run the Kafka Connect worker. If you already have access to the Kafka installation, you probably have the Kafka Connect executables. Otherwise, download Apache Kafka from the [website](http://kafka.apache.org/downloads).

Alternatively, you can run the connector against IBM Event Streams.

### IBM Event Streams

[IBM Event Streams](https://www.ibm.com/cloud/event-streams) is an event-streaming platform based on the open-source Apache Kafka project. It builds upon the IBM Cloud Private platform to deploy Apache Kafka in a resilient and manageable way. A commercially supported version of this connector is available for customers with a support entitlement for IBM Event Streams.

See the [documentation](https://ibm.github.io/event-streams/connecting/mq/source) for a guide to running this connector against IBM Event Streams. 

##### IBM Event Streams for IBM Cloud

[IBM Event Streams for IBM Cloud](https://www.ibm.com/cloud/message-hub) is a fully-managed cloud deployment of Apache Kafka running in IBM Cloud. In this case, the cluster is running in IBM Cloud but you will still need to run the Kafka Connect worker. If the queue manager is also in the cloud, you could also run the worker in the cloud. If the network latency between the queue manager and Kafka is high, you should run the worker near the queue manager.

For information about how to configure Kafka Connect to work with IBM Event Streams for IBM Cloud see the [documentation](https://console.bluemix.net/docs/services/EventStreams/eventstreams113.html#kafka_connect).
