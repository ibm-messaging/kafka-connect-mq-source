/**
 * Copyright 2017, 2018, 2019 IBM Corporation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.eventstreams.connect.mqsource;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MQSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(MQSourceConnector.class);

    public static final String CONFIG_GROUP_MQ = "mq";

    public static final String CONFIG_NAME_MQ_QUEUE_MANAGER = "mq.queue.manager";
    public static final String CONFIG_DOCUMENTATION_MQ_QUEUE_MANAGER = "The name of the MQ queue manager.";
    public static final String CONFIG_DISPLAY_MQ_QUEUE_MANAGER = "Queue manager";

    public static final String CONFIG_NAME_MQ_CONNECTION_MODE = "mq.connection.mode";
    public static final String CONFIG_DOCUMENTATION_MQ_CONNECTION_MODE = "The connection mode - bindings or client.";
    public static final String CONFIG_DISPLAY_MQ_CONNECTION_MODE = "Connection mode";
    public static final String CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT = "client";
    public static final String CONFIG_VALUE_MQ_CONNECTION_MODE_BINDINGS = "bindings";

    public static final String CONFIG_NAME_MQ_CONNECTION_NAME_LIST = "mq.connection.name.list";
    public static final String CONFIG_DOCUMENTATION_MQ_CONNNECTION_NAME_LIST = "A list of one or more host(port) entries for connecting to the queue manager. Entries are separated with a comma.";
    public static final String CONFIG_DISPLAY_MQ_CONNECTION_NAME_LIST = "List of connection names for queue manager";

    public static final String CONFIG_NAME_MQ_CHANNEL_NAME = "mq.channel.name";
    public static final String CONFIG_DOCUMENTATION_MQ_CHANNEL_NAME = "The name of the server-connection channel.";
    public static final String CONFIG_DISPLAY_MQ_CHANNEL_NAME = "Channel name";

    public static final String CONFIG_NAME_MQ_QUEUE = "mq.queue";
    public static final String CONFIG_DOCUMENTATION_MQ_QUEUE = "The name of the source MQ queue.";
    public static final String CONFIG_DISPLAY_MQ_QUEUE = "Source queue";

    public static final String CONFIG_NAME_MQ_USER_NAME = "mq.user.name";
    public static final String CONFIG_DOCUMENTATION_MQ_USER_NAME = "The user name for authenticating with the queue manager.";
    public static final String CONFIG_DISPLAY_MQ_USER_NAME = "User name";

    public static final String CONFIG_NAME_MQ_PASSWORD = "mq.password";
    public static final String CONFIG_DOCUMENTATION_MQ_PASSWORD = "The password for authenticating with the queue manager.";
    public static final String CONFIG_DISPLAY_MQ_PASSWORD = "Password";

    public static final String CONFIG_NAME_MQ_CCDT_URL = "mq.ccdt.url";
    public static final String CONFIG_DOCUMENTATION_MQ_CCDT_URL = "The CCDT URL to use to establish a connection to the queue manager.";
    public static final String CONFIG_DISPLAY_MQ_CCDT_URL = "CCDT URL";

    public static final String CONFIG_NAME_MQ_RECORD_BUILDER = "mq.record.builder";
    public static final String CONFIG_DOCUMENTATION_MQ_RECORD_BUILDER = "The class used to build the Kafka Connect records.";
    public static final String CONFIG_DISPLAY_MQ_RECORD_BUILDER = "Record builder";

    public static final String CONFIG_NAME_MQ_MESSAGE_BODY_JMS = "mq.message.body.jms";
    public static final String CONFIG_DOCUMENTATION_MQ_MESSAGE_BODY_JMS = "Whether to interpret the message body as a JMS message type.";
    public static final String CONFIG_DISPLAY_MQ_MESSAGE_BODY_JMS = "Message body as JMS";

    public static final String CONFIG_NAME_MQ_RECORD_BUILDER_KEY_HEADER = "mq.record.builder.key.header";
    public static final String CONFIG_DOCUMENTATION_MQ_RECORD_BUILDER_KEY_HEADER = "The JMS message header to use as the Kafka record key.";
    public static final String CONFIG_DISPLAY_MQ_RECORD_BUILDER_KEY_HEADER = "Record builder key header";
    public static final String CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSMESSAGEID = "JMSMessageID";
    public static final String CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSCORRELATIONID = "JMSCorrelationID";
    public static final String CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSCORRELATIONIDASBYTES = "JMSCorrelationIDAsBytes";

    public static final String CONFIG_NAME_MQ_SSL_CIPHER_SUITE = "mq.ssl.cipher.suite";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_CIPHER_SUITE = "The name of the cipher suite for the TLS (SSL) connection.";
    public static final String CONFIG_DISPLAY_MQ_SSL_CIPHER_SUITE = "SSL cipher suite";

    public static final String CONFIG_NAME_MQ_SSL_PEER_NAME = "mq.ssl.peer.name";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_PEER_NAME = "The distinguished name pattern of the TLS (SSL) peer.";
    public static final String CONFIG_DISPLAY_MQ_SSL_PEER_NAME = "SSL peer name";

    public static final String CONFIG_NAME_MQ_BATCH_SIZE = "mq.batch.size";
    public static final String CONFIG_DOCUMENTATION_MQ_BATCH_SIZE = "The maximum number of messages in a batch. A batch uses a single unit of work.";
    public static final String CONFIG_DISPLAY_MQ_BATCH_SIZE = "Batch size";
    public static final int CONFIG_VALUE_MQ_BATCH_SIZE_DEFAULT = 250;
    public static final int CONFIG_VALUE_MQ_BATCH_SIZE_MINIMUM = 1;

    public static final String CONFIG_NAME_TOPIC = "topic";
    public static final String CONFIG_DOCUMENTATION_TOPIC = "The name of the target Kafka topic.";
    public static final String CONFIG_DISPLAY_TOPIC = "Target Kafka topic";

    //PROPERTY SCHEMA REGISTRY URLS FOR AVRO CONVERSIONS
    public static final String CONFIG_SCHEMA_REGISTRY_URLS = "schema.registry.url";
    public static final String CONFIG_DOCUMENTATION_SCHEMA_REGISTRY_URLS = "Schema Registry URLs used for AVRO schemas.";
    public static final String CONFIG_DISPLAY_SCHEMA_REGISTRY_URLS = "Schema Registry URLs";

    public static String VERSION = "1.0.3";

    private Map<String, String> configProps;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return VERSION;
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        configProps = props;
        for (final Entry<String, String> entry : props.entrySet()) {
            String value;
            if (entry.getKey().toLowerCase().contains("password")) {
                value = "[hidden]";
            } else {
                value = entry.getValue();
            }
            log.debug("Connector props entry {} : {}", entry.getKey(), value);
        }

        log.trace("[{}]  Exit {}.start", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Returns the Task implementation for this Connector.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return MQSourceTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.trace("[{}] Entry {}.taskConfigs, maxTasks={}", Thread.currentThread().getId(), this.getClass().getName(), maxTasks);

        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(configProps);
        }

        log.trace("[{}]  Exit {}.taskConfigs, retval={}", Thread.currentThread().getId(), this.getClass().getName(), taskConfigs);
        return taskConfigs;
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() {
        log.trace("[{}] Entry {}.stop", Thread.currentThread().getId(), this.getClass().getName());
        log.trace("[{}]  Exit {}.stop", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Define the configuration for the connector.
     * @return The ConfigDef for this connector.
     */
    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();

        config.define(CONFIG_NAME_MQ_QUEUE_MANAGER, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                CONFIG_DOCUMENTATION_MQ_QUEUE_MANAGER, CONFIG_GROUP_MQ, 1, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_QUEUE_MANAGER);

        config.define(CONFIG_NAME_MQ_CONNECTION_MODE, Type.STRING, CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT,
                ConfigDef.ValidString.in(CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT,
                        CONFIG_VALUE_MQ_CONNECTION_MODE_BINDINGS),
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_CONNECTION_MODE, CONFIG_GROUP_MQ, 2, Width.SHORT,
                CONFIG_DISPLAY_MQ_CONNECTION_MODE);

        config.define(CONFIG_NAME_MQ_CONNECTION_NAME_LIST, Type.STRING, null, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_CONNNECTION_NAME_LIST, CONFIG_GROUP_MQ, 3, Width.LONG,
                CONFIG_DISPLAY_MQ_CONNECTION_NAME_LIST);

        config.define(CONFIG_NAME_MQ_CHANNEL_NAME, Type.STRING, null, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_CHANNEL_NAME, CONFIG_GROUP_MQ, 4, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_CHANNEL_NAME);

        config.define(CONFIG_NAME_MQ_CCDT_URL, Type.STRING, null, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_CCDT_URL, CONFIG_GROUP_MQ, 5, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_CCDT_URL);

        config.define(CONFIG_NAME_MQ_QUEUE, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                CONFIG_DOCUMENTATION_MQ_QUEUE, CONFIG_GROUP_MQ, 6, Width.LONG,
                CONFIG_DISPLAY_MQ_QUEUE);

        config.define(CONFIG_NAME_MQ_USER_NAME, Type.STRING, null, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_USER_NAME, CONFIG_GROUP_MQ, 7, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_USER_NAME);

        config.define(CONFIG_NAME_MQ_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_PASSWORD, CONFIG_GROUP_MQ, 8, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_PASSWORD);

        config.define(CONFIG_NAME_MQ_RECORD_BUILDER, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                CONFIG_DOCUMENTATION_MQ_RECORD_BUILDER, CONFIG_GROUP_MQ, 9, Width.LONG,
                CONFIG_DISPLAY_MQ_RECORD_BUILDER);

        config.define(CONFIG_NAME_MQ_MESSAGE_BODY_JMS, Type.BOOLEAN, Boolean.FALSE, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_MESSAGE_BODY_JMS, CONFIG_GROUP_MQ, 10, Width.SHORT,
                CONFIG_DISPLAY_MQ_MESSAGE_BODY_JMS);

        config.define(CONFIG_NAME_MQ_RECORD_BUILDER_KEY_HEADER, Type.STRING, null, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_RECORD_BUILDER_KEY_HEADER, CONFIG_GROUP_MQ, 11, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_RECORD_BUILDER_KEY_HEADER);

        config.define(CONFIG_NAME_MQ_SSL_CIPHER_SUITE, Type.STRING, null, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_SSL_CIPHER_SUITE, CONFIG_GROUP_MQ, 12, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_SSL_CIPHER_SUITE);

        config.define(CONFIG_NAME_MQ_SSL_PEER_NAME, Type.STRING, null, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_SSL_PEER_NAME, CONFIG_GROUP_MQ, 13, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_SSL_PEER_NAME);

        config.define(CONFIG_NAME_MQ_BATCH_SIZE, Type.INT, CONFIG_VALUE_MQ_BATCH_SIZE_DEFAULT,
                ConfigDef.Range.atLeast(CONFIG_VALUE_MQ_BATCH_SIZE_MINIMUM), Importance.LOW,
                CONFIG_DOCUMENTATION_MQ_BATCH_SIZE, CONFIG_GROUP_MQ, 14, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_BATCH_SIZE);

        config.define(CONFIG_NAME_TOPIC, Type.STRING, null, Importance.HIGH,
                CONFIG_DOCUMENTATION_TOPIC, null, 0, Width.MEDIUM,
                CONFIG_DISPLAY_TOPIC);

        //CONFIG FOR SCHEMA REGISTRY URLS ABOUT AVRO CONVERSIONS
        config.define(CONFIG_SCHEMA_REGISTRY_URLS, Type.LIST, null, Importance.LOW,
                CONFIG_DOCUMENTATION_SCHEMA_REGISTRY_URLS, null, 0, Width.LONG,
                CONFIG_DISPLAY_SCHEMA_REGISTRY_URLS);
        return config;
    }
}