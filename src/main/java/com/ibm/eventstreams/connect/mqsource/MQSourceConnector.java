/**
 * Copyright 2017, 2020, 2023, 2024 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.eventstreams.connect.mqsource;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ConnectorTransactionBoundaries;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(MQSourceConnector.class);

    public static final ConfigDef CONFIGDEF;

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

    public static final String CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE = "mq.exactly.once.state.queue";
    public static final String CONFIG_DOCUMENTATION_MQ_EXACTLY_ONCE_STATE_QUEUE = "The name of the MQ queue used to store state. Required to run with exactly-once processing.";
    public static final String CONFIG_DISPLAY_MQ_EXACTLY_ONCE_STATE_QUEUE = "Exactly-once state queue";

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

    public static final String CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER = "mq.jms.properties.copy.to.kafka.headers";
    public static final String CONFIG_DOCUMENTATION_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER = "Whether to copy JMS message properties to Kafka headers.";
    public static final String CONFIG_DISPLAY_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER = "Copy JMS message properties to Kafka headers";

    public static final String CONFIG_NAME_MQ_RECORD_BUILDER_KEY_HEADER = "mq.record.builder.key.header";
    public static final String CONFIG_DOCUMENTATION_MQ_RECORD_BUILDER_KEY_HEADER = "The JMS message header to use as the Kafka record key.";
    public static final String CONFIG_DISPLAY_MQ_RECORD_BUILDER_KEY_HEADER = "Record builder key header";
    public static final String CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSMESSAGEID = "JMSMessageID";
    public static final String CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSCORRELATIONID = "JMSCorrelationID";
    public static final String CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSCORRELATIONIDASBYTES = "JMSCorrelationIDAsBytes";
    public static final String CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSDESTINATION = "JMSDestination";

    public static final String CONFIG_NAME_MQ_SSL_CIPHER_SUITE = "mq.ssl.cipher.suite";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_CIPHER_SUITE = "The name of the cipher suite for the TLS (SSL) connection.";
    public static final String CONFIG_DISPLAY_MQ_SSL_CIPHER_SUITE = "SSL cipher suite";

    public static final String CONFIG_NAME_MQ_SSL_PEER_NAME = "mq.ssl.peer.name";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_PEER_NAME = "The distinguished name pattern of the TLS (SSL) peer.";
    public static final String CONFIG_DISPLAY_MQ_SSL_PEER_NAME = "SSL peer name";

    public static final String CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION = "mq.ssl.keystore.location";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_KEYSTORE_LOCATION = "The path to the JKS keystore to use for the TLS (SSL) connection.";
    public static final String CONFIG_DISPLAY_MQ_SSL_KEYSTORE_LOCATION = "SSL keystore location";

    public static final String CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD = "mq.ssl.keystore.password";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_KEYSTORE_PASSWORD = "The password of the JKS keystore to use for the TLS (SSL) connection.";
    public static final String CONFIG_DISPLAY_MQ_SSL_KEYSTORE_PASSWORD = "SSL keystore password";

    public static final String CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION = "mq.ssl.truststore.location";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_TRUSTSTORE_LOCATION = "The path to the JKS truststore to use for the TLS (SSL) connection.";
    public static final String CONFIG_DISPLAY_MQ_SSL_TRUSTSTORE_LOCATION = "SSL truststore location";

    public static final String CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD = "mq.ssl.truststore.password";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_TRUSTSTORE_PASSWORD = "The password of the JKS truststore to use for the TLS (SSL) connection.";
    public static final String CONFIG_DISPLAY_MQ_SSL_TRUSTSTORE_PASSWORD = "SSL truststore password";

    public static final String CONFIG_NAME_MQ_SSL_USE_IBM_CIPHER_MAPPINGS = "mq.ssl.use.ibm.cipher.mappings";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_USE_IBM_CIPHER_MAPPINGS = "Whether to set system property to control use of IBM cipher mappings.";
    public static final String CONFIG_DISPLAY_MQ_SSL_USE_IBM_CIPHER_MAPPINGS = "Use IBM cipher mappings";

    public static final String CONFIG_NAME_MQ_BATCH_SIZE = "mq.batch.size";
    public static final String CONFIG_DOCUMENTATION_MQ_BATCH_SIZE = "The maximum number of messages in a batch. A batch uses a single unit of work.";
    public static final String CONFIG_DISPLAY_MQ_BATCH_SIZE = "Batch size";
    public static final int CONFIG_VALUE_MQ_BATCH_SIZE_DEFAULT = 250;
    public static final int CONFIG_VALUE_MQ_BATCH_SIZE_MINIMUM = 1;

    public static final String CONFIG_NAME_MQ_MESSAGE_MQMD_READ = "mq.message.mqmd.read";
    public static final String CONFIG_DOCUMENTATION_MQ_MESSAGE_MQMD_READ = "Whether to enable reading of all MQMD fields.";
    public static final String CONFIG_DISPLAY_MQ_MESSAGE_MQMD_READ = "Enable reading of MQMD fields";

    public static final String CONFIG_NAME_MQ_USER_AUTHENTICATION_MQCSP = "mq.user.authentication.mqcsp";
    public static final String CONFIG_DOCUMENTATION_MQ_USER_AUTHENTICATION_MQCSP = "Whether to use MQ connection security parameters (MQCSP).";
    public static final String CONFIG_DISPLAY_MQ_USER_AUTHENTICATION_MQCSP = "User authentication using MQCSP";

    public static final String CONFIG_NAME_TOPIC = "topic";
    public static final String CONFIG_DOCUMENTATION_TOPIC = "The name of the target Kafka topic.";
    public static final String CONFIG_DISPLAY_TOPIC = "Target Kafka topic";

    public static final String CONFIG_MAX_POLL_BLOCKED_TIME_MS = "mq.max.poll.blocked.time.ms";
    public static final String CONFIG_DOCUMENTATION_MAX_POLL_BLOCKED_TIME_MS = "How long the SourceTask will wait for a "
                                + "previous batch of messages to be delivered to Kafka before starting a new poll.";
    public static final String CONFIG_DISPLAY_MAX_POLL_BLOCKED_TIME_MS = "Max poll blocked time ms";

    public static final String CONFIG_NAME_MQ_CLIENT_RECONNECT_OPTIONS = "mq.client.reconnect.options";
    public static final String CONFIG_DOCUMENTATION_MQ_CLIENT_RECONNECT_OPTIONS = "Options governing MQ reconnection.";
    public static final String CONFIG_DISPLAY_MQ_CLIENT_RECONNECT_OPTIONS = "MQ client reconnect options";
    public static final String CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_QMGR = "QMGR";
    public static final String CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_ANY = "ANY";
    public static final String CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_DISABLED = "DISABLED";
    public static final String CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_ASDEF = "ASDEF";

    public static final String CONFIG_MAX_RECEIVE_TIMEOUT = "mq.message.receive.timeout";
    public static final String CONFIG_DOCUMENTATION_MAX_RECEIVE_TIMEOUT = "How long the connector should wait (in milliseconds) for a message to arrive if no message is available immediately";
    public static final String CONFIG_DISPLAY_MAX_RECEIVE_TIMEOUT = "Initial receive timeout (ms)";
    public static final long CONFIG_MAX_RECEIVE_TIMEOUT_DEFAULT = 2000L;
    public static final long CONFIG_MAX_RECEIVE_TIMEOUT_MINIMUM = 1L;

    public static final String CONFIG_SUBSEQUENT_RECEIVE_TIMEOUT = "mq.receive.subsequent.timeout.ms";
    public static final String CONFIG_DOCUMENTATION_SUBSEQUENT_RECEIVE_TIMEOUT = "How long (in milliseconds) the connector should wait for subsequent receives, "
            + "defaults to 0 (no-wait) and uses receiveNoWait().";
    public static final String CONFIG_DISPLAY_SUBSEQUENT_RECEIVE_TIMEOUT = "Subsequent receive timeout (ms)";
    public static final long CONFIG_SUBSEQUENT_RECEIVE_TIMEOUT_DEFAULT = 0L;

    public static final String CONFIG_RECONNECT_DELAY_MIN = "mq.reconnect.delay.min.ms";
    public static final String CONFIG_DOCUMENTATION_RECONNECT_DELAY_MIN = "The minimum delay in milliseconds for reconnect attempts.";
    public static final String CONFIG_DISPLAY_RECONNECT_DELAY_MIN = "Reconnect minimum delay";
    public static final long CONFIG_RECONNECT_DELAY_MIN_DEFAULT = 64L;
    public static final long CONFIG_RECONNECT_DELAY_MIN_MINIMUM = 1L;

    public static final String CONFIG_RECONNECT_DELAY_MAX = "mq.reconnect.delay.max.ms";
    public static final String CONFIG_DOCUMENTATION_RECONNECT_DELAY_MAX = "The maximum delay in milliseconds for reconnect attempts.";
    public static final String CONFIG_DISPLAY_RECONNECT_DELAY_MAX = "Reconnect maximum delay";
    public static final long CONFIG_RECONNECT_DELAY_MAX_DEFAULT = 8192L;
    public static final long CONFIG_RECONNECT_DELAY_MAX_MINIMUM = 10L;

    public static final String CONFIG_MAX_POLL_TIME = "mq.receive.max.poll.time.ms";
    public static final String CONFIG_DOCUMENTATION_MAX_POLL_TIME = "Maximum time (in milliseconds) to poll for messages during a single Kafka Connect poll cycle. "
            + "Acts as a hard upper bound on how long the task will try to accumulate a batch. "
            + "If set to 0 or not defined, polling continues until either a message receive returns null or the batch size is met. "
            + "Note: It is recommended to keep this value less than or equal to both 'mq.message.receive.timeout' "
            + "and 'mq.receive.subsequent.timeout.ms' to avoid unexpected delays due to long blocking receive calls.";
    public static final String CONFIG_DISPLAY_MAX_POLL_TIME = "Max poll time (ms)";
    public static final long CONFIG_MAX_POLL_TIME_DEFAULT = 0L;

    public static final String DLQ_PREFIX = "errors.deadletterqueue.";

    public static final String DLQ_TOPIC_NAME_CONFIG = DLQ_PREFIX + "topic.name";
    public static final String DLQ_TOPIC_NAME_DOC = "The name of the topic to be used as the dead letter queue (DLQ) for messages that " +
        "result in an error when processed by this source connector, or its transformations or converters. The topic name is blank by default, " +
        "which means that no messages are to be recorded in the DLQ.";
    public static final String DLQ_TOPIC_DEFAULT = "";
    private static final String DLQ_TOPIC_DISPLAY = "Dead Letter Queue Topic Name";

    public static final String DLQ_CONTEXT_HEADERS_ENABLE_CONFIG = DLQ_PREFIX + "context.headers.enable";
    public static final boolean DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT = false;
    public static final String DLQ_CONTEXT_HEADERS_ENABLE_DOC = "If true, add headers containing error context to the messages " +
            "written to the dead letter queue. To avoid clashing with headers from the original record, all error context header " +
            "keys, all error context header keys will start with <code>__connect.errors.</code>";
    private static final String DLQ_CONTEXT_HEADERS_ENABLE_DISPLAY = "Enable Error Context Headers";

    // Define valid reconnect options
    public static final String[] CONFIG_VALUE_MQ_VALID_RECONNECT_OPTIONS = {
        CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_ASDEF,
        CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_ASDEF.toLowerCase(Locale.ENGLISH),
        CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_ANY,
        CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_ANY.toLowerCase(Locale.ENGLISH),
        CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_QMGR,
        CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_QMGR.toLowerCase(Locale.ENGLISH),
        CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_DISABLED,
        CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_DISABLED.toLowerCase(Locale.ENGLISH)
    };

    public static String version = "2.4.0";

    private Map<String, String> configProps;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override public String version() {
        return version;
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override public void start(final Map<String, String> props) {
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        configProps = props;
        for (final Entry<String, String> entry : props.entrySet()) {
            final String value;
            if (entry.getKey().toLowerCase(Locale.ENGLISH).contains("password")) {
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
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        log.trace("[{}] Entry {}.taskConfigs, maxTasks={}", Thread.currentThread().getId(), this.getClass().getName(),
                maxTasks);

        final List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(configProps);
        }

        log.trace("[{}]  Exit {}.taskConfigs, retval={}", Thread.currentThread().getId(), this.getClass().getName(),
                taskConfigs);
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
     *
     * @return The ConfigDef for this connector.
     */
    @Override
    public ConfigDef config() {
        return CONFIGDEF;
    }

    @Override
    public Config validate(final Map<String, String> connectorConfigs) {
        final Config config = super.validate(connectorConfigs);

        MQSourceConnector.validateMQClientReconnectOptions(config);
        MQSourceConnector.validateRetryDelayConfig(config);
        return config;
    }

    private static void validateMQClientReconnectOptions(final Config config) {
        // Collect all configuration values
        final Map<String, ConfigValue> configValues = config.configValues().stream()
                .collect(Collectors.toMap(ConfigValue::name, v -> v));

        final ConfigValue clientReconnectOptionsConfigValue = configValues
                .get(MQSourceConnector.CONFIG_NAME_MQ_CLIENT_RECONNECT_OPTIONS);
        final ConfigValue exactlyOnceStateQueueConfigValue = configValues
                .get(MQSourceConnector.CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE);

        // Check if the exactly once state queue is configured
        if (exactlyOnceStateQueueConfigValue.value() == null) {
            return;
        }

        // Validate the client reconnect options
        final Boolean isClientReconnectOptionQMGR = CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_QMGR.equals(clientReconnectOptionsConfigValue.value());
        if (!isClientReconnectOptionQMGR) {
            clientReconnectOptionsConfigValue.addErrorMessage(
                    "When running the MQ source connector with exactly once mode, the client reconnect option 'QMGR' should be provided. For example: `mq.client.reconnect.options: QMGR`");
        }
    }

    /**
    * Validates if the retry delay max value is greater than or equal to the min value.
    * Adds an error message if the validation fails.
    */
    private static void validateRetryDelayConfig(final Config config) {
        // Collect all configuration values
        final Map<String, ConfigValue> configValues = config.configValues().stream()
                        .collect(Collectors.toMap(ConfigValue::name, v -> v));

        final ConfigValue reconnectDelayMaxConfigValue = configValues.get(MQSourceConnector.CONFIG_RECONNECT_DELAY_MAX);
        final ConfigValue reconnectDelayMinConfigValue = configValues.get(MQSourceConnector.CONFIG_RECONNECT_DELAY_MIN);

        final long maxReceiveTimeout = (long) reconnectDelayMaxConfigValue.value();
        final long minReceiveTimeout = (long) reconnectDelayMinConfigValue.value();

        // Validate if the max value is greater than min value
        if (maxReceiveTimeout < minReceiveTimeout) {
            reconnectDelayMaxConfigValue.addErrorMessage(String.format(
                "The value of '%s' must be greater than or equal to the value of '%s'.",
                MQSourceConnector.CONFIG_RECONNECT_DELAY_MAX,
                MQSourceConnector.CONFIG_RECONNECT_DELAY_MIN
            ));
        }
    }

    /** Null validator - indicates that any value is acceptable for this config option. */
    private static final ConfigDef.Validator ANY = null;

    static {
        CONFIGDEF = new ConfigDef();

        CONFIGDEF.define(CONFIG_NAME_MQ_QUEUE_MANAGER,
                Type.STRING,
                // user must specify the queue manager name
                ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyStringWithoutControlChars(),
                Importance.HIGH,
                CONFIG_DOCUMENTATION_MQ_QUEUE_MANAGER,
                CONFIG_GROUP_MQ, 1, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_QUEUE_MANAGER);

        CONFIGDEF.define(CONFIG_NAME_MQ_CONNECTION_MODE,
                Type.STRING,
                // required value - two valid options
                CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT,
                ConfigDef.ValidString.in(CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT,
                        CONFIG_VALUE_MQ_CONNECTION_MODE_BINDINGS),
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_CONNECTION_MODE,
                CONFIG_GROUP_MQ, 2, Width.SHORT,
                CONFIG_DISPLAY_MQ_CONNECTION_MODE);

        CONFIGDEF.define(CONFIG_NAME_MQ_CONNECTION_NAME_LIST,
                Type.STRING,
                // can be null, for example when using bindings mode or a CCDT
                null, ANY,
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_CONNNECTION_NAME_LIST,
                CONFIG_GROUP_MQ, 3, Width.LONG,
                CONFIG_DISPLAY_MQ_CONNECTION_NAME_LIST);

        CONFIGDEF.define(CONFIG_NAME_MQ_CHANNEL_NAME,
                Type.STRING,
                // can be null, for example when using bindings mode
                null, ANY,
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_CHANNEL_NAME,
                CONFIG_GROUP_MQ, 4, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_CHANNEL_NAME);

        CONFIGDEF.define(CONFIG_NAME_MQ_CCDT_URL,
                Type.STRING,
                // can be null, for example when using bindings mode or a conname list
                null, new ValidURL(),
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_CCDT_URL,
                CONFIG_GROUP_MQ, 5, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_CCDT_URL);

        CONFIGDEF.define(CONFIG_NAME_MQ_QUEUE,
                Type.STRING,
                // user must specify the queue name
                ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyStringWithoutControlChars(),
                Importance.HIGH,
                CONFIG_DOCUMENTATION_MQ_QUEUE,
                CONFIG_GROUP_MQ, 6, Width.LONG,
                CONFIG_DISPLAY_MQ_QUEUE);

        CONFIGDEF.define(CONFIG_NAME_MQ_USER_NAME,
                Type.STRING,
                // can be null, when auth not required
                null, ANY,
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_USER_NAME,
                CONFIG_GROUP_MQ, 7, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_USER_NAME);

        CONFIGDEF.define(CONFIG_NAME_MQ_PASSWORD,
                Type.PASSWORD,
                // can be null, when auth not required
                null, ANY,
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_PASSWORD,
                CONFIG_GROUP_MQ, 8, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_PASSWORD);

        CONFIGDEF.define(CONFIG_NAME_MQ_RECORD_BUILDER,
                Type.STRING,
                // user must specify a record builder class
                ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyStringWithoutControlChars(),
                Importance.HIGH,
                CONFIG_DOCUMENTATION_MQ_RECORD_BUILDER,
                CONFIG_GROUP_MQ, 9, Width.LONG,
                CONFIG_DISPLAY_MQ_RECORD_BUILDER);

        CONFIGDEF.define(CONFIG_NAME_MQ_MESSAGE_BODY_JMS,
                Type.BOOLEAN,
                // must be a non-null boolean - assume false if not provided
                Boolean.FALSE, new ConfigDef.NonNullValidator(),
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_MESSAGE_BODY_JMS,
                CONFIG_GROUP_MQ, 10, Width.SHORT,
                CONFIG_DISPLAY_MQ_MESSAGE_BODY_JMS);

        CONFIGDEF.define(CONFIG_NAME_MQ_RECORD_BUILDER_KEY_HEADER,
                Type.STRING,
                // optional value - four valid values
                null, ConfigDef.ValidString.in(null,
                        CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSMESSAGEID,
                        CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSCORRELATIONID,
                        CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSCORRELATIONIDASBYTES,
                        CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSDESTINATION),
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_RECORD_BUILDER_KEY_HEADER,
                CONFIG_GROUP_MQ, 11, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_RECORD_BUILDER_KEY_HEADER);

        CONFIGDEF.define(CONFIG_NAME_MQ_SSL_CIPHER_SUITE,
                Type.STRING,
                // optional - not needed if not using SSL - SSL cipher suites change
                //   too frequently so we won't maintain a valid list here
                null, ANY,
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_SSL_CIPHER_SUITE,
                CONFIG_GROUP_MQ, 12, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_SSL_CIPHER_SUITE);

        CONFIGDEF.define(CONFIG_NAME_MQ_SSL_PEER_NAME,
                Type.STRING,
                // optional - not needed if not using SSL
                null, ANY,
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_SSL_PEER_NAME,
                CONFIG_GROUP_MQ, 13, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_SSL_PEER_NAME);

        CONFIGDEF.define(CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION,
                Type.STRING,
                // optional - if provided should be the location of a readable file
                null, new ReadableFile(),
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_SSL_KEYSTORE_LOCATION,
                CONFIG_GROUP_MQ, 14, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_SSL_KEYSTORE_LOCATION);

        CONFIGDEF.define(CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD,
                Type.PASSWORD,
                // optional - not needed if SSL keystore isn't provided
                null, ANY,
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_SSL_KEYSTORE_PASSWORD,
                CONFIG_GROUP_MQ, 15, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_SSL_KEYSTORE_PASSWORD);

        CONFIGDEF.define(CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION,
                Type.STRING,
                // optional - if provided should be the location of a readable file
                null, new ReadableFile(),
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_SSL_TRUSTSTORE_LOCATION,
                CONFIG_GROUP_MQ, 16, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_SSL_TRUSTSTORE_LOCATION);

        CONFIGDEF.define(CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD,
                Type.PASSWORD,
                // optional - not needed if SSL truststore isn't provided
                null, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_SSL_TRUSTSTORE_PASSWORD,
                CONFIG_GROUP_MQ, 17, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_SSL_TRUSTSTORE_PASSWORD);

        CONFIGDEF.define(CONFIG_NAME_MQ_BATCH_SIZE,
                Type.INT,
                // must be an int greater than min
                CONFIG_VALUE_MQ_BATCH_SIZE_DEFAULT, ConfigDef.Range.atLeast(CONFIG_VALUE_MQ_BATCH_SIZE_MINIMUM),
                Importance.LOW,
                CONFIG_DOCUMENTATION_MQ_BATCH_SIZE,
                CONFIG_GROUP_MQ, 18, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_BATCH_SIZE);

        CONFIGDEF.define(CONFIG_NAME_MQ_MESSAGE_MQMD_READ,
                Type.BOOLEAN,
                // must be a non-null boolean - assume false if not provided
                Boolean.FALSE, new ConfigDef.NonNullValidator(),
                Importance.LOW,
                CONFIG_DOCUMENTATION_MQ_MESSAGE_MQMD_READ,
                CONFIG_GROUP_MQ, 19, Width.SHORT,
                CONFIG_DISPLAY_MQ_MESSAGE_MQMD_READ);

        CONFIGDEF.define(CONFIG_NAME_MQ_USER_AUTHENTICATION_MQCSP,
                Type.BOOLEAN,
                // must be a non-null boolean - assume true if not provided
                Boolean.TRUE, new ConfigDef.NonNullValidator(),
                Importance.LOW,
                CONFIG_DOCUMENTATION_MQ_USER_AUTHENTICATION_MQCSP,
                CONFIG_GROUP_MQ, 20, Width.SHORT,
                CONFIG_DISPLAY_MQ_USER_AUTHENTICATION_MQCSP);

        CONFIGDEF.define(CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER,
                Type.BOOLEAN,
                // must be a non-null boolean - assume false if not provided
                Boolean.FALSE, new ConfigDef.NonNullValidator(),
                Importance.LOW,
                CONFIG_DOCUMENTATION_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER,
                CONFIG_GROUP_MQ, 21, Width.MEDIUM,
                CONFIG_DISPLAY_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER);

        CONFIGDEF.define(CONFIG_NAME_MQ_SSL_USE_IBM_CIPHER_MAPPINGS,
                Type.BOOLEAN,
                // must be a non-null boolean - assume true if not provided
                Boolean.TRUE, new ConfigDef.NonNullValidator(),
                Importance.LOW,
                CONFIG_DOCUMENTATION_MQ_SSL_USE_IBM_CIPHER_MAPPINGS,
                CONFIG_GROUP_MQ, 22, Width.SHORT,
                CONFIG_DISPLAY_MQ_SSL_USE_IBM_CIPHER_MAPPINGS);

        CONFIGDEF.define(CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE,
                Type.STRING,
                null, ANY,
                Importance.LOW,
                CONFIG_DOCUMENTATION_MQ_EXACTLY_ONCE_STATE_QUEUE,
                CONFIG_GROUP_MQ, 23, Width.LONG,
                CONFIG_DISPLAY_MQ_EXACTLY_ONCE_STATE_QUEUE);

        // How long the SourceTask will wait for a previous batch of messages to
        //  be delivered to Kafka before starting a new poll.
        // It is important that this is less than the time defined for
        //  task.shutdown.graceful.timeout.ms as that is how long Connect will
        //  wait for the task to perform lifecycle operations.
        CONFIGDEF.define(CONFIG_MAX_POLL_BLOCKED_TIME_MS,
                Type.INT,
                2000, ConfigDef.Range.atLeast(0),
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MAX_POLL_BLOCKED_TIME_MS,
                null, 24, Width.MEDIUM,
                CONFIG_DISPLAY_MAX_POLL_BLOCKED_TIME_MS);

        CONFIGDEF.define(CONFIG_NAME_MQ_CLIENT_RECONNECT_OPTIONS,
                Type.STRING,
                CONFIG_VALUE_MQ_CLIENT_RECONNECT_OPTION_ASDEF,
                ConfigDef.ValidString.in(CONFIG_VALUE_MQ_VALID_RECONNECT_OPTIONS),
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MQ_CLIENT_RECONNECT_OPTIONS,
                CONFIG_GROUP_MQ, 25,
                Width.SHORT,
                CONFIG_DISPLAY_MQ_CLIENT_RECONNECT_OPTIONS);
        CONFIGDEF.define(CONFIG_MAX_RECEIVE_TIMEOUT,
                ConfigDef.Type.LONG,
                CONFIG_MAX_RECEIVE_TIMEOUT_DEFAULT,
                ConfigDef.Range.atLeast(CONFIG_MAX_RECEIVE_TIMEOUT_MINIMUM),
                ConfigDef.Importance.MEDIUM,
                CONFIG_DOCUMENTATION_MAX_RECEIVE_TIMEOUT,
                CONFIG_GROUP_MQ,
                26,
                ConfigDef.Width.MEDIUM,
                CONFIG_DISPLAY_MAX_RECEIVE_TIMEOUT);
        CONFIGDEF.define(CONFIG_SUBSEQUENT_RECEIVE_TIMEOUT,
                ConfigDef.Type.LONG,
                CONFIG_SUBSEQUENT_RECEIVE_TIMEOUT_DEFAULT, ConfigDef.Range.atLeast(CONFIG_SUBSEQUENT_RECEIVE_TIMEOUT_DEFAULT),
                ConfigDef.Importance.LOW,
                CONFIG_DOCUMENTATION_SUBSEQUENT_RECEIVE_TIMEOUT,
                CONFIG_GROUP_MQ,
                27,
                ConfigDef.Width.MEDIUM,
                CONFIG_DISPLAY_SUBSEQUENT_RECEIVE_TIMEOUT);
        CONFIGDEF.define(CONFIG_RECONNECT_DELAY_MIN,
                Type.LONG,
                CONFIG_RECONNECT_DELAY_MIN_DEFAULT, ConfigDef.Range.atLeast(CONFIG_RECONNECT_DELAY_MIN_MINIMUM),
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_RECONNECT_DELAY_MIN,
                CONFIG_GROUP_MQ, 28,
                Width.MEDIUM,
                CONFIG_DISPLAY_RECONNECT_DELAY_MIN);
        CONFIGDEF.define(CONFIG_RECONNECT_DELAY_MAX,
                Type.LONG,
                CONFIG_RECONNECT_DELAY_MAX_DEFAULT, ConfigDef.Range.atLeast(CONFIG_RECONNECT_DELAY_MAX_MINIMUM),
                Importance.MEDIUM,
                CONFIG_DOCUMENTATION_RECONNECT_DELAY_MAX,
                CONFIG_GROUP_MQ, 29,
                Width.MEDIUM,
                CONFIG_DISPLAY_RECONNECT_DELAY_MAX);
        CONFIGDEF.define(DLQ_TOPIC_NAME_CONFIG,
                Type.STRING,
                DLQ_TOPIC_DEFAULT,
                Importance.MEDIUM,
                DLQ_TOPIC_NAME_DOC,
                CONFIG_GROUP_MQ, 30,
                Width.MEDIUM,
                DLQ_TOPIC_DISPLAY);
        CONFIGDEF.define(DLQ_CONTEXT_HEADERS_ENABLE_CONFIG,
                Type.BOOLEAN,
                DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT,
                Importance.MEDIUM,
                DLQ_CONTEXT_HEADERS_ENABLE_DOC,
                CONFIG_GROUP_MQ, 31,
                Width.MEDIUM,
                DLQ_CONTEXT_HEADERS_ENABLE_DISPLAY);
        CONFIGDEF.define(CONFIG_MAX_POLL_TIME,
                ConfigDef.Type.LONG,
                CONFIG_MAX_POLL_TIME_DEFAULT, ConfigDef.Range.atLeast(CONFIG_MAX_POLL_TIME_DEFAULT),
                ConfigDef.Importance.LOW,
                CONFIG_DOCUMENTATION_MAX_POLL_TIME,
                CONFIG_GROUP_MQ,
                32,
                ConfigDef.Width.MEDIUM,
                CONFIG_DISPLAY_MAX_POLL_TIME);

        CONFIGDEF.define(CONFIG_NAME_TOPIC,
                Type.STRING,
                // user must specify the topic name
                ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyStringWithoutControlChars(),
                Importance.HIGH,
                CONFIG_DOCUMENTATION_TOPIC,
                null, 0, Width.MEDIUM,
                CONFIG_DISPLAY_TOPIC);
    }


    private static class ReadableFile implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            final String strValue = (String) value;
            if (value == null || strValue.isEmpty()) {
                // only validate non-empty locations
                return;
            }

            final File file;
            try {
                file = new File((String) value);
            } catch (final Exception exc) {
                throw new ConfigException(name, value, "Value must be a valid file location");
            }
            if (!file.isFile() || !file.canRead()) {
                throw new ConfigException(name, value, "Value must be the location of a readable file");
            }
        }
    }

    private static class ValidURL implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            final String strValue = (String) value;
            if (value == null || strValue.isEmpty()) {
                // only validate non-empty locations
                return;
            }

            try {
                new URL(strValue);
            } catch (final MalformedURLException exc) {
                throw new ConfigException(name, value, "Value must be a valid URL");
            }
        }
    }

    /**
     * Signals that this connector is not capable of defining other transaction boundaries.
     * A new transaction will be started and committed for every batch of records returned by {@link MQSourceTask#poll()}.
     *
     * @param connectorConfig the configuration that will be used for the connector
     * @return {@link ConnectorTransactionBoundaries#UNSUPPORTED}
     */
    @Override
    public ConnectorTransactionBoundaries canDefineTransactionBoundaries(final Map<String, String> connectorConfig) {
        // The connector only supports Kafka transaction boundaries on the poll() method
        return ConnectorTransactionBoundaries.UNSUPPORTED;
    }

    /**
     * Signals whether this connector supports exactly-once semantics with the supplied configuration.
     *
     * @param connectorConfig the configuration that will be used for the connector.
     * 'mq.exactly.once.state.queue' must be supplied in the configuration to enable exactly-once semantics.
     *
     * @return {@link ExactlyOnceSupport#SUPPORTED} if the configuration supports exactly-once semantics,
     * {@link ExactlyOnceSupport#UNSUPPORTED} otherwise.
     */
    @Override
    public ExactlyOnceSupport exactlyOnceSupport(final Map<String, String> connectorConfig) {
        if (configSupportsExactlyOnce(connectorConfig)) {
            return ExactlyOnceSupport.SUPPORTED;
        }
        return ExactlyOnceSupport.UNSUPPORTED;
    }

    /**
     * Returns true if the supplied connector configuration supports exactly-once semantics.
     * Checks that 'mq.exactly.once.state.queue' property is supplied and is not empty and
     * that 'tasks.max' is 1.
     *
     * @param connectorConfig the connector config
     * @return true if 'mq.exactly.once.state.queue' property is supplied and is not empty and 'tasks.max' is 1.
     */
    public static final boolean configSupportsExactlyOnce(final Map<String, String> connectorConfig) {
        // If there is a state queue configured and tasks.max is 1 we can do exactly-once semantics
        final String exactlyOnceStateQueue = connectorConfig.get(CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE);
        final String tasksMax = connectorConfig.get("tasks.max");
        return exactlyOnceStateQueue != null && !exactlyOnceStateQueue.isEmpty() && (tasksMax == null || "1".equals(tasksMax));
    }
}