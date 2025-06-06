/**
 * Copyright 2018, 2019, 2023, 2024 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsource.builders;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import javax.jms.BytesMessage;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.errors.DeadLetterQueueReporter;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.eventstreams.connect.mqsource.MQSourceConnector;
import com.ibm.eventstreams.connect.mqsource.processor.JmsToKafkaHeaderConverter;

/**
 * Builds Kafka Connect SourceRecords from messages.
 */
public abstract class BaseRecordBuilder implements RecordBuilder {
    private static final Logger log = LoggerFactory.getLogger(BaseRecordBuilder.class);

    public enum KeyHeader {
        NONE, MESSAGE_ID, CORRELATION_ID, CORRELATION_ID_AS_BYTES, DESTINATION
    };

    protected KeyHeader keyheader = KeyHeader.NONE;

    private boolean copyJmsPropertiesFlag = Boolean.FALSE;
    private JmsToKafkaHeaderConverter jmsToKafkaHeaderConverter;
    private boolean tolerateErrors;
    private boolean logErrors;
    private boolean logIncludeMessages;
    private String dlqTopic = "";

    public static final String ERROR_HEADER_EXCEPTION_TIMESTAMP = DeadLetterQueueReporter.HEADER_PREFIX + "timestamp";
    public static final String ERROR_HEADER_EXCEPTION_CAUSE_CLASS = DeadLetterQueueReporter.HEADER_PREFIX + "cause.class";
    public static final String ERROR_HEADER_EXCEPTION_CAUSE_MESSAGE = DeadLetterQueueReporter.HEADER_PREFIX + "cause.message";

    /**
     * Configure this class.
     *
     * @param props initial configuration
     *
     * @throws RecordBuilderException Operation failed and connector should stop.
     */
    @Override
    public void configure(final Map<String, String> props) {
        log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(), this.getClass().getName(),
                props);

        initializeErrorTolerance(props);

        final String kh = props.get(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER_KEY_HEADER);
        if (kh != null) {
            if (kh.equals(MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSMESSAGEID)) {
                keyheader = KeyHeader.MESSAGE_ID;
                log.debug("Setting Kafka record key from JMSMessageID header field");
            } else if (kh.equals(MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSCORRELATIONID)) {
                keyheader = KeyHeader.CORRELATION_ID;
                log.debug("Setting Kafka record key from JMSCorrelationID header field");
            } else if (kh.equals(MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSCORRELATIONIDASBYTES)) {
                keyheader = KeyHeader.CORRELATION_ID_AS_BYTES;
                log.debug("Setting Kafka record key from JMSCorrelationIDAsBytes header field");
            } else if (kh.equals(MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSDESTINATION)) {
                keyheader = KeyHeader.DESTINATION;
                log.debug("Setting Kafka record key from JMSDestination header field");
            } else {
                log.error("Unsupported MQ record builder key header value {}", kh);
                throw new RecordBuilderException("Unsupported MQ record builder key header value");
            }
        }

        final String str = props.get(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER);
        copyJmsPropertiesFlag = Boolean.parseBoolean(Optional.ofNullable(str).orElse("false"));
        jmsToKafkaHeaderConverter = new JmsToKafkaHeaderConverter();

        log.trace("[{}]  Exit {}.configure", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Initializes error tolerance configuration by reading directly from properties
     * map
     * instead of using AbstractConfig
     */
    private void initializeErrorTolerance(final Map<String, String> props) {
        // Read tolerateErrors directly from props
        final String errorToleranceValue = props.getOrDefault(
                ConnectorConfig.ERRORS_TOLERANCE_CONFIG,
                ToleranceType.NONE.toString()).toUpperCase(Locale.ROOT);

        tolerateErrors = ToleranceType.valueOf(errorToleranceValue).equals(ToleranceType.ALL);

        // Read logErrors directly from props
        if (tolerateErrors) {
            final String logErrorsValue = props.getOrDefault(
                    ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG,
                    String.valueOf(ConnectorConfig.ERRORS_LOG_ENABLE_DEFAULT));
            logErrors = Boolean.parseBoolean(logErrorsValue);
            final String logIncludeMessagesValue = props.getOrDefault(
                    ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG,
                    String.valueOf(ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT));
            logIncludeMessages = Boolean.parseBoolean(logIncludeMessagesValue);

            dlqTopic = props.get(MQSourceConnector.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
            if (dlqTopic != null && !dlqTopic.isEmpty()) {
                dlqTopic = dlqTopic.trim();
            }
        } else {
            logErrors = false;
            logIncludeMessages = false;
        }
    }

    /**
     * Gets the key to use for the Kafka Connect SourceRecord.
     *
     * @param context the JMS context to use for building messages
     * @param topic   the Kafka topic
     * @param message the message
     *
     * @return the Kafka Connect SourceRecord's key
     *
     * @throws JMSException Message could not be converted
     */
    public SchemaAndValue getKey(final JMSContext context, final String topic, final Message message)
            throws JMSException {
        Schema keySchema = null;
        Object key = null;
        final String keystr;

        switch (keyheader) {
            case MESSAGE_ID:
                keySchema = Schema.OPTIONAL_STRING_SCHEMA;
                keystr = message.getJMSMessageID();
                if (keystr.startsWith("ID:", 0)) {
                    key = keystr.substring(3);
                } else {
                    key = keystr;
                }
                break;
            case CORRELATION_ID:
                keySchema = Schema.OPTIONAL_STRING_SCHEMA;
                keystr = message.getJMSCorrelationID();
                if (keystr.startsWith("ID:", 0)) {
                    key = keystr.substring(3);
                } else {
                    key = keystr;
                }
                break;
            case CORRELATION_ID_AS_BYTES:
                keySchema = Schema.OPTIONAL_BYTES_SCHEMA;
                key = message.getJMSCorrelationIDAsBytes();
                break;
            case DESTINATION:
                keySchema = Schema.OPTIONAL_STRING_SCHEMA;
                key = message.getJMSDestination().toString();
                break;
            default:
                break;
        }

        return new SchemaAndValue(keySchema, key);
    }

    /**
     * Gets the value to use for the Kafka Connect SourceRecord.
     *
     * @param context        the JMS context to use for building messages
     * @param topic          the Kafka topic
     * @param messageBodyJms whether to interpret MQ messages as JMS messages
     * @param message        the message
     *
     * @return the Kafka Connect SourceRecord's value
     *
     * @throws JMSException Message could not be converted
     */
    public abstract SchemaAndValue getValue(JMSContext context, String topic, boolean messageBodyJms, Message message)
            throws JMSException;

    /**
     * Convert a message into a Kafka Connect SourceRecord.
     *
     * @param context        the JMS context to use for building messages
     * @param topic          the Kafka topic
     * @param messageBodyJms whether to interpret MQ messages as JMS messages
     * @param message        the message
     *
     * @return the Kafka Connect SourceRecord
     *
     * @throws JMSException Message could not be converted
     */
    @Override
    public SourceRecord toSourceRecord(final JMSContext context, final String topic, final boolean messageBodyJms,
            final Message message) throws JMSException {
        return toSourceRecord(context, topic, messageBodyJms, message, null, null);
    }

    @Override
    public SourceRecord toSourceRecord(final JMSContext context, final String topic, final boolean messageBodyJms,
            final Message message, final Map<String, Long> sourceOffset, final Map<String, String> sourceQueuePartition)
            throws JMSException {

        SchemaAndValue key = new SchemaAndValue(null, null);

        try {
            // Extract key and value
            final SchemaAndValue value = this.getValue(context, topic, messageBodyJms, message);
            key = this.getKey(context, topic, message);

            // Create and return appropriate record based on configuration
            if (copyJmsPropertiesFlag && messageBodyJms) {
                return new SourceRecord(
                        sourceQueuePartition,
                        sourceOffset,
                        topic,
                        null,
                        key.schema(),
                        key.value(),
                        value.schema(),
                        value.value(),
                        message.getJMSTimestamp(),
                        jmsToKafkaHeaderConverter.convertJmsPropertiesToKafkaHeaders(message));
            } else {
                return new SourceRecord(
                        sourceQueuePartition,
                        sourceOffset,
                        topic,
                        null,
                        key.schema(),
                        key.value(),
                        value.schema(),
                        value.value());
            }
        } catch (final Exception e) {
            // Log the error
            logError(e, topic, message);

            // If errors are not tolerated, rethrow
            if (!tolerateErrors) {
                throw e;
            }

            // Handle the error based on configured error tolerance
            return handleBuildException(message, sourceQueuePartition, sourceOffset, topic, key, e);
        }
    }

    /**
     * Logs error based on `errors.log.enable` and `errors.log.include.messages` configurations.
     *
     * @param exception       The exception that needs to be logged.
     * @param topic           The Kafka topic associated with the message.
     * @param message         The JMS message that caused the error.
     */
    private void logError(final Exception exception, final String topic, final Message message) {
        if (logErrors) {
            if (logIncludeMessages) {
                log.error("Failed to process message on topic '{}'. Message content: {}. \nException: {}",
                        topic, message, exception.toString(), exception);
            } else {
                log.error("Failed to process message on topic '{}'. \nException: {}", topic, exception.toString(), exception);
            }
        } else {
            log.warn("Error during message processing on topic '{}', but logging is suppressed. \nReason: {}",
                    topic, extractReason(exception));
        }
    }

    private String extractReason(final Exception exception) {
        if (exception == null) {
            return "Unknown error";
        }

        final String message = exception.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return "Unknown error";
        }

        // Clean up trailing punctuation or whitespace (e.g., "error:" → "error")
        return message.replaceAll("[:\\s]+$", "");
    }


    /**
     *
     * Handles conversion errors based on configuration
     *
     * @param message              The actual MQ message
     * @param sourceQueuePartition The Source Record queue partition
     * @param sourceOffset         The Source Record offset
     * @param originalTopic        The original topic name
     * @param key                  The SchemaAndValue to include in the source
     *                             record key
     * @param exception            The exception that needs to be stored in the
     *                             header
     * @return SourceRecord
     */
    private SourceRecord handleBuildException(final Message message, final Map<String, String> sourceQueuePartition,
            final Map<String, Long> sourceOffset, final String topic, final SchemaAndValue key,
            final Exception exception) {

        // If errors are tolerated but no DLQ is configured, skip the message
        if (dlqTopic == null) {
            log.debug(
                    "Skipping message due to conversion error: error tolerance is enabled but DLQ is not configured. Message will not be processed further.");
            return null;
        }

        // Create DLQ record
        return createDlqRecord(message, sourceQueuePartition, sourceOffset, topic, key, exception);
    }

    /**
     *
     * Creates a DLQ record with error information
     *
     * @param message              The actual MQ message
     * @param sourceQueuePartition The Source Record queue partition
     * @param sourceOffset         The Source Record offset
     * @param originalTopic        The original topic name
     * @param key                  The SchemaAndValue to include in the source
     *                             record key
     * @param exception            The exception that needs to be stored in the
     *                             header
     * @return SourceRecord
     */
    private SourceRecord createDlqRecord(final Message message, final Map<String, String> sourceQueuePartition,
            final Map<String, Long> sourceOffset, final String originalTopic,
            final SchemaAndValue key, final Exception exception) {

        try {
            // Extract payload or return null if extraction fails
            final Optional<byte[]> maybePayload = extractPayload(message);
            if (!maybePayload.isPresent()) {
                log.error("Skipping message due to payload extraction failure");
                return null;
            }

            final byte[] payload = maybePayload.get();

            // Create headers with error information
            final Headers headers = createErrorHeaders(message, originalTopic, exception);

            return new SourceRecord(
                    sourceQueuePartition,
                    sourceOffset,
                    dlqTopic,
                    null,
                    key.schema(),
                    key.value(),
                    Schema.OPTIONAL_BYTES_SCHEMA,
                    payload,
                    message.getJMSTimestamp(),
                    headers);
        } catch (final Exception dlqException) {
            // If DLQ processing itself fails, log and skip
            log.error("Failed to create DLQ record: {}", dlqException.getMessage(), dlqException);
            return null;
        }
    }

    /**
     *
     * Extracts payload from a JMS message with improved error handling
     *
     * @param message The actual message coming from mq
     *
     * @return Optional<byte[]>
     */
    private Optional<byte[]> extractPayload(final Message message) {
        try {
            if (message instanceof BytesMessage) {
                log.debug("Extracting payload from BytesMessage for DLQ");
                return Optional.ofNullable(message.getBody(byte[].class));
            } else if (message instanceof TextMessage) {
                log.debug("Extracting payload from TextMessage for DLQ");
                final String text = message.getBody(String.class);
                return Optional.ofNullable(text != null ? text.getBytes(UTF_8) : null);
            } else {
                log.warn("Unsupported JMS message type '{}' encountered while extracting payload for DLQ. Falling back to message.toString().",
                        message.getClass().getName());
                return Optional.ofNullable(message.toString().getBytes(UTF_8));
            }
        } catch (final JMSException e) {
            log.error("JMSException while extracting payload from message type '{}': {} for DLQ. Falling back to message.toString().",
                    message.getClass().getName(), e.getMessage(), e);
            return Optional.ofNullable(message.toString().getBytes(UTF_8));
        }
    }


    /**
     *
     * Creates enhanced headers with error information for DLQ records
     * @param message       The orginal message
     *
     * @param originalTopic The original topic name
     * @param exception     The execption that needs to be included in the header
     *
     * @return Headers
     */
    private Headers createErrorHeaders(final Message message, final String originalTopic, final Exception exception) {
        Headers headers = new ConnectHeaders();
        if (copyJmsPropertiesFlag) {
            headers = jmsToKafkaHeaderConverter.convertJmsPropertiesToKafkaHeaders(message);
        }

        // Basic error information
        headers.addString(DeadLetterQueueReporter.ERROR_HEADER_ORIG_TOPIC, originalTopic);
        headers.addString(DeadLetterQueueReporter.ERROR_HEADER_EXECUTING_CLASS, exception.getClass().getName());
        headers.addString(DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION_MESSAGE, exception.getMessage());
        headers.addLong(ERROR_HEADER_EXCEPTION_TIMESTAMP, System.currentTimeMillis());

        // Add cause if available
        if (exception.getCause() != null) {
            headers.addString(ERROR_HEADER_EXCEPTION_CAUSE_MESSAGE, exception.getCause().getMessage());
            headers.addString(ERROR_HEADER_EXCEPTION_CAUSE_CLASS, exception.getCause().getClass().getName());
        }

        // Add first few lines of stack trace (full stack trace might be too large)
        headers.addString(DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION_STACK_TRACE, stacktrace(exception));

        return headers;
    }

    private String stacktrace(final Exception exception) {
        try {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            exception.printStackTrace(pw);
            final String stackTrace = sw.toString();

            // First 500 characters or less to avoid overly large headers
            final String truncatedStackTrace = stackTrace.length() <= 500 ? stackTrace
                    : stackTrace.substring(0, 500) + "... [truncated]";
            return truncatedStackTrace;
        } catch (final Exception e) {
            log.warn("Could not add stack trace to DLQ headers", e);
        }
        return null;
    }
}