package com.ibm.eventstreams.connect.mqsource.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.jms.BytesMessage;
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
 * Handles error processing and Dead Letter Queue (DLQ) functionality for MQ
 * Source Connector.
 */
public class ErrorHandler {
    private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

    public static final String HEADER_PREFIX = "__connect.errors.";
    public static final String ERROR_HEADER_ORIG_TOPIC = HEADER_PREFIX + "topic";
    public static final String ERROR_HEADER_ORIG_PARTITION = HEADER_PREFIX + "partition";
    public static final String ERROR_HEADER_ORIG_OFFSET = HEADER_PREFIX + "offset";
    public static final String ERROR_HEADER_CONNECTOR_NAME = HEADER_PREFIX + "connector.name";
    public static final String ERROR_HEADER_TASK_ID = HEADER_PREFIX + "task.id";
    public static final String ERROR_HEADER_STAGE = HEADER_PREFIX + "stage";
    public static final String ERROR_HEADER_EXECUTING_CLASS = HEADER_PREFIX + "class.name";
    public static final String ERROR_HEADER_EXCEPTION = HEADER_PREFIX + "exception.class.name";
    public static final String ERROR_HEADER_EXCEPTION_MESSAGE = HEADER_PREFIX + "exception.message";
    public static final String ERROR_HEADER_EXCEPTION_STACK_TRACE = HEADER_PREFIX + "exception.stacktrace";
    public static final String ERROR_HEADER_EXCEPTION_TIMESTAMP = HEADER_PREFIX + "timestamp";
    public static final String ERROR_HEADER_EXCEPTION_CAUSE_CLASS = HEADER_PREFIX + "cause.class";
    public static final String ERROR_HEADER_EXCEPTION_CAUSE_MESSAGE = HEADER_PREFIX + "cause.message";
    public static final String ERROR_HEADER_JMS_MESSAGE_ID = HEADER_PREFIX + "jms.message.id";
    public static final String ERROR_HEADER_JMS_TIMESTAMP = HEADER_PREFIX + "jms.timestamp";
    public static final String ERROR_HEADER_QUEUE = HEADER_PREFIX + "mq.queue";

    private boolean tolerateErrors;
    private boolean logErrors;
    private boolean logIncludeMessages;
    private String dlqTopic;
    private String queueName;
    private boolean copyJmsPropertiesFlag;
    private JmsToKafkaHeaderConverter jmsToKafkaHeaderConverter;
    private boolean enableDLQContextHeader;

    /**
     * Configure the error handler with the provided properties.
     *
     * @param props                     Configuration properties
     * @param copyJmsPropertiesFlag     Whether to copy JMS properties to Kafka
     *                                  headers
     * @param jmsToKafkaHeaderConverter Converter for JMS properties to Kafka
     *                                  headers
     */
    public void configure(final Map<String, String> props, final boolean copyJmsPropertiesFlag,
            final JmsToKafkaHeaderConverter jmsToKafkaHeaderConverter) {
        log.trace("[{}] Entry {}.configure", Thread.currentThread().getId(), this.getClass().getName());

        this.copyJmsPropertiesFlag = copyJmsPropertiesFlag;
        this.jmsToKafkaHeaderConverter = jmsToKafkaHeaderConverter;

        initializeErrorTolerance(props);

        log.trace("[{}] Exit {}.configure", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Initializes error tolerance configuration by reading directly from properties
     * map.
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

            final String enableDLQContextHeaderValue = props.getOrDefault(
                    MQSourceConnector.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG,
                    String.valueOf(MQSourceConnector.DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT));
            enableDLQContextHeader = Boolean.parseBoolean(enableDLQContextHeaderValue);

            dlqTopic = props.get(MQSourceConnector.DLQ_TOPIC_NAME_CONFIG);
            if (dlqTopic != null && !dlqTopic.isEmpty()) {
                dlqTopic = dlqTopic.trim();
                // TODO: Check if DLQ topic exists
            }

            queueName = props.get(MQSourceConnector.CONFIG_NAME_MQ_QUEUE);
            if (queueName != null && !queueName.isEmpty()) {
                queueName = queueName.trim();
            }
        } else {
            logErrors = false;
            logIncludeMessages = false;
        }
    }

    /**
     * Checks if errors should be tolerated based on configuration.
     *
     * @return true if errors should be tolerated, false otherwise
     */
    public boolean shouldTolerateErrors() {
        return tolerateErrors;
    }

    /**
     * Logs error based on configuration settings.
     *
     * @param exception The exception that needs to be logged
     * @param topic     The Kafka topic associated with the message
     * @param message   The JMS message that caused the error
     */
    public void logError(final Exception exception, final String topic, final Message message) {
        if (logErrors) {
            if (logIncludeMessages) {
                log.error("Failed to process message on topic '{}'. Message content: {}. \nException: {}",
                        topic, message, exception.toString(), exception);
            } else {
                log.error("Failed to process message on topic '{}'. \nException: {}",
                        topic, exception.toString(), exception);
            }
        } else {
            log.warn("Error during message processing on topic '{}', but logging is suppressed. \nReason: {}",
                    topic, extractReason(exception));
        }
    }

    /**
     * Handles conversion errors based on configuration.
     *
     * @param message              The actual MQ message
     * @param sourceQueuePartition The Source Record queue partition
     * @param sourceOffset         The Source Record offset
     * @param topic                The original topic name
     * @param key                  The SchemaAndValue to include in the source
     *                             record key
     * @param exception            The exception that needs to be stored in the
     *                             header
     * @return SourceRecord for DLQ or null if message should be skipped
     */
    public SourceRecord handleBuildException(final Message message, final Map<String, String> sourceQueuePartition,
            final Map<String, Long> sourceOffset, final String topic,
            final SchemaAndValue key, final Exception exception) {

        // If errors are tolerated but no DLQ is configured, skip the message
        if (dlqTopic == null || dlqTopic.isEmpty()) {
            log.debug(
                    "Skipping message due to conversion error: error tolerance is enabled but DLQ is not configured. Message will not be processed further.");
            return null;
        }

        // Create DLQ record
        return createDlqRecord(message, sourceQueuePartition, sourceOffset, topic, key, exception);
    }

    /**
     * Creates a DLQ record with error information.
     *
     * @param message              The actual MQ message
     * @param sourceQueuePartition The Source Record queue partition
     * @param sourceOffset         The Source Record offset
     * @param originalTopic        The original topic name
     * @param key                  The SchemaAndValue to include in the source
     *                             record key
     * @param exception            The exception that needs to be stored in the
     *                             header
     * @return SourceRecord for DLQ or null if creation fails
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

            // Create headers with error information, if DLQ context header config is enabled
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
     * Extracts payload from a JMS message with improved error handling.
     *
     * @param message The actual message coming from MQ
     * @return Optional<byte[]> containing the payload or empty if extraction fails
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
                log.warn(
                        "Unsupported JMS message type '{}' encountered while extracting payload for DLQ. Falling back to message.toString().",
                        message.getClass().getName());
                return Optional.ofNullable(message.toString().getBytes(UTF_8));
            }
        } catch (final JMSException e) {
            log.error(
                    "JMSException while extracting payload from message type '{}': {} for DLQ. Falling back to message.toString().",
                    message.getClass().getName(), e.getMessage(), e);
            return Optional.ofNullable(message.toString().getBytes(UTF_8));
        }
    }

    /**
     * Creates enhanced headers with error information for DLQ records.
     *
     * @param message       The original message
     * @param originalTopic The original topic name
     * @param exception     The exception that needs to be included in the header
     * @return Headers containing error information
     */
    private Headers createErrorHeaders(final Message message, final String originalTopic, final Exception exception) {
        Headers headers = new ConnectHeaders();
        if (copyJmsPropertiesFlag && jmsToKafkaHeaderConverter != null) {
            headers = jmsToKafkaHeaderConverter.convertJmsPropertiesToKafkaHeaders(message);
        }

        // if DLQ context header is not enabled continue with JMS propery headers
        if (!enableDLQContextHeader) {
            return headers;
        }

        // Basic error information
        headers.addString(DeadLetterQueueReporter.ERROR_HEADER_ORIG_TOPIC, originalTopic);
        headers.addString(DeadLetterQueueReporter.ERROR_HEADER_EXECUTING_CLASS, exception.getClass().getName());
        headers.addString(DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION_MESSAGE, exception.getMessage());

        try {
            headers.addString(ERROR_HEADER_JMS_MESSAGE_ID, message.getJMSMessageID());
            headers.addLong(ERROR_HEADER_JMS_TIMESTAMP, message.getJMSTimestamp());
        } catch (final JMSException jmsException) {
            log.warn("Failed to extract JMS message ID or timestamp for DLQ headers", jmsException);
        }

        headers.addString(ERROR_HEADER_QUEUE, queueName != null ? queueName : "");
        headers.addLong(ERROR_HEADER_EXCEPTION_TIMESTAMP, System.currentTimeMillis());

        // Add cause if available
        if (exception.getCause() != null) {
            headers.addString(ERROR_HEADER_EXCEPTION_CAUSE_MESSAGE, exception.getCause().getMessage());
            headers.addString(ERROR_HEADER_EXCEPTION_CAUSE_CLASS, exception.getCause().getClass().getName());
        }

        // Add first few lines of stack trace (full stack trace might be too large)
        final String stackTrace = getStackTrace(exception);
        if (stackTrace != null) {
            headers.addString(DeadLetterQueueReporter.ERROR_HEADER_EXCEPTION_STACK_TRACE, stackTrace);
        }

        return headers;
    }

    /**
     * Extracts and truncates stack trace from exception.
     *
     * @param exception The exception to extract stack trace from
     * @return Truncated stack trace string or null if extraction fails
     */
    private String getStackTrace(final Exception exception) {
        try {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            exception.printStackTrace(pw);
            final String stackTrace = sw.toString();

            // First 500 characters or less to avoid overly large headers
            return stackTrace.length() <= 500 ? stackTrace
                    : stackTrace.substring(0, 500) + "... [truncated]";
        } catch (final Exception e) {
            log.warn("Could not extract stack trace for DLQ headers", e);
            return null;
        }
    }

    /**
     * Extracts a clean reason message from an exception.
     *
     * @param exception The exception to extract reason from
     * @return Clean reason string
     */
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
}