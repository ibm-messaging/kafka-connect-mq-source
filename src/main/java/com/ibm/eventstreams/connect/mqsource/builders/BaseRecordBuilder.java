/**
 * Copyright 2018, 2019 IBM Corporation
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

import com.ibm.eventstreams.connect.mqsource.MQSourceConnector;
import com.ibm.eventstreams.connect.mqsource.processor.JmsToKafkaHeaderConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import java.util.Map;
import java.util.Optional;

/**
 * Builds Kafka Connect SourceRecords from messages.
 */
public abstract class BaseRecordBuilder implements RecordBuilder {
    private static final Logger log = LoggerFactory.getLogger(BaseRecordBuilder.class);

    public enum KeyHeader {NONE, MESSAGE_ID, CORRELATION_ID, CORRELATION_ID_AS_BYTES, DESTINATION};
    protected KeyHeader keyheader = KeyHeader.NONE;


	private boolean copyJmsPropertiesFlag = Boolean.FALSE;
	private JmsToKafkaHeaderConverter jmsToKafkaHeaderConverter;

    /**
     * Configure this class.
     *
     * @param props initial configuration
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    @Override public void configure(Map<String, String> props) {
        log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        String kh = props.get(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER_KEY_HEADER);
        if (kh != null) {
            if (kh.equals(MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSMESSAGEID)) {
                keyheader = KeyHeader.MESSAGE_ID;
                log.debug("Setting Kafka record key from JMSMessageID header field");
            }
            else if (kh.equals(MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSCORRELATIONID)) {
                keyheader = KeyHeader.CORRELATION_ID;
                log.debug("Setting Kafka record key from JMSCorrelationID header field");
            }
            else if (kh.equals(MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSCORRELATIONIDASBYTES)) {
                keyheader = KeyHeader.CORRELATION_ID_AS_BYTES;
                log.debug("Setting Kafka record key from JMSCorrelationIDAsBytes header field");
            }
            else if (kh.equals(MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSDESTINATION)) {
                keyheader = KeyHeader.DESTINATION;
                log.debug("Setting Kafka record key from JMSDestination header field");
            }
            else {
                log.error("Unsupported MQ record builder key header value {}", kh);
                throw new ConnectException("Unsupported MQ record builder key header value");
            }
        }

        String str = props.get(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER);
        copyJmsPropertiesFlag = Boolean.parseBoolean(Optional.ofNullable(str).orElse("false"));
        jmsToKafkaHeaderConverter = new JmsToKafkaHeaderConverter();

		log.trace("[{}]  Exit {}.configure", Thread.currentThread().getId(), this.getClass().getName());
	}

    /**
     * Gets the key to use for the Kafka Connect SourceRecord.
     *
     * @param context            the JMS context to use for building messages
     * @param topic              the Kafka topic
     * @param message            the message
     *
     * @return the Kafka Connect SourceRecord's key
     *
     * @throws JMSException      Message could not be converted
     */
    public SchemaAndValue getKey(JMSContext context, String topic, Message message) throws JMSException {
        Schema keySchema = null;
        Object key = null;
        String keystr;

        switch (keyheader) {
            case MESSAGE_ID:
                keySchema = Schema.OPTIONAL_STRING_SCHEMA;
                keystr = message.getJMSMessageID();
                if (keystr.startsWith("ID:", 0)) {
                    key = keystr.substring(3);
                }
                else {
                    key = keystr;
                }
                break;
            case CORRELATION_ID:
                keySchema = Schema.OPTIONAL_STRING_SCHEMA;
                keystr = message.getJMSCorrelationID();
                if (keystr.startsWith("ID:", 0)) {
                    key = keystr.substring(3);
                }
                else {
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
     * @param context            the JMS context to use for building messages
     * @param topic              the Kafka topic
     * @param messageBodyJms     whether to interpret MQ messages as JMS messages
     * @param message            the message
     *
     * @return the Kafka Connect SourceRecord's value
     *
     * @throws JMSException      Message could not be converted
     */
    public abstract SchemaAndValue getValue(JMSContext context, String topic, boolean messageBodyJms, Message message) throws JMSException;

   /**
     * Convert a message into a Kafka Connect SourceRecord.
     *
     * @param context            the JMS context to use for building messages
     * @param topic              the Kafka topic
     * @param messageBodyJms     whether to interpret MQ messages as JMS messages
     * @param message            the message
     *
     * @return the Kafka Connect SourceRecord
     *
     * @throws JMSException      Message could not be converted
     */
    @Override public SourceRecord toSourceRecord(JMSContext context, String topic, boolean messageBodyJms, Message message) throws JMSException {
        SchemaAndValue key = this.getKey(context, topic, message);
        SchemaAndValue value = this.getValue(context, topic, messageBodyJms, message);

		if (copyJmsPropertiesFlag && messageBodyJms)
		    return new SourceRecord(null, null, topic, (Integer) null, key.schema(), key.value(), value.schema(), value.value(), message.getJMSTimestamp(), jmsToKafkaHeaderConverter.convertJmsPropertiesToKafkaHeaders(message));
        else
		    return new SourceRecord(null, null, topic, key.schema(), key.value(), value.schema(), value.value());
	}
}