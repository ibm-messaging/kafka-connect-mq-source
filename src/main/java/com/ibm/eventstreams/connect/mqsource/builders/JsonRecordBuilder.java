/**
 * Copyright 2017, 2018, 2019, 2023, 2024 IBM Corporation
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

import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.eventstreams.connect.mqsource.MQSourceConnector;

/**
 * Builds Kafka Connect SourceRecords from messages. It parses the bytes of the payload of JMS
 * BytesMessage and TextMessage as JSON and creates a SourceRecord with a null schema.
 */
public class JsonRecordBuilder extends BaseRecordBuilder {
    private static final Logger log = LoggerFactory.getLogger(JsonRecordBuilder.class);

    private JsonConverter converter;

    // From Kafka Connect 4.2 onwards, JsonConverter includes schema support
    //  To support earlier versions of the dependency, the record builder includes a
    //  workaround implementation.
    //  This variable should be true where the workaround implementation is required.
    private boolean recordBuilderSchemaSupport = false;

    // Workaround for supporting schemas is to embed the schema in the message payload
    //  given to the JsonConverter. This variable contains a String to concatenate with
    //  the string received from MQ in order to achieve this.
    private String schemaSupportEnvelope = null;

    public JsonRecordBuilder() {
        log.info("Building records using com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");
        converter = new JsonConverter();
    }

    /**
     * Configure this class. In addition to the MQ message handling config
     *  used by BaseRecordBuilder, this also configures the JsonConverter
     *  used by this record builder to parse JSON messages from MQ.
     */
    @Override
    public void configure(final Map<String, String> props) {
        super.configure(props);

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, props);
        final boolean schemasEnable = config.getBoolean(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER_JSON_SCHEMAS_ENABLE);
        String schemaContent = null;
        if (schemasEnable) {
            schemaContent = config.getString(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER_JSON_SCHEMA_CONTENT);
            if (schemaContent != null) {
                schemaContent = schemaContent.trim();
            }
        }

        if (schemasEnable && schemaContent != null &&
            !JsonConverterConfig.configDef().names().contains("schema.content")) {

            // support for schemas provided separately from message payloads is requested
            //  but not available natively within the JsonConverter present in the classpath
            recordBuilderSchemaSupport = true;
            schemaSupportEnvelope = "{\"schema\": " + schemaContent + ", \"payload\": ";
        }

        final Map<String, String> m = new HashMap<>();
        m.put("schemas.enable", Boolean.toString(schemasEnable));
        m.put("schema.content", schemaContent);

        // Convert the value, not the key (isKey == false)
        converter.configure(m, false);
    }

    /**
     * Gets the value schema to use for the Kafka Connect SourceRecord.
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
    @Override
    public SchemaAndValue getValue(final JMSContext context, final String topic, final boolean messageBodyJms,
            final Message message) throws JMSException {
        final byte[] payload;

        if (message instanceof BytesMessage) {
            payload = message.getBody(byte[].class);
        } else if (message instanceof TextMessage) {
            final String s = message.getBody(String.class);
            payload = s.getBytes(UTF_8);
        } else {
            log.error("Unsupported JMS message type {}", message.getClass());
            throw new RecordBuilderException("Unsupported JMS message type");
        }

        if (recordBuilderSchemaSupport) {
            return converter.toConnectData(topic,
                // embed schema in the event payload
                (schemaSupportEnvelope + new String(payload) + "}").getBytes());
        } else {
            return converter.toConnectData(topic,
                // submit the payload as-is to the converter
                payload);
        }
    }
}