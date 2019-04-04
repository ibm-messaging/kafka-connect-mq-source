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
package com.ibm.eventstreams.connect.mqsource.builders;

import com.ibm.eventstreams.connect.mqsource.MQSourceConnector;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Builds Kafka Connect SourceRecords from messages. It parses the bytes of the payload of JMS
 * BytesMessage and TextMessage as JSON and creates a SourceRecord with a null schema.
 */
public class AvroRecordBuilder extends BaseRecordBuilder {
    private static final Logger log = LoggerFactory.getLogger(AvroRecordBuilder.class);
    private static final Supplier<RuntimeException> SCHEMA_REGISTRY_MISSING_MESSAGE = () -> new ConnectException("Schema Registry URL is missing. Please provide it.");

    public AvroConverter converter;
    public JsonConverter jsonConverter;

    public AvroRecordBuilder() {
        log.info("Building records using com.ibm.eventstreams.connect.mqsource.builders.AvroConverter");
    }

    @Override
    public void configure(Map<String, String> props) {
        final String urls = props.get(MQSourceConnector.CONFIG_SCHEMA_REGISTRY_URLS);
        final List<String> urlList = Optional.ofNullable(urls)
                .filter(s -> !s.isEmpty())
                .map(optUrl -> urls.split(","))
                .map(Arrays::asList)
                .orElseThrow(SCHEMA_REGISTRY_MISSING_MESSAGE);
        converter = new AvroConverter(new CachedSchemaRegistryClient(urlList, 100));
        converter.configure(props,false);
        super.configure(props);
    }

    /**
     * Gets the value schema to use for the Kafka Connect SourceRecord.
     *
     * @param context        the JMS context to use for building messages
     * @param topic          the Kafka topic
     * @param messageBodyJms whether to interpret MQ messages as JMS messages
     * @param message        the message
     * @return the Kafka Connect SourceRecord's value
     * @throws JMSException Message could not be converted
     */
    @Override
    SchemaAndValue getValue(JMSContext context, String topic, boolean messageBodyJms, Message message) throws JMSException {
        byte[] payload;

        if (message instanceof BytesMessage) {
            log.info("I am in bytes");
            payload = message.getBody(byte[].class);
        } else if (message instanceof TextMessage) {
            String s = message.getBody(String.class);
            log.info("MESSAGE RECEIVED ---------> " + s);
            payload = s.getBytes(UTF_8);
        } else {
            log.error("Unsupported JMS message type {}", message.getClass());
            throw new ConnectException("Unsupported JMS message type");
        }
        log.info(" PAYLOAD ------------> " + new String(payload));
        return converter.toConnectData(topic, payload);
    }
}