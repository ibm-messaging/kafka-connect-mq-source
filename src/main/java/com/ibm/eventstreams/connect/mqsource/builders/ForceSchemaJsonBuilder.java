package com.ibm.eventstreams.connect.mqsource.builders;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.TextMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * JSON Builder that forces schema for messages
 * Schema is resolved according to destination topic name
 */
public class ForceSchemaJsonBuilder extends BaseRecordBuilder {
    private static final Logger log = LoggerFactory.getLogger(ForceSchemaJsonBuilder.class);

    private JsonConverter converter;

    private ObjectNode envelope;
    private static ObjectMapper MAPPER = new ObjectMapper();
    private String registryURL;

    public ForceSchemaJsonBuilder() {
        log.info("Building records using com.ibm.eventstreams.connect.mqsource.builders.ForceSchemaJsonBuilder");
        converter = new JsonConverter();
        HashMap<String, String> m = new HashMap<>();
        m.put("schemas.enable", "true");
        converter.configure(m, false);
    }

    @Override
    public void configure(Map<String, String> props) {
        super.configure(props);
        registryURL = props.get("value.converter.schema.registry.url");
    }

    /**
     * Extracts JSON value from JMS, wraps with schema according to topic and deserializes into connect data struct
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
        if (envelope == null) {
            envelope = resolveSchemaEnvelope(topic);
            log.debug("JSON Schema resolved for topic {}", topic);
        }
        String payload = extractPayload(message);
        envelope.putRawValue("payload", new RawValue(payload));
        return tryConvertToJSON(topic);
    }

    private String extractPayload(Message message) throws JMSException {
        if (message instanceof BytesMessage) {
            return new String(message.getBody(byte[].class), StandardCharsets.UTF_8);
        } else if (message instanceof TextMessage) {
            return message.getBody(String.class);
        } else {
            log.error("Unsupported JMS message type {}", message.getClass());
            throw new ConnectException("Unsupported JMS message type");
        }
    }

    private SchemaAndValue tryConvertToJSON(String topic) throws MessageFormatException {
        try {
            byte[] jsonBytes = MAPPER.writeValueAsBytes(envelope);
            return converter.toConnectData(topic, jsonBytes);
        } catch (JsonProcessingException exception) {
            throw new MessageFormatException("Message is not serializable", exception.toString());
        }
    }

    /**
     * Creates simple JSON envelope with schema, but without payload
     */
    private ObjectNode resolveSchemaEnvelope(String topic) throws JMSException {
        try {
            CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(registryURL, 1);
            String schemaFullName = topic + "-value";
            SchemaMetadata schemaMetadata = cachedSchemaRegistryClient.getLatestSchemaMetadata(schemaFullName);
            org.apache.avro.Schema bySubjectAndID = cachedSchemaRegistryClient
                    .getBySubjectAndId(schemaMetadata.getSchema(), schemaMetadata.getId());
            Schema schema = new AvroData(1).toConnectSchema(bySubjectAndID);
            return JsonSchema.envelope(converter.asJsonSchema(schema), NullNode.getInstance());
        } catch (IOException | RestClientException exception) {
            // Feels like I'm in Go Lang where I can't wrap exceptions
            throw new SchemaNotResolvableException("Can't resolve schema for message", exception.toString());
        }
    }
}
