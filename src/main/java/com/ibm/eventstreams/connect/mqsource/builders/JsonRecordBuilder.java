/**
 * Copyright 2017, 2018, 2019, 2023, 2024, 2025 IBM Corporation
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import javax.jms.BytesMessage;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds Kafka Connect SourceRecords from messages. It parses the bytes of the payload of JMS
 * BytesMessage and TextMessage as JSON and creates a SourceRecord with a null schema.
 *
 * When messageBodyJms is false, this builder can handle MQ messages with RFH2 headers by
 * automatically detecting and skipping them to extract the JSON payload.
 */
public class JsonRecordBuilder extends BaseRecordBuilder {
    private static final Logger log = LoggerFactory.getLogger(JsonRecordBuilder.class);

    private JsonConverter converter;

    // RFH2 header constants
    private static final String RFH2_STRUC_ID = "RFH ";
    private static final int RFH2_STRUCT_ID_LENGTH = 4;
    private static final int RFH2_STRUC_LENGTH = 4;
    private static final int RFH2_STRUC_LENGTH_OFFSET = RFH2_STRUCT_ID_LENGTH + RFH2_STRUC_LENGTH;
    private static final int RFH2_MIN_HEADER_SIZE = 36;

    public JsonRecordBuilder() {
        log.info("Building records using com.ibm.eventstreams.connect.mqsource.builders.JsonRecordBuilder");
        converter = new JsonConverter();

        // We just want the payload, not the schema in the output message
        final HashMap<String, String> m = new HashMap<>();
        m.put("schemas.enable", "false");

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
        byte[] payload;

        if (message instanceof BytesMessage) {
            payload = message.getBody(byte[].class);
        } else if (message instanceof TextMessage) {
            final String s = message.getBody(String.class);
            payload = s.getBytes(UTF_8);
        } else {
            log.error("Unsupported JMS message type {}", message.getClass());
            throw new RecordBuilderException("Unsupported JMS message type");
        }

        // When messageBodyJms is false, the message may contain RFH2 headers that need to be skipped
        if (!messageBodyJms) {
            payload = stripRFH2Header(payload);
        }
        return converter.toConnectData(topic, payload);
    }

    /**
     * Skips RFH2 (Rules and Formatting Header version 2) if present in the payload.
     * RFH2 headers are used by IBM MQ to carry additional message properties and metadata.
     *
     * When messageBodyJms is false (WMQ_MESSAGE_BODY_MQ), JMS does not automatically
     * strip RFH2 headers, so we need to parse and skip them manually.
     *
     * RFH2 structure:
     * - StrucId (4 bytes): "RFH " (with trailing space)
     * - Version (4 bytes): Version number (typically 2)
     * - StrucLength (4 bytes): Total length of RFH2 header including all folders
     * - Encoding (4 bytes): Numeric encoding
     * - CodedCharSetId (4 bytes): Character set identifier
     * - Format (8 bytes): Format name of data following the header
     * - Flags (4 bytes): Flags
     * - NameValueCCSID (4 bytes): CCSID of name-value data
     * - Variable length folders containing name-value pairs
     * 
     * Inspired from https://github.com/CommunityHiQ/Frends.Community.IBMMQ/blob/master/Frends.Community.IBMMQ/Helpers/IBMMQHelpers.cs
     * Header structure https://www.ibm.com/docs/en/integration-bus/10.0.0?topic=header-mqrfh2-structure
     *
     * @param payload the original message payload
     * @return the payload with RFH2 header removed if present, otherwise the original payload
     */
    private byte[] stripRFH2Header(final byte[] payload) {
        if (payload == null || payload.length < RFH2_MIN_HEADER_SIZE) {
            return payload;
        }

        // Check if the message starts with RFH2 structure ID
        final String strucId = new String(payload, 0, RFH2_STRUCT_ID_LENGTH, UTF_8);
        if (!RFH2_STRUC_ID.equals(strucId)) {
            log.debug("No RFH2 header detected");
            return payload;
        }

        try {
            // Read version to detect endianness
            final ByteBuffer buffer = ByteBuffer.wrap(payload);
            buffer.order(ByteOrder.LITTLE_ENDIAN); // Default to little-endian
            buffer.position(RFH2_STRUCT_ID_LENGTH); // Skip StrucId (4 bytes)
            int version = buffer.getInt();
            
            // Detect endianness: if version is not 1 or 2, it's likely big-endian
            if (version > 2 || version < 1) {
                version = Integer.reverseBytes(version);
                buffer.order(ByteOrder.BIG_ENDIAN);
                log.debug("Detected big-endian RFH2 header");
            } else {
                log.debug("Detected little-endian RFH2 header");
            }
            
            // Read the RFH2 structure length, Skip StrucId (4 bytes) and Version (4 bytes)
            buffer.position(RFH2_STRUC_LENGTH_OFFSET);
            final int strucLength = buffer.getInt();

            if (strucLength < RFH2_MIN_HEADER_SIZE || strucLength > payload.length) {
                log.warn("Invalid RFH2 structure length: {}. Treating entire payload as message.", strucLength);
                return payload;
            }

            log.debug("RFH2 header detected (version: {}, length: {} bytes). Stripping header.", version, strucLength);

            // Extract the actual message payload after the RFH2 header
            final byte[] actualPayload = new byte[payload.length - strucLength];
            System.arraycopy(payload, strucLength, actualPayload, 0, actualPayload.length);

            return actualPayload;

        } catch (final Exception e) {
            log.error("Error parsing RFH2 header: {}. Returning original payload.", e.getMessage());
            return payload;
        }
    }
}