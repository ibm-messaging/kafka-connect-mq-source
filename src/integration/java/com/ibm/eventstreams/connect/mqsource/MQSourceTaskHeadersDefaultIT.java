/**
 * Copyright 2026 IBM Corporation
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

import static com.ibm.eventstreams.connect.mqsource.MQSourceTaskObjectMother.getSourceTaskWithEmptyKafkaOffset;
import static com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil.putAllMessagesToQueue;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.TextMessage;

import com.ibm.msg.client.jms.JmsConstants;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder;
import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;
import com.ibm.eventstreams.connect.mqsource.utils.SourceTaskStopper;

/**
 * Integration tests for copying JMS message properties to Kafka headers.
 *
 * Verifies that:
 *  - byte-array properties (MQMD fields such as GroupId and AccountingToken)
 *    are stored as binary BYTES headers rather than object-reference strings
 *    (e.g. "[B@5cde362e").
 *  - MsgId and CorrelId byte-array fields are returned as "ID:"-prefixed hex
 *    strings via the JMS API.
 *  - all other property types (String, int, long, float, double, boolean,
 *    byte, short) are serialised as their string representations.
 */
public class MQSourceTaskHeadersDefaultIT extends AbstractJMSContextIT {

    private static final String TOPIC = "mytopic";

    private static final byte[] TEST_CORREL_ID = new byte[] {
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18
    };

    private MQSourceTask connectTask = null;
    private HeaderConverter converter;

    @Before
    public void before() throws Exception {
        MQTestUtil.removeAllMessagesFromQueue(DEFAULT_SOURCE_QUEUE);
        converter = new SimpleHeaderConverter();

        final Map<String, String> converterConfig = new HashMap<>();
        converter.configure(converterConfig);
    }

    @After
    public void after() throws InterruptedException {
        final SourceTaskStopper stopper = new SourceTaskStopper(connectTask);
        stopper.run();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void startTask(final boolean mqmdRead) {
        connectTask = getSourceTaskWithEmptyKafkaOffset();

        final Map<String, String> props = new HashMap<>();
        props.put("mq.queue.manager", QMGR_NAME);
        props.put("mq.connection.mode", "client");
        props.put("mq.connection.name.list", DEFAULT_CONNECTION_NAME);
        props.put("mq.channel.name", CHANNEL_NAME);
        props.put("mq.queue", DEFAULT_SOURCE_QUEUE);
        props.put("mq.user.authentication.mqcsp", "false");
        props.put("topic", TOPIC);
        props.put("mq.message.receive.timeout", "5000");
        props.put("mq.receive.subsequent.timeout.ms", "2000");
        props.put("mq.reconnect.delay.min.ms", "100");
        props.put("mq.reconnect.delay.max.ms", "10000");
        props.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_BODY_JMS, "true");
        props.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER, DefaultRecordBuilder.class.getCanonicalName());
        props.put(MQSourceConnector.CONFIG_NAME_MQ_JMS_PROPERTY_COPY_TO_KAFKA_HEADER, "true");
        props.put(MQSourceConnector.CONFIG_NAME_MQ_MESSAGE_MQMD_READ, Boolean.toString(mqmdRead));

        connectTask.start(props);
    }

    private Headers pollHeaders(final TextMessage message) throws Exception {
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));
        final List<SourceRecord> records = connectTask.poll();
        assertThat(records).hasSize(1);
        return records.get(0).headers();
    }

    /**
     * Serialises the named header through SimpleHeaderConverter and returns the
     * result decoded as a UTF-8 string.
     */
    private String getHeaderAsString(final Headers headers, final String headerName) {
        final Header h = headers.lastWithName(headerName);
        assertThat(h).as("header '%s' should be present", headerName).isNotNull();
        final byte[] wire = converter.fromConnectHeader(TOPIC, headerName, h.schema(), h.value());
        assertThat(wire).isNotNull();
        return new String(wire, StandardCharsets.UTF_8);
    }

    /**
     * Serialises the named header through SimpleHeaderConverter and returns the
     * raw bytes.
     */
    private byte[] getHeaderAsBytes(final Headers headers, final String headerName) {
        final Header h = headers.lastWithName(headerName);
        assertThat(h).as("header '%s' should be present", headerName).isNotNull();
        return converter.fromConnectHeader(TOPIC, headerName, h.schema(), h.value());
    }


    // =========================================================================
    // User-defined message properties — all should serialise as strings
    // =========================================================================

    @Test
    public void userDefinedStringProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setStringProperty("strProp", "hello");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, "strProp")).isEqualTo("hello");
    }

    @Test
    public void userDefinedIntProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setIntProperty("intProp", 42);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, "intProp")).isEqualTo("42");
    }

    @Test
    public void userDefinedLongProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setLongProperty("longProp", 123456789012345L);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, "longProp")).isEqualTo("123456789012345");
    }

    @Test
    public void userDefinedFloatProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setFloatProperty("floatProp", 1.5f);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, "floatProp")).isEqualTo("1.5");
    }

    @Test
    public void userDefinedDoubleProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setDoubleProperty("doubleProp", 3.141592653589793);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, "doubleProp")).isEqualTo("3.141592653589793");
    }

    @Test
    public void userDefinedBooleanProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setBooleanProperty("boolProp", true);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, "boolProp")).isEqualTo("true");
    }

    @Test
    public void userDefinedByteProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setByteProperty("byteProp", (byte) 64);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, "byteProp")).isEqualTo("64");
    }

    @Test
    public void userDefinedShortProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setShortProperty("shortProp", (short) 1000);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, "shortProp")).isEqualTo("1000");
    }


    // =========================================================================
    // JMSx properties — all should serialise as strings
    // =========================================================================

    @Test
    public void jmsxDeliveryCount() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMSX_DELIVERY_COUNT)).isEqualTo("1");
    }

    @Test
    public void jmsxUserId() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMSX_USERID).trim()).isEqualTo("app");
    }

    @Test
    public void jmsxGroupId() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setStringProperty(JmsConstants.JMSX_GROUPID, "MY_GROUP");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMSX_GROUPID)).isEqualTo("MY_GROUP");
    }

    @Test
    public void jmsxGroupSeq() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setIntProperty(JmsConstants.JMSX_GROUPSEQ, 3);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMSX_GROUPSEQ)).isEqualTo("3");
    }


    // =========================================================================
    // JMS_IBM_* extension properties — all should serialise as strings
    // =========================================================================

    @Test
    public void jmsIbmFormat() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_FORMAT).trim()).isEqualTo("MQSTR");
    }

    @Test
    public void jmsIbmMsgType() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        // a normal datagram sent without a reply-to destination has a type of 8 (MQMT_DATAGRAM)
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_MSGTYPE)).isEqualTo("8");
    }

    @Test
    public void jmsIbmCharacterSet() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_CHARACTER_SET)).isEqualTo("UTF-8");
    }

    @Test
    public void jmsIbmEncoding() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_ENCODING)).isEqualTo("273");
    }

    @Test
    public void jmsIbmPutDate() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        final String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_PUTDATE)).isEqualTo(today);
    }

    @Test
    public void jmsIbmPutTime() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_PUTTIME)).matches("\\d{8}");
    }

    @Test
    public void jmsIbmPutApplType() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_PUTAPPLTYPE)).matches("\\d+");
    }


    // =========================================================================
    // JMS_IBM_MQMD_* properties
    // =========================================================================

    @Test
    public void jmsIbmMqmdPutApplName() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME)).isNotEmpty();
    }

    @Test
    public void jmsIbmMqmdPutDate() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        final String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_MQMD_PUTDATE)).isEqualTo(today);
    }

    @Test
    public void jmsIbmMqmdPutTime() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_MQMD_PUTTIME)).matches("\\d{8}");
    }

    @Test
    public void jmsIbmMqmdBackoutCount() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_MQMD_BACKOUTCOUNT)).isEqualTo("0");
    }

    @Test
    public void jmsIbmMqmdPriority() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setJMSPriority(4);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_MQMD_PRIORITY)).isEqualTo("4");
    }

    @Test
    public void jmsIbmMqmdPersistence() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_MQMD_PERSISTENCE)).isEqualTo("1");
    }

    @Test
    public void jmsIbmMqmdMsgType() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        // a normal datagram sent without a reply-to destination has a type of 8 (MQMT_DATAGRAM)
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_MQMD_MSGTYPE)).isEqualTo("8");
    }

    @Test
    public void jmsIbmMqmdEncoding() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_MQMD_ENCODING)).isEqualTo("273");
    }

    /**
     * Verifies that MsgId is returned as an "ID:"-prefixed hex string rather than
     * a raw byte-array object reference (e.g. "[B@5cde362e").
     */
    @Test
    public void jmsIbmMqmdMsgId() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        // ID will be dynamically generated; verify it is a readable hex string, not an object ref
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_MQMD_MSGID))
            .startsWith("ID:")
            .doesNotContain("[B@")
            .hasSize("ID:".length() + 48);
    }

    /**
     * Verifies that CorrelId is returned as an "ID:"-prefixed lowercase hex string
     * rather than a raw byte-array object reference.
     */
    @Test
    public void jmsIbmMqmdCorrelId() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setJMSCorrelationIDAsBytes(TEST_CORREL_ID);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderAsString(headers, JmsConstants.JMS_IBM_MQMD_CORRELID))
            .doesNotContain("[B@")
            .isEqualTo("ID:0102030405060708090a0b0c0d0e0f101112131415161718");
    }

    /**
     * Verifies that AccountingToken is stored as a binary BYTES header (not an
     * object reference string). SimpleHeaderConverter base64-encodes BYTES headers;
     * a 32-byte value produces a 44-character base64 string.
     */
    @Test
    public void jmsIbmMqmdAccountingToken() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        // SimpleHeaderConverter base64-encodes BYTES values; 32 bytes → 44 base64 chars
        final byte[] wire = getHeaderAsBytes(headers, JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN);
        assertThat(wire).hasSize(44);

        // The wire bytes should be valid base64 encoding of a 32-byte value
        final byte[] decoded = Base64.getDecoder().decode(wire);
        assertThat(decoded).hasSize(32);

        // Crucially, the raw string must NOT look like a Java object reference
        assertThat(new String(wire, StandardCharsets.UTF_8)).doesNotContain("[B@");
    }

    /**
     * Verifies that GroupId is stored as a binary BYTES header (not an object
     * reference string). SimpleHeaderConverter base64-encodes BYTES headers;
     * a 24-byte value produces a 32-character base64 string.
     */
    @Test
    public void jmsIbmMqmdGroupId() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        // SimpleHeaderConverter base64-encodes BYTES values; 24 bytes → 32 base64 chars
        final byte[] wire = getHeaderAsBytes(headers, JmsConstants.JMS_IBM_MQMD_GROUPID);
        assertThat(wire).hasSize(32);

        // The wire bytes should be valid base64 encoding of a 24-byte value
        final byte[] decoded = Base64.getDecoder().decode(wire);
        assertThat(decoded).hasSize(24);

        // Crucially, the raw string must NOT look like a Java object reference
        assertThat(new String(wire, StandardCharsets.UTF_8)).doesNotContain("[B@");
    }
}
