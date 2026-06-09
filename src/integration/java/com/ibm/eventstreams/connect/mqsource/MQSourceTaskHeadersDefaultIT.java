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
import java.util.HexFormat;
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
import org.testcontainers.containers.GenericContainer;

import com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder;
import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;
import com.ibm.eventstreams.connect.mqsource.utils.SourceTaskStopper;

/**
 * Integration tests for copying JMS message properties to Kafka headers
 *  using the default Connect header converter.
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

        grantAppUserMqmdPutOnQueue(DEFAULT_SOURCE_QUEUE);
    }

    @After
    public void after() throws InterruptedException {
        final SourceTaskStopper stopper = new SourceTaskStopper(connectTask);
        stopper.run();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Starts the connector task with header-copying enabled.
     *
     * @param mqmdRead true if MQMD fields should also be read
     */
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

    protected void grantAppUserMqmdPutOnQueue(final String queueName) throws Exception {
        grantAppUserMqmdPut(mqContainer, QMGR_NAME, queueName, USERNAME);
    }

    public static void grantAppUserMqmdPut(final GenericContainer<?> mqContainer, final String queueManager,
            final String queueName, final String username) throws Exception {
        mqContainer.execInContainer("setmqaut",
                "-m", queueManager,
                "-n", queueName,
                "-p", username,
                "-t", "queue",
                "+setall", "+get", "+browse", "+put", "+inq");

        mqContainer.execInContainer("setmqaut",
                "-m", queueManager,
                "-p", username,
                "-t", "qmgr",
                "+setall");
    }

    /**
     * Puts a single message to the queue, polls the connector, and 
     * returns the headers from the resulting SourceRecord.
     */
    private Headers pollHeaders(final TextMessage message) throws Exception {
        putAllMessagesToQueue(DEFAULT_SOURCE_QUEUE, Arrays.asList(message));
        final List<SourceRecord> records = connectTask.poll();
        assertThat(records).hasSize(1);
        return records.get(0).headers();
    }

    /**
     * Serialises the named header through SimpleHeaderConverter (as Connect would
     * before writing to the Kafka topic) and returns the result decoded as a UTF-8
     * string.
     */
    private String getHeaderDataAsString(final Headers headers, final String headerName) {
        final Header h = headers.lastWithName(headerName);
        assertThat(h).isNotNull();
        final byte[] wire = converter.fromConnectHeader(TOPIC, headerName, h.schema(), h.value());
        assertThat(wire).isNotNull();
        return new String(wire, StandardCharsets.UTF_8);
    }

    /**
     * Serialises the named header through SimpleHeaderConverter and returns the raw
     * bytes.
     */
    private byte[] getHeaderDataAsBytes(final Headers headers, final String headerName) {
        final Header h = headers.lastWithName(headerName);
        assertThat(h).isNotNull();
        return converter.fromConnectHeader(TOPIC, headerName, h.schema(), h.value());
    }

    /**
     * Gets header data as String, then Base64 decodes it to get the original binary data.
     * This is useful for MQMD properties that are stored as Base64-encoded Strings.
     */
    private byte[] getHeaderDataAsBase64DecodedBytes(final Headers headers, final String headerName) {
        final String base64String = getHeaderDataAsString(headers, headerName);
        return Base64.getDecoder().decode(base64String);
    }

    /**
     * Gets header data as String, strips "ID:" prefix if present, then hex decodes it.
     * This is useful for MQMD MsgId and CorrelId that are stored as hex Strings with "ID:" prefix.
     */
    private byte[] getHeaderDataAsHexDecodedBytes(final Headers headers, final String headerName) {
        String hexString = getHeaderDataAsString(headers, headerName);
        // Remove "ID:" prefix if present
        if (hexString.startsWith("ID:")) {
            hexString = hexString.substring(3);
        }
        // Convert hex string to bytes
        return HexFormat.of().parseHex(hexString);
    }


    // =========================================================================
    // User-defined message properties
    // =========================================================================

    /**
     * Verifies that a JMS String property should be written to Kafka topics as 
     * the UTF-8 bytes of its string value.
     */
    @Test
    public void userDefinedStringProperties() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setStringProperty("strProp", "hello");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, "strProp")).isEqualTo("hello");
    }

    /**
     * Verifies that a JMS int property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void userDefinedIntProperties() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setIntProperty("intProp", 42);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, "intProp")).isEqualTo("42");
    }

    /**
     * Verifies that a JMS long property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void userDefinedLongProperties() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setLongProperty("longProp", 123456789012345L);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, "longProp")).isEqualTo("123456789012345");
    }

    /**
     * Verifies that a JMS float property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void userDefinedFloatProperties() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setFloatProperty("floatProp", 1.5f);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, "floatProp")).isEqualTo("1.5");
    }

    /**
     * Verifies that a JMS double property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void userDefinedDoublePropertyPreservedOnWire() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setDoubleProperty("doubleProp", 3.141592653589793);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, "doubleProp")).isEqualTo("3.141592653589793");
    }

    /**
     * Verifies that a JMS boolean property should be written to Kafka topics as 
     * the UTF-8 bytes of "true" or "false"
     */
    @Test
    public void userDefinedBooleanProperties() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setBooleanProperty("boolProp", true);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, "boolProp")).isEqualTo("true");
    }

    /**
     * Verifies that a JMS byte property (signed 8-bit integer) should be written 
     * to Kafka topics as the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void userDefinedByteProperties() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setByteProperty("byteProp", (byte) 64);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, "byteProp")).isEqualTo("64");
    }

    /**
     * Verifies that a JMS short property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void userDefinedShortProperties() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setShortProperty("shortProp", (short) 1000);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, "shortProp")).isEqualTo("1000");
    }

    /**
     * Verifies that when a String is used to add a byte array as a JMS property 
     * it should be written to Kafka topics as provided.
     */
    @Test
    public void userDefinedByteArrayProperties() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final byte[] TEST_PROP = new byte[] { 0x01, 0x02, 0x03, 0x04 };
        message.setObjectProperty("bytesProp", new String(TEST_PROP));
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsBytes(headers, "bytesProp")).isEqualTo(TEST_PROP);
    }

    // =========================================================================
    // JMSx properties (standard JMS)
    // =========================================================================

    /**
     * Verifies that a JMS int properties should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void jmsxDeliveryCount() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMSX_DELIVERY_COUNT)).isEqualTo("1");
    }

    /**
     * Verifies that a JMS String property should be written to Kafka topics as 
     * the UTF-8 bytes of its string value.
     */
    @Test
    public void jmsxUserId() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setStringProperty(JmsConstants.JMS_IBM_MQMD_USERIDENTIFIER, "app");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMSX_USERID).trim()).isEqualTo("app");
    }

    /**
     * Verifies that a JMS String property should be written to Kafka topics as 
     * the UTF-8 bytes of its string value.
     */
    @Test
    public void jmsxGroupId() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final String TEST_GROUP = "MY_GROUP";
        message.setStringProperty(JmsConstants.JMSX_GROUPID, TEST_GROUP);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMSX_GROUPID)).isEqualTo(TEST_GROUP);
    }

    /**
     * Verifies that a JMS int properties should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void jmsxGroupSeq() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setIntProperty(JmsConstants.JMSX_GROUPSEQ, 3);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMSX_GROUPSEQ)).isEqualTo("3");
    }

    // =========================================================================
    // JMS_IBM_* extension properties
    // =========================================================================

    /**
     * Verifies that a JMS String property should be written to Kafka topics as 
     * the UTF-8 bytes of its string value.
     */
    @Test
    public void jmsIbmFormat() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_FORMAT).trim()).isEqualTo("MQSTR");
    }

    /**
     * Verifies that a JMS int property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void jmsIbmMsgType() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        // a normal datagram sent without a reply-to destination has a type of 8 (MQMT_DATAGRAM)
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_MSGTYPE)).isEqualTo("8");
    }

    /**
     * Verifies that a JMS String property should be written to Kafka topics as 
     * the UTF-8 bytes of its string value.
     */
    @Test
    public void jmsIbmCharacterSet() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_CHARACTER_SET)).isEqualTo("UTF-8");
    }

    /**
     * Verifies that a JMS int property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void jmsIbmEncoding() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_ENCODING)).isEqualTo("273");
    }

    /**
     * Verifies that a JMS String property should be written to Kafka topics as 
     * the UTF-8 bytes of its string value.
     */
    @Test
    public void jmsIbmPutDate() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        message.setStringProperty(JmsConstants.JMS_IBM_MQMD_PUTDATE, today);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_PUTDATE)).isEqualTo(today);
    }

    /**
     * Verifies that a JMS String property should be written to Kafka topics as 
     * the UTF-8 bytes of its string value.
     */
    @Test
    public void jmsIbmPutTime() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setStringProperty(JmsConstants.JMS_IBM_MQMD_PUTTIME, "20190101");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_PUTTIME)).isEqualTo("20190101");
    }

    /**
     * Verifies that a JMS int property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void jmsIbmPutApplType() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_PUTAPPLTYPE)).matches("\\d+");
    }


    // =========================================================================
    // JMS_IBM_MQMD_* properties 
    // =========================================================================

    /**
     * Verifies that a JMS String property should be written to Kafka topics as 
     * the UTF-8 bytes of its string value.
     */
    @Test
    public void jmsIbmMqmdPutApplName() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME)).isNotEmpty();
    }

    /**
     * Verifies that a JMS String property should be written to Kafka topics as 
     * the UTF-8 bytes of its string value.
     */
    @Test
    public void jmsIbmMqmdPutDate() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        // set date in format "yyyyMMdd"
        final String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        message.setStringProperty(JmsConstants.JMS_IBM_MQMD_PUTDATE, today);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_MQMD_PUTDATE)).isEqualTo(today);
    }

    /**
     * Verifies that a JMS String property should be written to Kafka topics as 
     * the UTF-8 bytes of its string value.
     */
    @Test
    public void jmsIbmMqmdPutTime() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final String time = "123456";
        message.setStringProperty(JmsConstants.JMS_IBM_MQMD_PUTTIME, time);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_MQMD_PUTTIME).trim()).matches(time);
    }

    /**
     * Verifies that a JMS int property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void jmsIbmMqmdBackoutCount() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_MQMD_BACKOUTCOUNT)).isEqualTo("0");
    }

    /**
     * Verifies that a JMS int property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void jmsIbmMqmdPriority() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setJMSPriority(4);
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_MQMD_PRIORITY)).matches("4");
    }

    /**
     * Verifies that a JMS int property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void jmsIbmMqmdPersistence() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_MQMD_PERSISTENCE)).isEqualTo("1");
    }

    /**
     * Verifies that a JMS int property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void jmsIbmMqmdMsgType() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        // a normal datagram sent without a reply-to destination has a type of 8 (MQMT_DATAGRAM)
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_MQMD_MSGTYPE)).isEqualTo("8");
    }

    /**
     * Verifies that a JMS int property should be written to Kafka topics as 
     * the UTF-8 bytes of its decimal string representation.
     */
    @Test
    public void jmsIbmMqmdEncoding() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_MQMD_ENCODING)).isEqualTo("273");
    }

    /**
     * Verifies that MQMD MsgId is stored as a hex String with "ID:" prefix.
     * When decoded, it should be 24 bytes.
     */
    @Test
    public void jmsIbmMqmdMsgId() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        // MsgId is stored as hex String with "ID:" prefix, decode and verify size
        assertThat(getHeaderDataAsHexDecodedBytes(headers, JmsConstants.JMS_IBM_MQMD_MSGID)).hasSize(24);
    }

    /**
     * Verifies that MQMD CorrelId is stored as a hex String with "ID:" prefix.
     * When decoded, it should match the original bytes set.
     */
    @Test
    public void jmsIbmMqmdCorrelId() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setJMSCorrelationIDAsBytes(TEST_CORREL_ID);
        final Headers headers = pollHeaders(message);
        // CorrelId is stored as hex String with "ID:" prefix, decode and verify
        assertThat(getHeaderDataAsHexDecodedBytes(headers, JmsConstants.JMS_IBM_MQMD_CORRELID)).isEqualTo(TEST_CORREL_ID);
    }

    /**
     * Verifies that MQMD AccountingToken is stored as a Base64-encoded String.
     * When decoded, it should be 32 bytes.
     */
    @Test
    public void jmsIbmMqmdAccountingToken() throws Exception {
        startTask(true);
        final byte[] customAccountingToken = new byte[32]; 
        final String accountToken = "MyBillingDeptToken123";
        final byte[] sourceData = accountToken.getBytes();
        System.arraycopy(sourceData, 0, customAccountingToken, 0, sourceData.length);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setObjectProperty(JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN, sourceData);
        final Headers headers = pollHeaders(message);
        // AccountingToken is stored as Base64 String, decode and verify size
        byte[] fixedBytes = Arrays.copyOf(accountToken.getBytes(StandardCharsets.UTF_8), 32);
        String base64Result = Base64.getEncoder().encodeToString(fixedBytes);
        assertThat(getHeaderDataAsString(headers, JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN))
            .isEqualTo(base64Result);
    }

    /**
     * Verifies that MQMD GroupId is stored as a Base64-encoded String.
     * When decoded, it should be 24 bytes.
     */
    @Test
    public void jmsIbmMqmdGroupId() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        // GroupId is stored as Base64 String, decode and verify size
        assertThat(getHeaderDataAsBase64DecodedBytes(headers, JmsConstants.JMS_IBM_MQMD_GROUPID)).hasSize(24);
    }
}