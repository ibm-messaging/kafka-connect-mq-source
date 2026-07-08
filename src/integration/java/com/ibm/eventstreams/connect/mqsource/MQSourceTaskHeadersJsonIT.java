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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.TextMessage;

import com.ibm.msg.client.jms.JmsConstants;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder;
import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;
import com.ibm.eventstreams.connect.mqsource.utils.SourceTaskStopper;

/**
 * Integration tests for copying JMS message properties to Kafka headers
 *  using the JSON Connect header converter.
 *
 * JsonConverter is schema-sensitive so this is used to catch schema type 
 *  bugs that using the default SimpleHeaderConverter cannot reveal.
 */
public class MQSourceTaskHeadersJsonIT extends AbstractJMSContextIT {

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
        converter = new JsonConverter();

        final Map<String, String> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", "false");
        converterConfig.put("converter.type", "header");
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
     * Serialises the named header through JsonConverter (as Connect would before
     * writing to the Kafka topic) and returns the result decoded as a UTF-8
     * string.
     */
    private String getHeaderAsJson(final Headers headers, final String headerName) {
        final Header h = headers.lastWithName(headerName);
        assertThat(h).isNotNull();
        final byte[] wire = converter.fromConnectHeader(TOPIC, headerName, h.schema(), h.value());
        assertThat(wire).isNotNull();
        return new String(wire, StandardCharsets.UTF_8);
    }

    /**
     * Returns the Connect schema type of the named header.
     */
    private Schema.Type getHeaderSchemaType(final Headers headers, final String headerName) {
        final Header h = headers.lastWithName(headerName);
        assertThat(h).isNotNull();
        return h.schema().type();
    }


    // =========================================================================
    // User-defined message properties
    // =========================================================================

    /**
     * A JMS String property must produce a header with Schema.STRING.
     * JsonConverter serialises this as a JSON string: "hello"
     */
    @Test
    public void userDefinedStringProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setStringProperty("strProp", "hello");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, "strProp")).isEqualTo(Schema.Type.STRING);
        assertThat(getHeaderAsJson(headers, "strProp")).isEqualTo("\"hello\"");
    }

    /**
     * A JMS int property must produce a header with Schema.INT32.
     * JsonConverter serialises this as a bare JSON number: 42
     */
    @Test
    public void userDefinedIntProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setIntProperty("intProp", 42);
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, "intProp")).isEqualTo(Schema.Type.INT32);
        assertThat(getHeaderAsJson(headers, "intProp")).isEqualTo("42");
    }

    /**
     * A JMS long property must produce a header with Schema.INT64.
     * JsonConverter serialises this as a bare JSON number: 123456789012345
     */
    @Test
    public void userDefinedLongProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setLongProperty("longProp", 123456789012345L);
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, "longProp")).isEqualTo(Schema.Type.INT64);
        assertThat(getHeaderAsJson(headers, "longProp")).isEqualTo("123456789012345");
    }

    /**
     * A JMS float property must produce a header with Schema.FLOAT32.
     * JsonConverter serialises this as a bare JSON number: 1.5
     */
    @Test
    public void userDefinedFloatProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setFloatProperty("floatProp", 1.5f);
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, "floatProp")).isEqualTo(Schema.Type.FLOAT32);
        assertThat(getHeaderAsJson(headers, "floatProp")).isEqualTo("1.5");
    }

    /**
     * A JMS double property must produce a header with Schema.FLOAT64.
     * JsonConverter serialises this as a bare JSON number.
     */
    @Test
    public void userDefinedDoubleProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setDoubleProperty("doubleProp", 3.141592653589793);
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, "doubleProp")).isEqualTo(Schema.Type.FLOAT64);
        assertThat(getHeaderAsJson(headers, "doubleProp")).isEqualTo("3.141592653589793");
    }

    /**
     * A JMS boolean property must produce a header with Schema.BOOLEAN.
     * JsonConverter serialises this as a bare JSON boolean: true
     */
    @Test
    public void userDefinedBooleanProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setBooleanProperty("boolProp", true);
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, "boolProp")).isEqualTo(Schema.Type.BOOLEAN);
        assertThat(getHeaderAsJson(headers, "boolProp")).isEqualTo("true");
    }

    /**
     * A JMS byte property (signed 8-bit integer) must produce a header with
     * Schema.INT8.  JsonConverter serialises this as a bare JSON number: 64
     */
    @Test
    public void userDefinedByteProperty() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setByteProperty("byteProp", (byte) 64);
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, "byteProp")).isEqualTo(Schema.Type.INT8);
        assertThat(getHeaderAsJson(headers, "byteProp")).isEqualTo("64");
    }

    /**
     * A JMS short property must produce a header with Schema.INT16.
     * JsonConverter serialises this as a bare JSON number: 1000
     */
    @Test
    public void userDefinedShortProperty_schemaIsInt16() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setShortProperty("shortProp", (short) 1000);
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, "shortProp")).isEqualTo(Schema.Type.INT16);
        assertThat(getHeaderAsJson(headers, "shortProp")).isEqualTo("1000");
    }


    // =========================================================================
    // JMSx properties (standard JMS) 
    // =========================================================================

    /**
     * JMSXDeliveryCount is an int property; must arrive as Schema.INT32.
     */
    @Test
    public void jmsxDeliveryCount() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMSX_DELIVERY_COUNT)).isEqualTo(Schema.Type.INT32);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMSX_DELIVERY_COUNT)).isEqualTo("1");
    }

    /**
     * JMSXUserID is a String property; must arrive as Schema.STRING.
     */
    @Test
    public void jmsxUserId() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMSX_USERID)).isEqualTo(Schema.Type.STRING);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMSX_USERID)).isEqualTo("\"app         \"");
    }

    /**
     * JMSXGroupID is a String property; must arrive as Schema.STRING.
     */
    @Test
    public void jmsxGroupId() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setStringProperty(JmsConstants.JMSX_GROUPID, "MY_GROUP");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMSX_GROUPID)).isEqualTo(Schema.Type.STRING);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMSX_GROUPID)).isEqualTo("\"MY_GROUP\"");
    }

    /**
     * JMSXGroupSeq is an int property; must arrive as Schema.INT32.
     */
    @Test
    public void jmsxGroupSeq() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setIntProperty(JmsConstants.JMSX_GROUPSEQ, 3);
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMSX_GROUPSEQ)).isEqualTo(Schema.Type.INT32);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMSX_GROUPSEQ)).isEqualTo("3");
    }


    // =========================================================================
    // JMS_IBM_* extension properties 
    // =========================================================================

    /**
     * JMS_IBM_Format is a String property; must arrive as Schema.STRING.
     */
    @Test
    public void jmsIbmFormat() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_FORMAT)).isEqualTo(Schema.Type.STRING);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_FORMAT)).isEqualTo("\"MQSTR   \"");
    }

    /**
     * JMS_IBM_MsgType is an int property; must arrive as Schema.INT32.
     */
    @Test
    public void jmsIbmMsgType() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MSGTYPE)).isEqualTo(Schema.Type.INT32);
        // a normal datagram sent without a reply-to destination has a type of 8 (MQMT_DATAGRAM)
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_MSGTYPE)).isEqualTo("8");
    }

    /**
     * JMS_IBM_Character_Set is a String property; must arrive as Schema.STRING.
     */
    @Test
    public void jmsIbmCharacterSet() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_CHARACTER_SET)).isEqualTo(Schema.Type.STRING);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_CHARACTER_SET)).isEqualTo("\"UTF-8\"");
    }

    /**
     * JMS_IBM_Encoding is an int property; must arrive as Schema.INT32.
     */
    @Test
    public void jmsIbmEncoding() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_ENCODING)).isEqualTo(Schema.Type.INT32);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_ENCODING)).isEqualTo("273");
    }

    /**
     * JMS_IBM_PutDate is a String property; must arrive as Schema.STRING.
     */
    @Test
    public void jmsIbmPutDate() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        final String today = "\"" + LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + "\"";

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_PUTDATE)).isEqualTo(Schema.Type.STRING);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_PUTDATE)).isEqualTo(today);
    }

    /**
     * JMS_IBM_PutTime is a String property; must arrive as Schema.STRING.
     */
    @Test
    public void jmsIbmPutTime() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_PUTTIME)).isEqualTo(Schema.Type.STRING);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_PUTTIME)).startsWith("\"").endsWith("\"");
    }

    /**
     * JMS_IBM_PutApplType is an int property; must arrive as Schema.INT32.
     */
    @Test
    public void jmsIbmPutApplType() throws Exception {
        startTask(false);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_PUTAPPLTYPE)).isEqualTo(Schema.Type.INT32);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_PUTAPPLTYPE)).matches("\\d+");
    }


    // =========================================================================
    // JMS_IBM_MQMD_* properties
    // =========================================================================

    /**
     * JMS_IBM_MQMD_PutApplName is a String property; must arrive as Schema.STRING.
     */
    @Test
    public void jmsIbmMqmdPutApplName() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME)).isEqualTo(Schema.Type.STRING);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME)).startsWith("\"").endsWith("\"");
    }

    /**
     * JMS_IBM_MQMD_PutDate is a String property; must arrive as Schema.STRING.
     */
    @Test
    public void jmsIbmMqmdPutDate() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);
        final String today = "\"" + LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + "\"";

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_PUTDATE)).isEqualTo(Schema.Type.STRING);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_PUTDATE)).isEqualTo(today);
    }

    /**
     * JMS_IBM_MQMD_PutTime is a String property; must arrive as Schema.STRING.
     */
    @Test
    public void jmsIbmMqmdPutTime() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_PUTTIME)).isEqualTo(Schema.Type.STRING);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_PUTTIME)).startsWith("\"").endsWith("\"");
    }

    /**
     * JMS_IBM_MQMD_BackoutCount is an int property; must arrive as Schema.INT32.
     */
    @Test
    public void jmsIbmMqmdBackoutCount() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_BACKOUTCOUNT)).isEqualTo(Schema.Type.INT32);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_BACKOUTCOUNT)).isEqualTo("0");
    }    

    /**
     * JMS_IBM_MQMD_Priority is an int property; must arrive as Schema.INT32.
     */
    @Test
    public void jmsIbmMqmdPriority() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setJMSPriority(4);
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_PRIORITY)).isEqualTo(Schema.Type.INT32);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_PRIORITY)).isEqualTo("4");
    }

    /**
     * JMS_IBM_MQMD_Persistence is an int property; must arrive as Schema.INT32.
     */
    @Test
    public void jmsIbmMqmdPersistence() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_PERSISTENCE)).isEqualTo(Schema.Type.INT32);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_PERSISTENCE)).isEqualTo("1");
    }    

    /**
     * JMS_IBM_MQMD_MsgType is an int property; must arrive as Schema.INT32.
     */
    @Test
    public void jmsIbmMqmdMsgType() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_MSGTYPE)).isEqualTo(Schema.Type.INT32);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_MSGTYPE)).isEqualTo("8");
    }

    /**
     * JMS_IBM_MQMD_Encoding is an int property; must arrive as Schema.INT32.
     */
    @Test
    public void jmsIbmMqmdEncoding() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_ENCODING)).isEqualTo(Schema.Type.INT32);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_ENCODING)).isEqualTo("273");
    }

    @Test
    public void jmsIbmMqmdMsgId() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_MSGID)).isEqualTo(Schema.Type.STRING);
        final String json = getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_MSGID);
        assertThat(json).startsWith("\"ID:").endsWith("\"");
    }

    /**
     * JMS_IBM_MQMD_CorrelId is returned as Schema.STRING via getJMSCorrelationID(),
     * which formats it as "ID:" followed by lowercase hex digits.
     * The hex encoding must round-trip the bytes set via setJMSCorrelationIDAsBytes().
     */
    @Test
    public void jmsIbmMqmdCorrelId() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        message.setJMSCorrelationIDAsBytes(TEST_CORREL_ID);
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_CORRELID)).isEqualTo(Schema.Type.STRING);
        assertThat(getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_CORRELID))
            .isEqualTo("\"ID:0102030405060708090a0b0c0d0e0f101112131415161718\"");
    }

    /**
     * JMS_IBM_MQMD_AccountingToken is a 32-byte binary property; must arrive as Schema.BYTES.
     */
    @Test
    public void jmsIbmMqmdAccountingToken() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN)).isEqualTo(Schema.Type.BYTES);
        final String json = getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN);
        assertThat(json).startsWith("\"").endsWith("\"");
        final byte[] decoded = java.util.Base64.getDecoder().decode(json.substring(1, json.length() - 1));
        assertThat(decoded).hasSize(32);
    }

    /**
     * JMS_IBM_MQMD_GroupId is a 24-byte binary property; must arrive as Schema.BYTES.
     */
    @Test
    public void jmsIbmMqmdGroupId() throws Exception {
        startTask(true);
        final TextMessage message = getJmsContext().createTextMessage("msg");
        final Headers headers = pollHeaders(message);

        assertThat(getHeaderSchemaType(headers, JmsConstants.JMS_IBM_MQMD_GROUPID)).isEqualTo(Schema.Type.BYTES);
        final String json = getHeaderAsJson(headers, JmsConstants.JMS_IBM_MQMD_GROUPID);
        assertThat(json).startsWith("\"").endsWith("\"");
        final byte[] decoded = java.util.Base64.getDecoder().decode(json.substring(1, json.length() - 1));
        assertThat(decoded).hasSize(24);
    }
}
