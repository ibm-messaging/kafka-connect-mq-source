/**
 * Copyright 2019, 2024, 2026 IBM Corporation
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

import com.ibm.eventstreams.connect.mqsource.processor.JmsToKafkaHeaderConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JmsToKafkaHeaderConverterTest {

    @Mock
    private TextMessage message;

    @Test
    public void convertJmsPropertiesToKafkaHeaders() throws JMSException {
        // Test that JMS properties are copied to Kafka headers
        final JmsToKafkaHeaderConverter converter = new JmsToKafkaHeaderConverter();
        final List<String> keys = Arrays.asList("facilityCountryCode", "facilityNum", "nullProperty");
        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("facilityCountryCode")).thenReturn("US");
        when(message.getObjectProperty("facilityNum")).thenReturn("12345");
        when(message.getObjectProperty("nullProperty")).thenReturn(null);

        // Act
        final ConnectHeaders actualConnectHeaders = converter.convertJmsPropertiesToKafkaHeaders(message);

        //Verify
        assertEquals("All three custom JMS properties were copied to kafka successfully.", 3, actualConnectHeaders.size());
    }

    @Test
    public void convertIntegerJmsPropertiesToKafkaHeaders() throws JMSException {
        // Test that Integer JMS properties are stored as INT32 headers
        final JmsToKafkaHeaderConverter converter = new JmsToKafkaHeaderConverter();

        final List<String> keys = Arrays.asList("JMS_IBM_MQMD_Priority", "customIntProp");
        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("JMS_IBM_MQMD_Priority")).thenReturn(5);
        when(message.getObjectProperty("customIntProp")).thenReturn(12345);

        // Act
        final ConnectHeaders actualConnectHeaders = converter.convertJmsPropertiesToKafkaHeaders(message);

        // Verify
        assertEquals(2, actualConnectHeaders.size());

        Header priorityHeader = actualConnectHeaders.lastWithName("JMS_IBM_MQMD_Priority");
        assertEquals(Schema.Type.INT32, priorityHeader.schema().type());
        assertEquals(5, priorityHeader.value());

        Header customIntHeader = actualConnectHeaders.lastWithName("customIntProp");
        assertEquals(Schema.Type.INT32, customIntHeader.schema().type());
        assertEquals(12345, customIntHeader.value());
    }

    @Test
    public void convertMqmdByteArrayPropertiesToKafkaHeaders() throws JMSException {
        // Test that MQMD byte array properties are preserved as byte arrays.
        // Note: JMS spec does not allow custom byte[] properties - only MQMD properties can be byte[].
        final JmsToKafkaHeaderConverter converter = new JmsToKafkaHeaderConverter();

        final List<String> keys = Arrays.asList("JMS_IBM_MQMD_GroupId", "JMS_IBM_MQMD_AccountingToken");
        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        final byte[] groupId = new byte[]{0x01, 0x02, 0x03, 0x04};
        final byte[] accountingToken = new byte[]{0x05, 0x06, 0x07, 0x08};

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("JMS_IBM_MQMD_GroupId")).thenReturn(groupId);
        when(message.getObjectProperty("JMS_IBM_MQMD_AccountingToken")).thenReturn(accountingToken);

        // Act
        final ConnectHeaders actualConnectHeaders = converter.convertJmsPropertiesToKafkaHeaders(message);

        // Verify
        assertEquals(2, actualConnectHeaders.size());
        Header groupIdHeader = actualConnectHeaders.lastWithName("JMS_IBM_MQMD_GroupId");
        assertEquals(Schema.Type.BYTES, groupIdHeader.schema().type());
        assertArrayEquals(groupId, (byte[]) groupIdHeader.value());

        Header accountingTokenHeader = actualConnectHeaders.lastWithName("JMS_IBM_MQMD_AccountingToken");
        assertEquals(Schema.Type.BYTES, accountingTokenHeader.schema().type());
        assertArrayEquals(accountingToken, (byte[]) accountingTokenHeader.value());
    }

    @Test
    public void convertStringJmsPropertiesToKafkaHeaders() throws JMSException {
        // Test that String properties are stored as STRING headers
        final JmsToKafkaHeaderConverter converter = new JmsToKafkaHeaderConverter();

        final List<String> keys = Arrays.asList("JMS_IBM_MQMD_Format", "customStringProp");
        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("JMS_IBM_MQMD_Format")).thenReturn("MQSTR");
        when(message.getObjectProperty("customStringProp")).thenReturn("testValue");

        // Act
        final ConnectHeaders actualConnectHeaders = converter.convertJmsPropertiesToKafkaHeaders(message);

        // Verify
        assertEquals(2, actualConnectHeaders.size());
        Header formatHeader = actualConnectHeaders.lastWithName("JMS_IBM_MQMD_Format");
        assertEquals(Schema.Type.STRING, formatHeader.schema().type());
        assertEquals("MQSTR", formatHeader.value());

        Header customHeader = actualConnectHeaders.lastWithName("customStringProp");
        assertEquals(Schema.Type.STRING, customHeader.schema().type());
        assertEquals("testValue", customHeader.value());
    }

    @Test
    public void convertNumericAndPrimitiveJmsPropertiesToKafkaHeaders() throws JMSException {
        // Test that all numeric and primitive JMS types are stored with their native schema types
        final JmsToKafkaHeaderConverter converter = new JmsToKafkaHeaderConverter();

        final List<String> keys = Arrays.asList("longProp", "shortProp", "byteProp",
                                                  "booleanProp", "floatProp", "doubleProp");
        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("longProp")).thenReturn(123456789L);
        when(message.getObjectProperty("shortProp")).thenReturn((short) 100);
        when(message.getObjectProperty("byteProp")).thenReturn((byte) 42);
        when(message.getObjectProperty("booleanProp")).thenReturn(true);
        when(message.getObjectProperty("floatProp")).thenReturn(3.14f);
        when(message.getObjectProperty("doubleProp")).thenReturn(2.718281828);

        // Act
        final ConnectHeaders actualConnectHeaders = converter.convertJmsPropertiesToKafkaHeaders(message);

        // Verify: each type maps to its corresponding Connect schema type
        assertEquals(6, actualConnectHeaders.size());

        Header longHeader = actualConnectHeaders.lastWithName("longProp");
        assertEquals(Schema.Type.INT64, longHeader.schema().type());
        assertEquals(123456789L, longHeader.value());

        Header shortHeader = actualConnectHeaders.lastWithName("shortProp");
        assertEquals(Schema.Type.INT16, shortHeader.schema().type());
        assertEquals((short) 100, shortHeader.value());

        Header byteHeader = actualConnectHeaders.lastWithName("byteProp");
        assertEquals(Schema.Type.INT8, byteHeader.schema().type());
        assertEquals((byte) 42, byteHeader.value());

        Header booleanHeader = actualConnectHeaders.lastWithName("booleanProp");
        assertEquals(Schema.Type.BOOLEAN, booleanHeader.schema().type());
        assertEquals(true, booleanHeader.value());

        Header floatHeader = actualConnectHeaders.lastWithName("floatProp");
        assertEquals(Schema.Type.FLOAT32, floatHeader.schema().type());
        assertEquals(3.14f, floatHeader.value());

        Header doubleHeader = actualConnectHeaders.lastWithName("doubleProp");
        assertEquals(Schema.Type.FLOAT64, doubleHeader.schema().type());
        assertEquals(2.718281828, doubleHeader.value());
    }

    @Test
    public void convertNullValuesInJmsPropertiesToKafkaHeaders() throws JMSException {
        final JmsToKafkaHeaderConverter converter = new JmsToKafkaHeaderConverter();
        final List<String> keys = Arrays.asList("nullProperty");
        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("nullProperty")).thenReturn(null);

        // Act
        final ConnectHeaders actualConnectHeaders = converter.convertJmsPropertiesToKafkaHeaders(message);

        // Verify
        assertEquals(1, actualConnectHeaders.size());
        Header nullHeader = actualConnectHeaders.lastWithName("nullProperty");
        assertEquals(Schema.Type.STRING, nullHeader.schema().type());
        assertNull(nullHeader.value());
    }
}
