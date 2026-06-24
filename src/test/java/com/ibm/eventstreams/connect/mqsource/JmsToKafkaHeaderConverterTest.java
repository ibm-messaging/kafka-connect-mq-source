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
    public void convertMqmdByteArrayPropertiesToKafkaHeaders() throws JMSException {
        // Test that MQMD byte array properties are preserved as byte arrays.
        // Note: JMS spec does not allow custom byte[] properties - only MQMD properties can be byte[].
        // Previously these were converted to their object reference string (e.g. "[B@5cde362e")
        // which was not useful. This test verifies they are now stored as BYTES headers.

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
    public void convertNumericJmsPropertiesToStringKafkaHeaders() throws JMSException {
        // Test that numeric JMS properties are converted to their string representation.
        // Integer 42 becomes the string "42", long 123456789L becomes "123456789", etc.
        final JmsToKafkaHeaderConverter converter = new JmsToKafkaHeaderConverter();

        final List<String> keys = Arrays.asList("intProp", "longProp", "floatProp",
                                                  "doubleProp", "boolProp", "byteProp", "shortProp");
        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("intProp")).thenReturn(42);
        when(message.getObjectProperty("longProp")).thenReturn(123456789L);
        when(message.getObjectProperty("floatProp")).thenReturn(3.14f);
        when(message.getObjectProperty("doubleProp")).thenReturn(2.718281828);
        when(message.getObjectProperty("boolProp")).thenReturn(true);
        when(message.getObjectProperty("byteProp")).thenReturn((byte) 64);
        when(message.getObjectProperty("shortProp")).thenReturn((short) 1000);

        // Act
        final ConnectHeaders actualConnectHeaders = converter.convertJmsPropertiesToKafkaHeaders(message);

        // Verify: all non-byte-array types are stored as STRING headers
        assertEquals(7, actualConnectHeaders.size());

        assertEquals(Schema.Type.STRING, actualConnectHeaders.lastWithName("intProp").schema().type());
        assertEquals("42", actualConnectHeaders.lastWithName("intProp").value());

        assertEquals(Schema.Type.STRING, actualConnectHeaders.lastWithName("longProp").schema().type());
        assertEquals("123456789", actualConnectHeaders.lastWithName("longProp").value());

        assertEquals(Schema.Type.STRING, actualConnectHeaders.lastWithName("floatProp").schema().type());
        assertEquals("3.14", actualConnectHeaders.lastWithName("floatProp").value());

        assertEquals(Schema.Type.STRING, actualConnectHeaders.lastWithName("doubleProp").schema().type());
        assertEquals("2.718281828", actualConnectHeaders.lastWithName("doubleProp").value());

        assertEquals(Schema.Type.STRING, actualConnectHeaders.lastWithName("boolProp").schema().type());
        assertEquals("true", actualConnectHeaders.lastWithName("boolProp").value());

        assertEquals(Schema.Type.STRING, actualConnectHeaders.lastWithName("byteProp").schema().type());
        assertEquals("64", actualConnectHeaders.lastWithName("byteProp").value());

        assertEquals(Schema.Type.STRING, actualConnectHeaders.lastWithName("shortProp").schema().type());
        assertEquals("1000", actualConnectHeaders.lastWithName("shortProp").value());
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
