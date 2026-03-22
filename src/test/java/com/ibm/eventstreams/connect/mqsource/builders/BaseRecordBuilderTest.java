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
package com.ibm.eventstreams.connect.mqsource.builders;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsource.MQSourceConnector;

/**
 * Unit tests for BaseRecordBuilder key extraction functionality.
 */
public class BaseRecordBuilderTest {

    /**
     * Concrete implementation of BaseRecordBuilder for testing.
     */
    private static class TestRecordBuilder extends BaseRecordBuilder {
        @Override
        public SchemaAndValue getValue(JMSContext context, String topic, boolean messageBodyJms, Message message)
                throws JMSException {
            return new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, "test-value");
        }
    }

    @Test
    public void testJMSXGroupIDKeyExtraction() throws JMSException {
        // Setup
        TestRecordBuilder builder = new TestRecordBuilder();
        Map<String, String> config = new HashMap<>();
        config.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER_KEY_HEADER, 
                   MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSXGROUPID);
        builder.configure(config);

        // Mock JMS message with JMSXGroupID property
        Message message = mock(Message.class);
        String expectedGroupId = "GROUP-A";
        when(message.getStringProperty("JMSXGroupID")).thenReturn(expectedGroupId);

        // Execute
        JMSContext context = mock(JMSContext.class);
        SchemaAndValue result = builder.getKey(context, "test-topic", message);

        // Verify
        assertEquals("Key schema should be OPTIONAL_STRING_SCHEMA", 
                     Schema.OPTIONAL_STRING_SCHEMA, result.schema());
        assertEquals("Key value should be the JMSXGroupID value", 
                     expectedGroupId, result.value());
    }

    @Test
    public void testJMSXGroupIDKeyExtractionWithNullValue() throws JMSException {
        // Setup
        TestRecordBuilder builder = new TestRecordBuilder();
        Map<String, String> config = new HashMap<>();
        config.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER_KEY_HEADER, 
                   MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSXGROUPID);
        builder.configure(config);

        // Mock JMS message without JMSXGroupID property
        Message message = mock(Message.class);
        when(message.getStringProperty("JMSXGroupID")).thenReturn(null);

        // Execute
        JMSContext context = mock(JMSContext.class);
        SchemaAndValue result = builder.getKey(context, "test-topic", message);

        // Verify
        assertEquals("Key schema should be OPTIONAL_STRING_SCHEMA", 
                     Schema.OPTIONAL_STRING_SCHEMA, result.schema());
        assertNull("Key value should be null when JMSXGroupID is not present", 
                   result.value());
    }

    @Test
    public void testMultipleMessagesWithSameGroupID() throws JMSException {
        // Setup
        TestRecordBuilder builder = new TestRecordBuilder();
        Map<String, String> config = new HashMap<>();
        config.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER_KEY_HEADER, 
                   MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSXGROUPID);
        builder.configure(config);

        String groupId = "GROUP-B";
        JMSContext context = mock(JMSContext.class);

        // Create three mock messages with the same group ID
        Message message1 = mock(Message.class);
        when(message1.getStringProperty("JMSXGroupID")).thenReturn(groupId);

        Message message2 = mock(Message.class);
        when(message2.getStringProperty("JMSXGroupID")).thenReturn(groupId);

        Message message3 = mock(Message.class);
        when(message3.getStringProperty("JMSXGroupID")).thenReturn(groupId);

        // Execute
        SchemaAndValue result1 = builder.getKey(context, "test-topic", message1);
        SchemaAndValue result2 = builder.getKey(context, "test-topic", message2);
        SchemaAndValue result3 = builder.getKey(context, "test-topic", message3);

        // Verify all have the same key value
        assertEquals("All messages should have the same key", groupId, result1.value());
        assertEquals("All messages should have the same key", groupId, result2.value());
        assertEquals("All messages should have the same key", groupId, result3.value());
        assertEquals("Keys should be equal", result1.value(), result2.value());
        assertEquals("Keys should be equal", result2.value(), result3.value());
    }

    @Test
    public void testDifferentGroupIDsProduceDifferentKeys() throws JMSException {
        // Setup
        TestRecordBuilder builder = new TestRecordBuilder();
        Map<String, String> config = new HashMap<>();
        config.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER_KEY_HEADER, 
                   MQSourceConnector.CONFIG_VALUE_MQ_RECORD_BUILDER_KEY_HEADER_JMSXGROUPID);
        builder.configure(config);

        JMSContext context = mock(JMSContext.class);

        // Create mock messages with different group IDs
        Message messageA = mock(Message.class);
        when(messageA.getStringProperty("JMSXGroupID")).thenReturn("GROUP-A");

        Message messageB = mock(Message.class);
        when(messageB.getStringProperty("JMSXGroupID")).thenReturn("GROUP-B");

        Message messageC = mock(Message.class);
        when(messageC.getStringProperty("JMSXGroupID")).thenReturn("GROUP-C");

        // Execute
        SchemaAndValue resultA = builder.getKey(context, "test-topic", messageA);
        SchemaAndValue resultB = builder.getKey(context, "test-topic", messageB);
        SchemaAndValue resultC = builder.getKey(context, "test-topic", messageC);

        // Verify all have different key values
        assertEquals("Key A should be GROUP-A", "GROUP-A", resultA.value());
        assertEquals("Key B should be GROUP-B", "GROUP-B", resultB.value());
        assertEquals("Key C should be GROUP-C", "GROUP-C", resultC.value());
        
        // Verify they are different from each other
        assert !resultA.value().equals(resultB.value());
        assert !resultB.value().equals(resultC.value());
        assert !resultA.value().equals(resultC.value());
    }

    @Test
    public void testNoKeyHeaderConfigured() throws JMSException {
        // Setup - no key header configured
        TestRecordBuilder builder = new TestRecordBuilder();
        Map<String, String> config = new HashMap<>();
        builder.configure(config);

        // Mock JMS message with JMSXGroupID property
        Message message = mock(Message.class);
        when(message.getStringProperty("JMSXGroupID")).thenReturn("GROUP-A");

        // Execute
        JMSContext context = mock(JMSContext.class);
        SchemaAndValue result = builder.getKey(context, "test-topic", message);

        // Verify - key should be null when no key header is configured
        assertNull("Key schema should be null when no key header configured", result.schema());
        assertNull("Key value should be null when no key header configured", result.value());
    }
}
