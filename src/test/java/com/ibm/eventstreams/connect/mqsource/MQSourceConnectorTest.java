/**
 * Copyright 2017, 2018, 2019, 2023 IBM Corporation
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

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.source.ConnectorTransactionBoundaries;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

public class MQSourceConnectorTest {
    @Test
    public void testVersion() {
        final String version = new MQSourceConnector().version();
        final String expectedVersion = System.getProperty("connectorVersion");
        assertEquals("Expected connector version to match version of built jar file.", expectedVersion, version);
    }

    @Test
    public void testConnectorType() {
        final Connector connector = new MQSourceConnector();
        assertTrue(SourceConnector.class.isAssignableFrom(connector.getClass()));
    }

    @Test
    public void testConnectorCanDefineTransactionBoundaries() {
        final SourceConnector connector = new MQSourceConnector();
        // Not supported
        assertEquals(ConnectorTransactionBoundaries.UNSUPPORTED, connector.canDefineTransactionBoundaries(Collections.emptyMap()));
        assertEquals(ConnectorTransactionBoundaries.UNSUPPORTED, connector.canDefineTransactionBoundaries(Collections.singletonMap("mq.exactly.once.state.queue", "DEV.QUEUE.2")));
    }

    @Test
    public void testConnectorExactlyOnceSupport() {
        final SourceConnector connector = new MQSourceConnector();
        // Only supported if mq.exactly.once.state.queue is supplied in the config and 'tasks.max' is 1
        assertEquals(ExactlyOnceSupport.UNSUPPORTED, connector.exactlyOnceSupport(Collections.emptyMap()));
        assertEquals(ExactlyOnceSupport.UNSUPPORTED, connector.exactlyOnceSupport(Collections.singletonMap("mq.exactly.once.state.queue", "")));
        assertEquals(ExactlyOnceSupport.UNSUPPORTED, connector.exactlyOnceSupport(Collections.singletonMap("mq.exactly.once.state.queue", null)));
        assertEquals(ExactlyOnceSupport.UNSUPPORTED, connector.exactlyOnceSupport(Collections.singletonMap("tasks.max", "1")));

        final Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("mq.exactly.once.state.queue", "DEV.QUEUE.2");
        configProps.put("tasks.max", "1");
        assertEquals(ExactlyOnceSupport.SUPPORTED, connector.exactlyOnceSupport(configProps));
        assertEquals(ExactlyOnceSupport.SUPPORTED, connector.exactlyOnceSupport(Collections.singletonMap("mq.exactly.once.state.queue", "DEV.QUEUE.2")));
    }

    @Test
    public void testConnectorConfigSupportsExactlyOnce() {
        // True if an mq.exactly.once.state.queue value is supplied in the config and 'tasks.max' is 1
        final Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("mq.exactly.once.state.queue", "DEV.QUEUE.2");
        configProps.put("tasks.max", "1");
        assertTrue(MQSourceConnector.configSupportsExactlyOnce(configProps));
        assertTrue(MQSourceConnector.configSupportsExactlyOnce(Collections.singletonMap("mq.exactly.once.state.queue", "DEV.QUEUE.2")));
        // False otherwise
        assertFalse(MQSourceConnector.configSupportsExactlyOnce(Collections.singletonMap("tasks.max", "1")));
        assertFalse(MQSourceConnector.configSupportsExactlyOnce(Collections.emptyMap()));
        assertFalse(MQSourceConnector.configSupportsExactlyOnce(Collections.singletonMap("mq.exactly.once.state.queue", "")));
        assertFalse(MQSourceConnector.configSupportsExactlyOnce(Collections.singletonMap("mq.exactly.once.state.queue", null)));
    }
    
}