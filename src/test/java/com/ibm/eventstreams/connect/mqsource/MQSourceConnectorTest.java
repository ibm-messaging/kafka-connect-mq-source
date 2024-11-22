/**
 * Copyright 2017, 2018, 2019, 2023, 2024 IBM Corporation
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

import org.apache.kafka.common.config.Config;
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

    @Test
    public void testValidateMQClientReconnectOptions() {
        final Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("mq.exactly.once.state.queue", "DEV.QUEUE.2");
        configProps.put("tasks.max", "1");

        final Config config = new MQSourceConnector().validate(configProps);

        assertTrue(config.configValues().stream().anyMatch(cv -> cv.errorMessages().size() > 0));
        assertTrue(config.configValues().stream()
                .filter(cv -> cv.name().equals(MQSourceConnector.CONFIG_NAME_MQ_CLIENT_RECONNECT_OPTIONS))
                .flatMap(cv -> cv.errorMessages().stream())
                .anyMatch(msg -> msg.contains("When running the MQ source connector with exactly once mode, the client reconnect option 'QMGR' should be provided.")));
    }

    @Test
    public void testValidateMQClientReconnectOptionsWithoutExactlyOnce() {
        final Map<String, String> configProps = new HashMap<String, String>();
        final Config config = new MQSourceConnector().validate(configProps);

        assertFalse(config.configValues().stream()
            .filter(cv -> cv.name().equals(MQSourceConnector.CONFIG_NAME_MQ_CLIENT_RECONNECT_OPTIONS))
            .flatMap(cv -> cv.errorMessages().stream())
            .anyMatch(msg -> msg.contains("When running the MQ source connector with exactly once mode, the client reconnect option 'QMGR' should be provided.")));
    }

    @Test
    public void testValidateMQClientReconnectOptionsWithQMGROption() {
        final Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("mq.exactly.once.state.queue", "DEV.QUEUE.2");
        configProps.put("mq.client.reconnect.options", "QMGR");
        configProps.put("tasks.max", "1");

        final Config config = new MQSourceConnector().validate(configProps);

        assertTrue(config.configValues().stream().anyMatch(cv -> cv.errorMessages().size() > 0));
        assertFalse(config.configValues().stream()
                .filter(cv -> cv.name().equals(MQSourceConnector.CONFIG_NAME_MQ_CLIENT_RECONNECT_OPTIONS))
                .flatMap(cv -> cv.errorMessages().stream())
                .anyMatch(msg -> msg.contains("When running the MQ source connector with exactly once mode, the client reconnect option 'QMGR' should be provided.")));
    }

    @Test
    public void testValidateMQClientReconnectOptionsWithANYOption() {
        final Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("mq.exactly.once.state.queue", "DEV.QUEUE.2");
        configProps.put("mq.client.reconnect.options", "ANY");
        configProps.put("tasks.max", "1");

        final Config config = new MQSourceConnector().validate(configProps);

        assertTrue(config.configValues().stream().anyMatch(cv -> cv.errorMessages().size() > 0));
        assertTrue(config.configValues().stream()
                .filter(cv -> cv.name().equals(MQSourceConnector.CONFIG_NAME_MQ_CLIENT_RECONNECT_OPTIONS))
                .flatMap(cv -> cv.errorMessages().stream())
                .anyMatch(msg -> msg.contains("When running the MQ source connector with exactly once mode, the client reconnect option 'QMGR' should be provided.")));
    }

    @Test
    public void testValidateRetryDelayConfig() {
        final Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("mq.reconnect.delay.max.ms", "10");
        configProps.put("mq.reconnect.delay.min.ms", "100");
        configProps.put("tasks.max", "1");

        final Config config = new MQSourceConnector().validate(configProps);

        assertTrue(config.configValues().stream().anyMatch(cv -> cv.errorMessages().size() > 0));
        assertTrue(config.configValues().stream()
                .filter(cv -> cv.name().equals(MQSourceConnector.CONFIG_RECONNECT_DELAY_MAX))
                .flatMap(cv -> cv.errorMessages().stream())
                .anyMatch(msg -> msg.contains("The value of 'mq.reconnect.delay.max.ms' must be greater than or equal to the value of 'mq.reconnect.delay.min.ms'.")));
    }

    @Test
    public void testValidateRetryDelayConfigWithNoReconnectValues() {
        final Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("tasks.max", "1");

        final Config config = new MQSourceConnector().validate(configProps);

        assertTrue(config.configValues().stream().anyMatch(cv -> cv.errorMessages().size() > 0));
        assertTrue(config.configValues().stream()
                .filter(cv -> cv.name().equals(MQSourceConnector.CONFIG_RECONNECT_DELAY_MAX))
                .flatMap(cv -> cv.errorMessages().stream())
                .allMatch(msg -> msg == null));
    }

    @Test
    public void testValidateRetryDelayConfigWithDefaultValues() {
        final Map<String, String> configProps = new HashMap<String, String>();
        configProps.put("mq.reconnect.delay.min.ms", "1000000");
        configProps.put("tasks.max", "1");

        final Config config = new MQSourceConnector().validate(configProps);

        assertTrue(config.configValues().stream().anyMatch(cv -> cv.errorMessages().size() > 0));
        assertTrue(config.configValues().stream()
                .filter(cv -> cv.name().equals(MQSourceConnector.CONFIG_RECONNECT_DELAY_MAX))
                .flatMap(cv -> cv.errorMessages().stream())
                .anyMatch(msg -> msg.contains("The value of 'mq.reconnect.delay.max.ms' must be greater than or equal to the value of 'mq.reconnect.delay.min.ms'.")));
    }
}
