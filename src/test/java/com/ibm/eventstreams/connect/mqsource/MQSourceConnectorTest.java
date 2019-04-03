/**
 * Copyright 2017 IBM Corporation
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
import org.apache.kafka.connect.source.SourceConnector;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MQSourceConnectorTest {
    @Test
    public void testVersion() {
        String version = new MQSourceConnector().version();
        String expectedVersion = System.getProperty("connectorVersion");
        assertEquals("Expected connector version to match version of built jar file.", expectedVersion, version);
    }

    @Test
    public void testConnectorType() throws IOException {
        Connector connector = new MQSourceConnector();
        assertTrue(SourceConnector.class.isAssignableFrom(connector.getClass()));
        final InputStream resourceAsStream = connector.getClass().getClassLoader().getResourceAsStream("conf.properties");
        final Properties prop = new Properties();
        prop.load(resourceAsStream);
        connector.start(prop.entrySet().stream().collect(Collectors.toMap(e -> (String)e.getKey(), e -> (String)e.getValue())));
    }
}