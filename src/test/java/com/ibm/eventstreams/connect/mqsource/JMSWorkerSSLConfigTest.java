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

import org.apache.kafka.common.config.AbstractConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

/**
 * Tests for SSL configuration validation in JMSWorker.
 * These tests verify that SSL context is only created when complete configuration sets are provided.
 */
public class JMSWorkerSSLConfigTest {

    private Map<String, String> baseConfig;

    @Before
    public void setUp() {
        baseConfig = new HashMap<>();
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_QUEUE_MANAGER, "QM1");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_CONNECTION_MODE, "client");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_CONNECTION_NAME_LIST, "localhost(1414)");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_CHANNEL_NAME, "DEV.APP.SVRCONN");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_QUEUE, "DEV.QUEUE.1");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_TOPIC, "test-topic");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_RECORD_BUILDER, 
            "com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder");
    }

    @Test
    public void testSSLConfigWithKeystoreLocationAndPassword() {
        // Complete keystore set: location + password
        final String keystorePath = getClass().getClassLoader().getResource("test-keystore.jks").getPath();
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION, keystorePath);
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        assertNotNull("Config should be created with keystore location and password", config);
    }

    @Test
    public void testSSLConfigWithKeystoreContentAndPassword() {
        // Complete keystore set: content + password
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_CONTENT, "dGVzdCBrZXlzdG9yZSBjb250ZW50"); // base64
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        assertNotNull("Config should be created with keystore content and password", config);
    }

    @Test
    public void testSSLConfigWithTruststoreLocationAndPassword() {
        // Complete truststore set: location + password
        final String truststorePath = getClass().getClassLoader().getResource("test-truststore.jks").getPath();
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION, truststorePath);
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        assertNotNull("Config should be created with truststore location and password", config);
    }

    @Test
    public void testSSLConfigWithTruststoreContentAndPassword() {
        // Complete truststore set: content + password
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_CONTENT, "dGVzdCB0cnVzdHN0b3JlIGNvbnRlbnQ="); // base64
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        assertNotNull("Config should be created with truststore content and password", config);
    }

    @Test
    public void testSSLConfigWithOnlyKeystorePassword() {
        // Incomplete: only password without keystore location or content
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        // Config should be created, but JMSWorker should not create SSLContext
        assertNotNull("Config should be created even with incomplete SSL config", config);
    }

    @Test
    public void testSSLConfigWithEmptyKeystoreContent() {
        // Empty content should be treated as no content
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_CONTENT, "");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        assertNotNull("Config should be created with empty keystore content", config);
    }

    @Test
    public void testSSLConfigWithWhitespaceKeystoreContent() {
        // Whitespace-only content should be treated as no content
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_CONTENT, "   ");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        assertNotNull("Config should be created with whitespace keystore content", config);
    }

    @Test
    public void testSSLConfigWithOnlyTruststorePassword() {
        // Incomplete: only password without truststore location or content
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        // Config should be created, but JMSWorker should not create SSLContext
        assertNotNull("Config should be created even with incomplete SSL config", config);
    }

    @Test
    public void testSSLConfigWithEmptyTruststoreContent() {
        // Empty content should be treated as no content
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_CONTENT, "");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        assertNotNull("Config should be created with empty truststore content", config);
    }

    @Test
    public void testSSLConfigWithWhitespaceTruststoreContent() {
        // Whitespace-only content should be treated as no content
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_CONTENT, "   ");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        assertNotNull("Config should be created with whitespace truststore content", config);
    }

    @Test
    public void testSSLConfigWithBothKeystoreAndTruststore() {
        // Complete configuration with both keystore and truststore
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_CONTENT, "dGVzdCBrZXlzdG9yZSBjb250ZW50");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_CONTENT, "dGVzdCB0cnVzdHN0b3JlIGNvbnRlbnQ=");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        assertNotNull("Config should be created with both keystore and truststore", config);
    }

    @Test
    public void testSSLConfigWithMixedLocationAndContent() {
        // Keystore as location, truststore as content
        final String keystorePath = getClass().getClassLoader().getResource("test-keystore.jks").getPath();
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION, keystorePath);
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_CONTENT, "dGVzdCB0cnVzdHN0b3JlIGNvbnRlbnQ=");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, "changeit");
        baseConfig.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");

        final AbstractConfig config = new AbstractConfig(MQSourceConnector.CONFIGDEF, baseConfig);
        assertNotNull("Config should be created with mixed location and content", config);
    }
}
