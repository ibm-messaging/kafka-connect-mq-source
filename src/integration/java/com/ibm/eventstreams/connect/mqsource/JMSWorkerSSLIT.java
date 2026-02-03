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

import com.ibm.eventstreams.connect.mqsource.utils.MQTestUtil;
import com.ibm.mq.jms.MQConnectionFactory;

import org.apache.kafka.common.config.types.Password;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSException;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Integration tests for SSL configuration with keystore/truststore content (base64-encoded).
 * These tests verify that the SSL context is properly built and can be used with MQ connections.
 * Uses SSL-enabled MQ container with proper certificates.
 */
public class JMSWorkerSSLIT extends AbstractSSLJMSContextIT {
    private JMSWorker jmsWorker;

    @Before
    public void setUpJMSWorker() {
        jmsWorker = new JMSWorker();
    }

    @After
    public void cleanUp() {
        try {
            if (jmsWorker != null) {
                try {
                    jmsWorker.stop();
                } catch (Exception e) {
                    // Ignore errors during cleanup if worker wasn't fully initialized
                }
            }
            MQTestUtil.removeAllMessagesFromQueue(DEFAULT_STATE_QUEUE);
        } catch (JMSException e) {
            System.out.println("Could not clean up: " + e.getMessage());
        }
    }

    @Test
    public void testSSLContextBuilderWithKeystoreContent() throws Exception {
        // Load client keystore and convert to base64
        final String base64Keystore = encodeFileToBase64(CLIENT_KEYSTORE_PATH);
        
        final SSLContextBuilder builder = new SSLContextBuilder();
        final Password keystoreContent = new Password(base64Keystore);
        final Password keystorePassword = new Password(KEYSTORE_PASSWORD);
        
        // Build SSL context with keystore content
        final SSLContext sslContext = builder.buildSslContext(
            null, keystorePassword, keystoreContent,
            null, null, null
        );
        
        assertNotNull("SSLContext should be created with keystore content", sslContext);
        assertThat(sslContext.getProtocol()).isEqualTo("TLS");
    }

    @Test
    public void testSSLContextBuilderWithTruststoreContent() throws Exception {
        // Load truststore and convert to base64
        final String base64Truststore = encodeFileToBase64(TRUSTSTORE_PATH);
        
        final SSLContextBuilder builder = new SSLContextBuilder();
        final Password truststoreContent = new Password(base64Truststore);
        final Password truststorePassword = new Password(KEYSTORE_PASSWORD);
        
        // Build SSL context with truststore content
        final SSLContext sslContext = builder.buildSslContext(
            null, null, null,
            null, truststorePassword, truststoreContent
        );
        
        assertNotNull("SSLContext should be created with truststore content", sslContext);
        assertThat(sslContext.getProtocol()).isEqualTo("TLS");
    }

    @Test
    public void testSSLContextBuilderWithBothKeystoreAndTruststoreContent() throws Exception {
        // Load both keystore and truststore and convert to base64
        final String base64Keystore = encodeFileToBase64(CLIENT_KEYSTORE_PATH);
        final String base64Truststore = encodeFileToBase64(TRUSTSTORE_PATH);
        
        final SSLContextBuilder builder = new SSLContextBuilder();
        final Password keystoreContent = new Password(base64Keystore);
        final Password keystorePassword = new Password(KEYSTORE_PASSWORD);
        final Password truststoreContent = new Password(base64Truststore);
        final Password truststorePassword = new Password(KEYSTORE_PASSWORD);
        
        // Build SSL context with both keystore and truststore content
        final SSLContext sslContext = builder.buildSslContext(
            null, keystorePassword, keystoreContent,
            null, truststorePassword, truststoreContent
        );
        
        assertNotNull("SSLContext should be created with both keystore and truststore content", sslContext);
        assertThat(sslContext.getProtocol()).isEqualTo("TLS");
    }

    @Test
    public void testJMSWorkerConfigureWithBothKeystoreAndTruststoreContent() throws Exception {
        final Map<String, String> props = getSSLConnectorProperties();
        
        // Load both keystore and truststore and convert to base64
        final String base64Keystore = encodeFileToBase64(CLIENT_KEYSTORE_PATH);
        final String base64Truststore = encodeFileToBase64(TRUSTSTORE_PATH);
        
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_CONTENT, base64Keystore);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, KEYSTORE_PASSWORD);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_CONTENT, base64Truststore);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD);
        
        jmsWorker.configure(getPropertiesConfig(props));
        jmsWorker.connect();
        
        // Verify SSL is configured
        MQConnectionFactory cf = jmsWorker.getConnectionFactory();
        assertNotNull("SSL cipher suite should be configured", cf.getSSLCipherSuite());
        assertEquals("SSL cipher suite should match configured value",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", cf.getSSLCipherSuite());
    }

    @Test
    public void testJMSWorkerConfigureWithEmptyKeystoreContent() throws JMSException {
        final Map<String, String> props = getDefaultConnectorProperties();
        
        // Empty content should not create SSL context
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_CONTENT, "");
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, "changeit");
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");
        
        jmsWorker.configure(getPropertiesConfig(props));
        
        // Verify SSL is NOT configured (don't connect, just check configuration)
        MQConnectionFactory cf = jmsWorker.getConnectionFactory();
        assertNull("SSL cipher suite should be null when keystore content is empty", cf.getSSLCipherSuite());
    }

    @Test
    public void testJMSWorkerConfigureWithWhitespaceKeystoreContent() throws JMSException {
        final Map<String, String> props = getDefaultConnectorProperties();
        
        // Whitespace-only content should not create SSL context
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_CONTENT, "   ");
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, "changeit");
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");
        
        jmsWorker.configure(getPropertiesConfig(props));
        
        // Verify SSL is NOT configured (don't connect, just check configuration)
        MQConnectionFactory cf = jmsWorker.getConnectionFactory();
        assertNull("SSL cipher suite should be null when keystore content is whitespace", cf.getSSLCipherSuite());
    }

    @Test
    public void testJMSWorkerConfigureWithOnlyKeystorePassword() throws JMSException {
        final Map<String, String> props = getDefaultConnectorProperties();
        
        // Only password without content or location should not create SSL context
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, "changeit");
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");
        
        jmsWorker.configure(getPropertiesConfig(props));
        
        // Verify SSL is NOT configured (don't connect, just check configuration)
        MQConnectionFactory cf = jmsWorker.getConnectionFactory();
        assertNull("SSL cipher suite should be null when only password is provided", cf.getSSLCipherSuite());
    }

    @Test
    public void testJMSWorkerConfigureWithBothLocationFiles() throws Exception {
        final Map<String, String> props = getSSLConnectorProperties();
        
        // Use both keystore and truststore as file locations (traditional approach)
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION, CLIENT_KEYSTORE_PATH);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, KEYSTORE_PASSWORD);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION, TRUSTSTORE_PATH);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD);
        
        jmsWorker.configure(getPropertiesConfig(props));
        jmsWorker.connect();
        
        // Verify SSL is configured
        MQConnectionFactory cf = jmsWorker.getConnectionFactory();
        assertNotNull("SSL cipher suite should be configured", cf.getSSLCipherSuite());
        assertEquals("SSL cipher suite should match configured value",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", cf.getSSLCipherSuite());
    }

     @Test
    public void testJMSWorkerConfigureWithKeystoreContentAndTruststoreLocation() throws Exception {
        final Map<String, String> props = getSSLConnectorProperties();
        
        // Load client keystore and convert to base64
        final String base64Keystore = encodeFileToBase64(CLIENT_KEYSTORE_PATH);
        
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_CONTENT, base64Keystore);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, KEYSTORE_PASSWORD);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION, TRUSTSTORE_PATH);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD);
        
        jmsWorker.configure(getPropertiesConfig(props));
        jmsWorker.connect();
        
        // Verify SSL is configured
        MQConnectionFactory cf = jmsWorker.getConnectionFactory();
        assertNotNull("SSL cipher suite should be configured", cf.getSSLCipherSuite());
        assertEquals("SSL cipher suite should match configured value",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", cf.getSSLCipherSuite());
    }


    @Test
    public void testJMSWorkerConfigureWithKeystoreLocationAndTruststoreContent() throws Exception {
        final Map<String, String> props = getSSLConnectorProperties();
        
        // Keystore as location, truststore as content (this is valid - different stores)
        final String base64Truststore = encodeFileToBase64(TRUSTSTORE_PATH);
        
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION, CLIENT_KEYSTORE_PATH);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, KEYSTORE_PASSWORD);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_CONTENT, base64Truststore);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD);
        
        jmsWorker.configure(getPropertiesConfig(props));
        jmsWorker.connect();
        
        // Verify SSL is configured
        MQConnectionFactory cf = jmsWorker.getConnectionFactory();
        assertNotNull("SSL cipher suite should be configured", cf.getSSLCipherSuite());
        assertEquals("SSL cipher suite should match configured value",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", cf.getSSLCipherSuite());
    }

    @Test
    public void testValidationRejectsBothKeystoreLocationAndContent() throws Exception {
        // This test verifies that MQSourceConnector validation prevents both location and content
        // from being specified for the same store (keystore or truststore)
        final Map<String, String> props = getDefaultConnectorProperties();
        
        final String base64Keystore = encodeFileToBase64(CLIENT_KEYSTORE_PATH);
        
        // Try to configure both location and content for keystore (should be rejected by validation)
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION, CLIENT_KEYSTORE_PATH);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_CONTENT, base64Keystore);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, KEYSTORE_PASSWORD);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");
        
        // Validate the configuration
        final MQSourceConnector connector = new MQSourceConnector();
        final org.apache.kafka.common.config.Config validationResult = connector.validate(props);
        
        // Check that validation found errors for both keystore location and content
        boolean foundKeystoreLocationError = false;
        boolean foundKeystoreContentError = false;
        
        for (org.apache.kafka.common.config.ConfigValue configValue : validationResult.configValues()) {
            if (configValue.name().equals(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION)
                && !configValue.errorMessages().isEmpty()) {
                foundKeystoreLocationError = true;
                assertThat(configValue.errorMessages().get(0))
                    .contains("Cannot specify both")
                    .contains("mq.ssl.keystore.location")
                    .contains("mq.ssl.keystore.content");
            }
            if (configValue.name().equals(MQSourceConnector.CONFIG_NAME_MQ_SSL_KEYSTORE_CONTENT)
                && !configValue.errorMessages().isEmpty()) {
                foundKeystoreContentError = true;
                assertThat(configValue.errorMessages().get(0))
                    .contains("Cannot specify both")
                    .contains("mq.ssl.keystore.location")
                    .contains("mq.ssl.keystore.content");
            }
        }
        
        assertThat(foundKeystoreLocationError)
            .as("Validation should reject keystore location when content is also provided")
            .isTrue();
        assertThat(foundKeystoreContentError)
            .as("Validation should reject keystore content when location is also provided")
            .isTrue();
    }

    @Test
    public void testValidationRejectsBothTruststoreLocationAndContent() throws Exception {
        // This test verifies that MQSourceConnector validation prevents both location and content
        // from being specified for the truststore
        final Map<String, String> props = getDefaultConnectorProperties();
        
        final String base64Truststore = encodeFileToBase64(TRUSTSTORE_PATH);
        
        // Try to configure both location and content for truststore (should be rejected by validation)
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION, TRUSTSTORE_PATH);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_CONTENT, base64Truststore);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD);
        props.put(MQSourceConnector.CONFIG_NAME_MQ_SSL_CIPHER_SUITE, "TLS_RSA_WITH_AES_128_CBC_SHA256");
        
        // Validate the configuration
        final MQSourceConnector connector = new MQSourceConnector();
        final org.apache.kafka.common.config.Config validationResult = connector.validate(props);
        
        // Check that validation found errors for both truststore location and content
        boolean foundTruststoreLocationError = false;
        boolean foundTruststoreContentError = false;
        
        for (org.apache.kafka.common.config.ConfigValue configValue : validationResult.configValues()) {
            if (configValue.name().equals(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION)
                && !configValue.errorMessages().isEmpty()) {
                foundTruststoreLocationError = true;
                assertThat(configValue.errorMessages().get(0))
                    .contains("Cannot specify both")
                    .contains("mq.ssl.truststore.location")
                    .contains("mq.ssl.truststore.content");
            }
            if (configValue.name().equals(MQSourceConnector.CONFIG_NAME_MQ_SSL_TRUSTSTORE_CONTENT)
                && !configValue.errorMessages().isEmpty()) {
                foundTruststoreContentError = true;
                assertThat(configValue.errorMessages().get(0))
                    .contains("Cannot specify both")
                    .contains("mq.ssl.truststore.location")
                    .contains("mq.ssl.truststore.content");
            }
        }
        
        assertThat(foundTruststoreLocationError)
            .as("Validation should reject truststore location when content is also provided")
            .isTrue();
        assertThat(foundTruststoreContentError)
            .as("Validation should reject truststore content when location is also provided")
            .isTrue();
    }

    /**
     * Helper method to encode a file to base64 string.
     */
    private String encodeFileToBase64(final String filePath) throws IOException {
        final byte[] fileContent = Files.readAllBytes(Paths.get(filePath));
        return Base64.getEncoder().encodeToString(fileContent);
    }

}
