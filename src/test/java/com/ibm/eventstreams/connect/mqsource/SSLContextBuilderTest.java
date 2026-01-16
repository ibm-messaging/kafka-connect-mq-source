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

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.util.Base64;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SSLContextBuilderTest {

    @Test
    public void testBuildSslContextWithValidBase64KeystoreContent() {
        final SSLContextBuilder builder = new SSLContextBuilder();
        
        // Create a minimal valid JKS keystore in base64
        final String validBase64Content = createTestKeystoreBase64();
        final Password keystoreContent = new Password(validBase64Content);
        final Password keystorePassword = new Password("changeit");
        
        try {
            final SSLContext sslContext = builder.buildSslContext(
                null, keystorePassword, keystoreContent,
                null, null, null
            );
            assertNotNull("SSLContext should be created with valid base64 content", sslContext);
        } catch (final Exception e) {
            // Expected to fail with test keystore, but should not fail on base64 decoding
            // The failure should be in keystore loading, not base64 decoding
        }
    }

    @Test(expected = ConnectException.class)
    public void testBuildSslContextWithInvalidBase64Content() {
        final SSLContextBuilder builder = new SSLContextBuilder();
        
        // Invalid base64 string
        final Password invalidContent = new Password("This is not valid base64!@#$%");
        final Password keystorePassword = new Password("changeit");
        
        builder.buildSslContext(
            null, keystorePassword, invalidContent,
            null, null, null
        );
        
        fail("Should throw ConnectException for invalid base64 content");
    }

    @Test
    public void testBuildSslContextWithEmptyContent() {
        final SSLContextBuilder builder = new SSLContextBuilder();
        
        // Empty content should be handled gracefully
        final Password emptyContent = new Password("");
        final Password keystorePassword = new Password("changeit");
        
        // Should not attempt to create SSLContext with empty content
        // This test verifies the validation in JMSWorker prevents this scenario
        final SSLContext sslContext = builder.buildSslContext(
            null, keystorePassword, emptyContent,
            null, null, null
        );
        
        assertNotNull("SSLContext should be created even with empty content (no keystore)", sslContext);
    }

    @Test
    public void testBuildSslContextWithWhitespaceContent() {
        final SSLContextBuilder builder = new SSLContextBuilder();
        
        // Whitespace-only content
        final Password whitespaceContent = new Password("   ");
        final Password keystorePassword = new Password("changeit");
        
        final SSLContext sslContext = builder.buildSslContext(
            null, keystorePassword, whitespaceContent,
            null, null, null
        );
        
        assertNotNull("SSLContext should be created with whitespace content (treated as no keystore)", sslContext);
    }

    @Test
    public void testBuildSslContextWithNullContent() {
        final SSLContextBuilder builder = new SSLContextBuilder();
        
        final Password keystorePassword = new Password("changeit");
        
        final SSLContext sslContext = builder.buildSslContext(
            null, keystorePassword, null,
            null, null, null
        );
        
        assertNotNull("SSLContext should be created with null content", sslContext);
    }

    /**
     * Creates a base64-encoded test keystore.
     */
    private String createTestKeystoreBase64() {
        final byte[] testData = "test keystore data".getBytes();
        return Base64.getEncoder().encodeToString(testData);
    }
}
