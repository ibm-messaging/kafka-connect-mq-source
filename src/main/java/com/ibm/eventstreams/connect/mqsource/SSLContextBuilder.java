/**
 * Copyright 2023, 2024, 2026 IBM Corporation
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

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.config.types.Password;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;

import java.util.Base64;

public class SSLContextBuilder {

    private static final Logger log = LoggerFactory.getLogger(SSLContextBuilder.class);

    public SSLContext buildSslContext(final String sslKeystoreLocation, final Password sslKeystorePassword,
                                       final Password sslKeystoreContent,
                                       final String sslTruststoreLocation, final Password sslTruststorePassword,
                                       final Password sslTruststoreContent) {
        log.trace("[{}] Entry {}.buildSslContext", Thread.currentThread().getId(), this.getClass().getName());

        try {
            KeyManager[] keyManagers = null;
            TrustManager[] trustManagers = null;

            if (sslKeystoreContent != null && sslKeystoreContent.value() != null && !sslKeystoreContent.value().trim().isEmpty()) {
                final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(loadKeyStoreFromContent(sslKeystoreContent.value(), sslKeystorePassword.value()), sslKeystorePassword.value().toCharArray());
                keyManagers = kmf.getKeyManagers();
            } else if (sslKeystoreLocation != null) {
                final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(loadKeyStoreFromFile(sslKeystoreLocation, sslKeystorePassword.value()), sslKeystorePassword.value().toCharArray());
                keyManagers = kmf.getKeyManagers();
            }

            if (sslTruststoreContent != null && sslTruststoreContent.value() != null && !sslTruststoreContent.value().trim().isEmpty()) {
                final TrustManagerFactory tmf = TrustManagerFactory
                        .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(loadKeyStoreFromContent(sslTruststoreContent.value(), sslTruststorePassword.value()));
                trustManagers = tmf.getTrustManagers();
            } else if (sslTruststoreLocation != null) {
                final TrustManagerFactory tmf = TrustManagerFactory
                        .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(loadKeyStoreFromFile(sslTruststoreLocation, sslTruststorePassword.value()));
                trustManagers = tmf.getTrustManagers();
            }

            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, trustManagers, new SecureRandom());

            log.trace("[{}]  Exit {}.buildSslContext, retval={}", Thread.currentThread().getId(),
                    this.getClass().getName(), sslContext);
            return sslContext;
        } catch (final GeneralSecurityException e) {
            throw new ConnectException("Error creating SSLContext", e);
        }
    }

    private KeyStore loadKeyStoreFromFile(final String location, final String password) throws GeneralSecurityException {
        log.trace("[{}] Entry {}.loadKeyStoreFromFile", Thread.currentThread().getId(), this.getClass().getName());

        try (final InputStream ksStr = new FileInputStream(location)) {
            final KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(ksStr, password.toCharArray());

            log.trace("[{}]  Exit {}.loadKeyStoreFromFile, retval={}", Thread.currentThread().getId(),
                    this.getClass().getName(), ks);
            return ks;
        } catch (final IOException e) {
            throw new ConnectException("Error reading keystore from file " + location, e);
        }
    }

    private KeyStore loadKeyStoreFromContent(final String content, final String password) throws GeneralSecurityException {
        log.trace("[{}] Entry {}.loadKeyStoreFromContent", Thread.currentThread().getId(), this.getClass().getName());

        try {
            // Decode from base64 string
            // Works for both inline YAML configs and config providers with base64-encoded files
            log.debug("Decoding keystore content from base64");
            final byte[] keystoreBytes = Base64.getDecoder().decode(content);
            
            try (final InputStream ksStr = new ByteArrayInputStream(keystoreBytes)) {
                final KeyStore ks = KeyStore.getInstance("JKS");
                ks.load(ksStr, password.toCharArray());

                log.trace("[{}]  Exit {}.loadKeyStoreFromContent, retval={}", Thread.currentThread().getId(),
                        this.getClass().getName(), ks);
                return ks;
            }
        } catch (final IllegalArgumentException e) {
            throw new ConnectException("Invalid base64 encoding in keystore content", e);
        } catch (final IOException e) {
            throw new ConnectException("Error reading keystore from content", e);
        }
    }
}
