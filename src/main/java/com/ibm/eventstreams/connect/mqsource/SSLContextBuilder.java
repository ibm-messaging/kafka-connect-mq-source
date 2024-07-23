/**
 * Copyright 2023 IBM Corporation
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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;

public class SSLContextBuilder {

    private static final Logger log = LoggerFactory.getLogger(SSLContextBuilder.class);

    public SSLContext buildSslContext(final String sslKeystoreLocation, final Password sslKeystorePassword,
                                       final String sslTruststoreLocation, final Password sslTruststorePassword) {
        log.trace("[{}] Entry {}.buildSslContext", Thread.currentThread().getId(), this.getClass().getName());

        try {
            KeyManager[] keyManagers = null;
            TrustManager[] trustManagers = null;

            if (sslKeystoreLocation != null) {
                final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(loadKeyStore(sslKeystoreLocation, sslKeystorePassword.value()), sslKeystorePassword.value().toCharArray());
                keyManagers = kmf.getKeyManagers();
            }

            if (sslTruststoreLocation != null) {
                final TrustManagerFactory tmf = TrustManagerFactory
                        .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(loadKeyStore(sslTruststoreLocation, sslTruststorePassword.value()));
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

    private KeyStore loadKeyStore(final String location, final String password) throws GeneralSecurityException {
        log.trace("[{}] Entry {}.loadKeyStore", Thread.currentThread().getId(), this.getClass().getName());

        try (final InputStream ksStr = new FileInputStream(location)) {
            final KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(ksStr, password.toCharArray());

            log.trace("[{}]  Exit {}.loadKeyStore, retval={}", Thread.currentThread().getId(),
                    this.getClass().getName(), ks);
            return ks;
        } catch (final IOException e) {
            throw new ConnectException("Error reading keystore " + location, e);
        }
    }
}
