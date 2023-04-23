/**
 * Copyright 2022 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsource.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Base64;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.json.JSONException;
import org.json.JSONObject;

public class JsonRestApi {

    public static JSONObject jsonPost(final String url, final String username, final String password,
            final String payload) throws IOException, KeyManagementException, NoSuchAlgorithmException, JSONException {
        final URL urlObj = new URL(url);
        final HttpsURLConnection urlConnection = (HttpsURLConnection) urlObj.openConnection();
        urlConnection.setHostnameVerifier(new IgnoreCertVerifier());
        urlConnection.setSSLSocketFactory(getTrustAllCertsFactory());
        urlConnection.setRequestProperty("Authorization", getAuthHeader(username, password));
        urlConnection.setRequestProperty("Content-Type", "application/json");
        urlConnection.setRequestProperty("ibm-mq-rest-csrf-token", "junit");
        urlConnection.setDoOutput(true);

        try (OutputStream os = urlConnection.getOutputStream()) {
            final byte[] input = payload.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        try (InputStream input = urlConnection.getInputStream()) {
            final BufferedReader re = new BufferedReader(new InputStreamReader(input, Charset.forName("utf-8")));
            return new JSONObject(read(re));
        }
    }

    private static String read(final Reader re) throws IOException {
        final StringBuilder str = new StringBuilder();
        int ch;
        do {
            ch = re.read();
            str.append((char) ch);
        } while (ch != -1);
        return str.toString();
    }

    private static String getAuthHeader(final String username, final String password) {
        final String userpass = username + ":" + password;
        final String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
        return basicAuth;
    }

    private static class IgnoreCertVerifier implements HostnameVerifier {
        @Override
        public boolean verify(final String host, final SSLSession session) {
            return true;
        }
    }

    private static SSLSocketFactory getTrustAllCertsFactory() throws NoSuchAlgorithmException, KeyManagementException {
        final TrustManager[] trustAllCerts = new TrustManager[] {
            new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(final X509Certificate[] certs, final String authType) {
                }

                public void checkServerTrusted(final X509Certificate[] certs, final String authType) {
                }
            }
        };
        final SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        return sc.getSocketFactory();
    }
}
