/**
 * Copyright 2019 IBM Corporation
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

import com.ibm.eventstreams.connect.mqsource.processor.JmsToKafkaHeaderConverter;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JmsToKafkaHeaderConverterTest {

    @Mock
    private TextMessage message;
    @InjectMocks
    private JmsToKafkaHeaderConverter jmsToKafkaHeaderConverter;

    @Test
    public void convertJmsPropertiesToKafkaHeaders() throws JMSException {

        List<String> keys = Arrays.asList("facilityCountryCode", "facilityNum");

        Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        //Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("facilityCountryCode")).thenReturn("US");
        when(message.getObjectProperty("facilityNum")).thenReturn("12345");

        //Act
        ConnectHeaders actualConnectHeaders = jmsToKafkaHeaderConverter.convertJmsPropertiesToKafkaHeaders(message);


        //Verify
        assertEquals("Both custom JMS properties were copied to kafka successfully.", 2, actualConnectHeaders.size());


    }
}