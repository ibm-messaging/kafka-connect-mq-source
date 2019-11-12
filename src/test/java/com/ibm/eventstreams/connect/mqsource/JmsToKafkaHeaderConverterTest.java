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

		boolean messageBodyJms = true;
		List<String> keys = Arrays.asList("facilityCountryCode", "facilityNum");

		Enumeration<String> keyEnumeration = Collections.enumeration(keys);

		//Arrange
		when(message.getPropertyNames()).thenReturn(keyEnumeration);
		when(message.getObjectProperty("facilityCountryCode")).thenReturn("US");
		when(message.getObjectProperty("facilityNum")).thenReturn("12345");

		//Act
		ConnectHeaders actualConnectHeaders = jmsToKafkaHeaderConverter.convertJmsPropertiesToKafkaHeaders(messageBodyJms, message);


		//Verify
		assertEquals("Both custom JMS properties were copied to kafka successfully.", 2, actualConnectHeaders.size());


	}
}