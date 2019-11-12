package com.ibm.eventstreams.connect.mqsource.processor;

import org.apache.kafka.connect.header.ConnectHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import java.util.ArrayList;
import java.util.Collections;

/*
 * Single responsibility class to copy JMS properties to Kafka headers.
 *
 * */
public class JmsToKafkaHeaderConverter {

	private static final Logger log = LoggerFactory.getLogger(JmsToKafkaHeaderConverter.class);

	/**
	 * Copies the JMS properties to Kafka headers.
	 *
	 * @param message        JMS message.
	 * @param messageBodyJms flag whether incoming message is treated as jms message.
	 * @return Kafka connect headers.
	 */
	public ConnectHeaders convertJmsPropertiesToKafkaHeaders(boolean messageBodyJms, Message message) {

		ConnectHeaders connectHeaders = new ConnectHeaders();

		if (messageBodyJms) {


			ArrayList jmsPropertyKeys = null;
			try {
				jmsPropertyKeys = Collections.list(message.getPropertyNames());
				jmsPropertyKeys.forEach(key -> {
					try {
						connectHeaders.addString(key.toString(), message.getObjectProperty(key.toString()).toString());
					} catch (JMSException e) {
						//Not failing the message processing if JMS properties cannot be read for some reason.
						log.error("JMS message properties could not be read", e);
					}
				});
			} catch (JMSException e) {
				//Not failing the message processing if JMS properties cannot be read for some reason.
				log.error("JMS message properties could not be read", e);
			}

		}

		return connectHeaders;

	}
}
