package uk.co.optimisticpanda.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;

import uk.co.optimisticpanda.kafka.clients.SimpleConsumer;
import uk.co.optimisticpanda.kafka.clients.SimpleProducer;
import uk.co.optimisticpanda.kafka.fixture.KafkaFixture;

public class MyTest {
 
	@Rule
	public KafkaFixture fixture = new KafkaFixture();
	
	@Test
	public void testSomething() throws Exception {
		SimpleProducer producer = new SimpleProducer("topic-1");
		
		try (SimpleConsumer consumer = new SimpleConsumer("localhost:2181", "1", "topic-1")){
			producer.sendMessage();
			producer.sendMessage();
			producer.sendMessage();
			Thread.sleep(10000);
		}
	}
	
 
}