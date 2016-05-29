package uk.co.optimisticpanda.kafka;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;

import uk.co.optimisticpanda.kafka.clients.Producer;
import uk.co.optimisticpanda.kafka.clients.SimpleConsumer;
import uk.co.optimisticpanda.kafka.fixture.KafkaFixture;

public class SimpleConsumerTest {

	@Rule
	public final KafkaFixture fixture = KafkaFixture.create();

	@Test(timeout= 10_000)
	public void checkProducingAndConsuming() throws Exception {
		Producer producer = new Producer(fixture.getKafkaConnect(), "topic-1");
		
		try (SimpleConsumer consumer = new SimpleConsumer(fixture.getKafkaConnect(), "topic-1")) {
			
			consumer.startListening(message -> { });
			
			producer.sendMessage();
			producer.sendMessage();
			producer.sendMessage();

			AtomicInteger count = new AtomicInteger();
			
			while(count.get() < 3) {
				consumer.poll(message -> count.incrementAndGet());
			}
		}
	}
}