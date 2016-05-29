package uk.co.optimisticpanda.kafka;

import static org.assertj.core.api.Assertions.assertThat;

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
			
			AtomicInteger count = new AtomicInteger();
			
			consumer.startListening(message -> count.incrementAndGet());
			
			producer.sendMessage();
			producer.sendMessage();
			producer.sendMessage();

			while(count.get() < 3) {
				consumer.poll(message -> count.incrementAndGet());
			}
			
			assertThat(count.get()).isEqualTo(3);
		}
	}
}