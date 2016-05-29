package uk.co.optimisticpanda.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import uk.co.optimisticpanda.kafka.clients.SimpleConsumer;
import uk.co.optimisticpanda.kafka.clients.Producer;
import uk.co.optimisticpanda.kafka.fixture.KafkaFixture;

public class SimpleConsumerTest {

	@Rule
	public final KafkaFixture fixture = KafkaFixture.create();

	@Test
	public void checkProducingAndConsuming() throws Exception {
		Producer producer = new Producer(fixture.getKafkaConnect(), "topic-1");
		
		try (SimpleConsumer consumer = new SimpleConsumer(fixture.getKafkaConnect(), "topic-1")) {
			List<String> receivedValues = new ArrayList<>();
			
			consumer.startListening(receivedValues::add);
			
			producer.sendMessage();
			producer.sendMessage();
			producer.sendMessage();

			consumer.poll(receivedValues::add);
			
			assertThat(receivedValues).hasSize(3);
		}
	}
}