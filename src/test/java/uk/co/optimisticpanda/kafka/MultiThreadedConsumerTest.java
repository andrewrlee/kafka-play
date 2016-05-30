package uk.co.optimisticpanda.kafka;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;

import uk.co.optimisticpanda.kafka.clients.MultiThreadedConsumer;
import uk.co.optimisticpanda.kafka.clients.Producer;
import uk.co.optimisticpanda.kafka.fixture.KafkaFixture;

public class MultiThreadedConsumerTest {

	@Rule
	public final KafkaFixture fixture = KafkaFixture.create();

	@Test
	public void checkProducingAndConsuming() throws Exception {
		Producer producer = new Producer(fixture.getKafkaConnect(), "topic-1");
		
		try (MultiThreadedConsumer consumer = new MultiThreadedConsumer(fixture.getKafkaConnect(), "topic-1")) {
			
			CountDownLatch latch = new CountDownLatch(3);
			AtomicInteger count = new AtomicInteger();
			
			consumer.run(2, message -> {
				count.incrementAndGet();
				latch.countDown();
			});
			
			producer.sendMessage();
			producer.sendMessage();
			producer.sendMessage();

			latch.await(10, SECONDS);
		
			assertThat(count.get()).isEqualTo(3);
		}
	}
}