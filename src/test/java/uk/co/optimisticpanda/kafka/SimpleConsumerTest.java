package uk.co.optimisticpanda.kafka;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Rule;
import org.junit.Test;

import uk.co.optimisticpanda.kafka.clients.Producer;
import uk.co.optimisticpanda.kafka.fixture.KafkaFixture;

public class SimpleConsumerTest {

	@Rule
	public KafkaFixture fixture = new KafkaFixture();

	@Test
	public void testSomething() throws Exception {
		Producer producer = new Producer("topic-1");

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
	    props.put("group.id", "test");
	    props.put("enable.auto.commit", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("session.timeout.ms", "30000");
	    
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props, new StringDeserializer(), new StringDeserializer())) {

			consumer.subscribe(singleton("topic-1"));

			consumer.poll(1000);
			
			producer.sendMessage();
			producer.sendMessage();
			producer.sendMessage();

			ConsumerRecords<String, String> records = consumer.poll(1000);
			
			List<String> receivedValues = new ArrayList<>();
			records.forEach(record -> receivedValues.add(record.value()));
			assertEquals(3, receivedValues.size());
		}
	}
}