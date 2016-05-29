package uk.co.optimisticpanda.kafka.clients;

import static java.util.Collections.singleton;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleConsumer implements Closeable {

	private final KafkaConsumer<String, String> consumer;

	public SimpleConsumer(String bootstrapServers, String topic) throws FileNotFoundException, IOException {
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
	    props.put("group.id", "test");
	    props.put("enable.auto.commit", "true");
	    props.put("auto.commit.interval.ms", "1000");
	    props.put("session.timeout.ms", "30000");
	    
		this.consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
		this.consumer.subscribe(singleton(topic));
	}
	
	public void startListening(final java.util.function.Consumer<String> messageConsumer) {
		poll(messageConsumer);
	}
	
	public void poll(final java.util.function.Consumer<String> messageConsumer) {
		ConsumerRecords<String, String> records = consumer.poll(1000);
		records.forEach(record -> messageConsumer.accept(record.value()));
	}

	@Override
	public void close() throws IOException {
		consumer.close();
	}
}
