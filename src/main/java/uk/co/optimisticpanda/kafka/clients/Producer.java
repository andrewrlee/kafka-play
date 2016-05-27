package uk.co.optimisticpanda.kafka.clients;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

	private final KafkaProducer<String, String> producer;
	private final String topic;
	private final Random rnd = new Random();

	public Producer(String topic) throws FileNotFoundException, IOException {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		 
		this.producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
		this.topic = topic;
	}
	
	public void sendMessage() {
		long runtime = new Date().getTime();
		String msg = "hello-" + runtime +  "-" + rnd.nextInt();
		producer.send(new ProducerRecord<>(topic, msg));
	}
}
