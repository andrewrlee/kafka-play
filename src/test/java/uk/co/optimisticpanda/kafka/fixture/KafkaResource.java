package uk.co.optimisticpanda.kafka.fixture;

import java.io.FileInputStream;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import org.junit.rules.ExternalResource;
 
class KafkaResource extends ExternalResource {
 
	public KafkaServerStartable kafka;
	
	@Override
	protected void before() throws Throwable {
		Properties kafkaProperties = new Properties();
		kafkaProperties.load(new FileInputStream("/home/alee/bin/kafka_2.11-0.10.0.0/config/server.properties"));
		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
		kafka = new KafkaServerStartable(kafkaConfig);
		kafka.startup();
	}

	@Override
	protected void after() {
		//stop kafka broker
		System.out.println("stopping kafka...");
		kafka.shutdown();
	};
	
}