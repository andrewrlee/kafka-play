package uk.co.optimisticpanda.kafka.fixture;

import java.io.File;
import java.util.Properties;
import java.util.function.Supplier;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import org.junit.rules.ExternalResource;

class KafkaResource extends ExternalResource {

	private final Supplier<File> logFolder;
	public KafkaServerStartable kafka;

	public KafkaResource(Supplier<File> logFolder) {
		this.logFolder = logFolder;
	}

	@Override
	protected void before() throws Throwable {
		Properties properties = new Properties();
		properties.put("num.io.threads", "8");
		properties.put("log.dirs", logFolder.get().getAbsolutePath());
		properties.put("advertised.host.name", "localhost");
		properties.put("zookeeper.connect", "localhost:2181");
		properties.put("broker.id", "0");
		properties.put("zookeeper.connection.timeout.ms", "6000");

		KafkaConfig kafkaConfig = new KafkaConfig(properties);
		kafka = new KafkaServerStartable(kafkaConfig);
		kafka.startup();
	}

	@Override
	protected void after() {
		// stop kafka broker
		System.out.println("stopping kafka...");
		kafka.shutdown();
	};

}