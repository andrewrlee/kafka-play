package uk.co.optimisticpanda.kafka.fixture;

import java.io.File;
import java.util.Properties;
import java.util.function.Supplier;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import org.junit.rules.ExternalResource;

class KafkaResource extends ExternalResource {

	private final Supplier<File> logFolder;
	private final Supplier<String> zookeeperConnect;
	private final int listenPort;
	public KafkaServerStartable kafka;

	public KafkaResource(int listenPort, Supplier<String> zookeeperConnect, Supplier<File> logFolder) {
		this.listenPort = listenPort;
		this.zookeeperConnect = zookeeperConnect;
		this.logFolder = logFolder;
	}

	@Override
	protected void before() throws Throwable {
		Properties properties = new Properties();
		properties.put("log.dirs", logFolder.get().getAbsolutePath());
		properties.put("zookeeper.connect", zookeeperConnect.get());
		properties.put("listeners", "PLAINTEXT://0.0.0.0:" + listenPort);

		KafkaConfig kafkaConfig = new KafkaConfig(properties);
		kafka = new KafkaServerStartable(kafkaConfig);
		kafka.startup();
	}

	public String getKafkaConnect() {
		return "localhost:" + listenPort;
	}
	
	@Override
	protected void after() {
		kafka.shutdown();
	};

}