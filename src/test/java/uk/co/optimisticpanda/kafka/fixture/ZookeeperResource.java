package uk.co.optimisticpanda.kafka.fixture;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.rules.ExternalResource;

public class ZookeeperResource extends ExternalResource {

	@Override
	protected void before() throws Throwable {

		Properties properties = new Properties();
		properties.load(new FileInputStream(
						"/home/alee/bin/kafka_2.11-0.10.0.0/config/zookeeper.properties"));

		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		try {
			quorumConfiguration.parseProperties(properties);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		final ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
		final ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);

		new Thread() {
			public void run() {
				try {
					zooKeeperServer.runFromConfig(configuration);
				} catch (IOException e) {
					System.out.println("ZooKeeper Failed");
					e.printStackTrace(System.err);
				}
			}
		}.start();
	}
}
