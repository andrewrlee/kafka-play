package uk.co.optimisticpanda.kafka.fixture;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.rules.ExternalResource;

public class ZookeeperResource extends ExternalResource {

	public final Supplier<File> logFolder;

	public ZookeeperResource(Supplier<File> logFolder) {
		this.logFolder = logFolder;
	}

	@Override
	protected void before() throws Throwable {

		Properties properties = new Properties();
		properties.put("clientPort", 2181);
		properties.put("maxClientCnxns", 0);
		properties.put("dataDir", logFolder.get().getAbsolutePath());
		
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
