package uk.co.optimisticpanda.kafka.fixture;

import static uk.co.optimisticpanda.kafka.Utils.ThrowingRunnable.wrapAnyError;

import java.io.File;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.rules.ExternalResource;

public class ZookeeperResource extends ExternalResource {

	public final Supplier<File> logFolder;
	private int port;

	public ZookeeperResource(int port, Supplier<File> logFolder) {
		this.logFolder = logFolder;
		this.port = port;
	}

	@Override
	protected void before() throws Throwable {
		new Thread(this::run).start();
	}
	
	public void run() {
		Properties properties = new Properties();
		properties.put("clientPort", port);
		properties.put("dataDir", logFolder.get().getAbsolutePath());

		QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
		wrapAnyError(() -> quorumConfiguration.parseProperties(properties));

		ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
		ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfiguration);
		wrapAnyError(() -> zooKeeperServer.runFromConfig(configuration));
	}
	
	public String getZookeeperConnect() {
		return "localhost:" + port;
	}
}
