package uk.co.optimisticpanda.kafka.fixture;

import static uk.co.optimisticpanda.kafka.Utils.ThrowingSupplier.wrapAnyError;

import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class KafkaFixture implements TestRule {

	private ZookeeperResource zookeeperResource;
	private TemporaryFolder temporaryFolder;
	private KafkaResource kafkaResource;

	public KafkaFixture() {
		temporaryFolder = new TemporaryFolder();
		zookeeperResource = new ZookeeperResource(
				3818, 
				wrapAnyError(() -> temporaryFolder.newFolder("zookeeper")));
		kafkaResource = new KafkaResource(
				9092,
				zookeeperResource::getZookeeperConnect, 
				wrapAnyError(() -> temporaryFolder.newFolder("kafka")));
	}
	
	public String getKafkaConnect() {
		return kafkaResource.getKafkaConnect();
	}
	
	public Statement apply(Statement base, Description description) {
		return RuleChain.outerRule(temporaryFolder)
				.around(zookeeperResource)
				.around(kafkaResource)
				.apply(base, description);
	}

}
