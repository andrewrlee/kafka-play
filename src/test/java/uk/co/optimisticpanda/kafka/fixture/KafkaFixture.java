package uk.co.optimisticpanda.kafka.fixture;

import static uk.co.optimisticpanda.kafka.Utils.ThrowingSupplier.wrapAnyError;

import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class KafkaFixture implements TestRule {

	public Statement apply(Statement base, Description description) {
		TemporaryFolder temporaryFolder = new TemporaryFolder();
		return RuleChain.outerRule(temporaryFolder)
				.around(new ZookeeperResource(wrapAnyError(() -> temporaryFolder.newFolder("zookeeper"))))
				.around(new KafkaResource(wrapAnyError(() -> temporaryFolder.newFolder("kafka"))))
				.apply(base, description);
	}

}
