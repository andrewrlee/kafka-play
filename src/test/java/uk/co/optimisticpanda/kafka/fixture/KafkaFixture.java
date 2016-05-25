package uk.co.optimisticpanda.kafka.fixture;

import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class KafkaFixture implements TestRule {

	public Statement apply(Statement base, Description description) {
		return RuleChain.outerRule(new ZookeeperResource())
				.around(new KafkaResource())
				.apply(base, description);
	}

}
