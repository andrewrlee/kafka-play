package uk.co.optimisticpanda.kafka.fixture;

import static uk.co.optimisticpanda.kafka.Utils.ThrowingSupplier.propagateAnyErrorSupplier;

import java.util.function.Supplier;

import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import uk.co.optimisticpanda.kafka.Utils;
import uk.co.optimisticpanda.kafka.Utils.MemoizeSupplier;

public class KafkaFixture implements TestRule {

	private final ZookeeperResource zookeeperResource;
	private final TemporaryFolder temporaryFolder;
	private final KafkaResource kafkaResource;

	public static Builder build() {
		return new Builder();
	}
	
	public static KafkaFixture create() {
		return build().create();
	}
	
	private KafkaFixture(int zookeeperPort, int kafkaPort) {
		this.temporaryFolder = new TemporaryFolder();
		this.zookeeperResource = new ZookeeperResource(
				zookeeperPort,
				propagateAnyErrorSupplier(() -> temporaryFolder.newFolder("zookeeper")));
		this.kafkaResource = new KafkaResource(
				kafkaPort,
				zookeeperResource::getZookeeperConnect,
				propagateAnyErrorSupplier(() -> temporaryFolder.newFolder("kafka")));
	}

	public String getKafkaConnect() {
		return kafkaResource.getKafkaConnect();
	}

	@Override
	public Statement apply(Statement base, Description description) {
		return RuleChain.outerRule(temporaryFolder).around(zookeeperResource)
				.around(kafkaResource).apply(base, description);
	}

	public static class Builder {

		private Integer kafkaPort;
		private Integer zookeeperPort;

		private Builder() {
		}

		public Builder kafkaPort(int value) {
			this.kafkaPort = value;
			return this;
		}

		public Builder zookeeperPort(int value) {
			this.zookeeperPort = value;
			return this;
		}

		public KafkaFixture create() {
			Supplier<int[]> freePorts = MemoizeSupplier.of(() -> Utils.getFreePorts(2));
			
			if (zookeeperPort == null) {
				zookeeperPort = freePorts.get()[0];
			}
			if (kafkaPort == null) {
				kafkaPort = freePorts.get()[1];
			}
			return new KafkaFixture(zookeeperPort, kafkaPort);
		}
	}
}
