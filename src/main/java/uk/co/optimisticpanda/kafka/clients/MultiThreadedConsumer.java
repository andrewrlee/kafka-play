package uk.co.optimisticpanda.kafka.clients;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.IntStream.range;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiThreadedConsumer implements Closeable {
	private static final Logger L = LoggerFactory.getLogger(MultiThreadedConsumer.class);
	private final String topic;
	private final String bootstrapServers;
	private ExecutorService executor;

	public MultiThreadedConsumer(String bootstrapServers, String topic) {
		this.bootstrapServers = bootstrapServers;
		this.topic = topic;
	}

	public void run(int threads, Consumer<String> consumer) {
		executor = Executors.newFixedThreadPool(threads);

		Properties properties = new Properties();
		properties.put("bootstrap.servers", bootstrapServers);
	    properties.put("group.id", "group-1");
	    properties.put("enable.auto.commit", "true");
	    properties.put("auto.commit.interval.ms", "1000");
	    properties.put("session.timeout.ms", "30000");
		
		range(0, threads)
			.forEach(i -> executor.submit(new KafkaConsumerRunner(topic, properties, consumer)));
	}

	@Override
	public void close() {
		if (executor != null) {
			executor.shutdown();
		}
		try {
			if (!executor.awaitTermination(5000, MILLISECONDS)) {
				L.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			L.warn("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public static class KafkaConsumerRunner implements Runnable {
		private final AtomicBoolean closed = new AtomicBoolean(false);
		private final KafkaConsumer<String, String> kafkaConsumer;
		private final Consumer<String> consumer;

		public KafkaConsumerRunner(String topic, Properties properties, Consumer<String> consumer) {
			this.kafkaConsumer = new KafkaConsumer<String, String>(properties, new StringDeserializer(), new StringDeserializer());
			this.consumer = consumer;
			this.kafkaConsumer.subscribe(singletonList(topic));
			this.consume();
		}

		@Override
		public void run() {
			try {
				while (!closed.get()) {
					consume();
				}
			} catch (WakeupException e) {
				L.info("Woken up");
				if (!closed.get())
					throw e;
			} finally {
				L.info("Closing consumer");
				kafkaConsumer.close();
			}
		}

		private void consume() {
			kafkaConsumer.poll(1000)
				.forEach(row -> consumer.accept(row.value()));
		}

		public void shutdown() {
			closed.set(true);
			kafkaConsumer.wakeup();
		}
	}
}
