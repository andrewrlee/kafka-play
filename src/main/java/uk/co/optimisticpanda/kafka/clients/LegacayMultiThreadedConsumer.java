package uk.co.optimisticpanda.kafka.clients;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;

public class LegacayMultiThreadedConsumer implements Closeable {

	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;

	public LegacayMultiThreadedConsumer(String a_zookeeper, String a_groupId, String a_topic) {
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(a_zookeeper,
						a_groupId));
		this.topic = a_topic;
	}

	@Override
	public void close() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null) {
			executor.shutdown();
			try {
				if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
					System.out
							.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
				}
			} catch (InterruptedException e) {
				System.out
						.println("Interrupted during shutdown, exiting uncleanly");
			}
		}
	}

	public void run(int threads) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(threads));
		
		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer
				.createMessageStreams(topicCountMap, new StringDecoder(null), new StringDecoder(null));
		List<KafkaStream<String, String>> streams = consumerMap.get(topic);

		executor = Executors.newFixedThreadPool(threads);

		// now create an object to consume the messages
		//
		int threadNumber = 0;
		for (final KafkaStream<String,String> stream : streams) {
			executor.submit(new Consumer(stream, threadNumber));
			threadNumber++;
		}
	}

	private static ConsumerConfig createConsumerConfig(String a_zookeeper,
			String a_groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", "1");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);
	}

	public static class Consumer implements Runnable {
		private KafkaStream<String,String> m_stream;
		private int m_threadNumber;

		public Consumer(KafkaStream<String,String> a_stream, int a_threadNumber) {
			m_threadNumber = a_threadNumber;
			m_stream = a_stream;
		}

		public void run() {
			ConsumerIterator<String,String> it = m_stream.iterator();
			while (it.hasNext())
				System.out.println(">>Thread " + m_threadNumber + ": "
						+ new String(it.next().message()));
			System.out.println(">>Shutting down Thread: " + m_threadNumber);
		}
	}
}
