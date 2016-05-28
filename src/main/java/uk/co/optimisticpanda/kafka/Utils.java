package uk.co.optimisticpanda.kafka;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

	private static Logger L = LoggerFactory.getLogger(Utils.class);

	public interface ThrowingSupplier<T> {

		T get() throws Exception;

		static <T> Supplier<T> wrapAnyError(ThrowingSupplier<T> supplier) {
			return () -> {
				try {
					return supplier.get();
				} catch (Exception e) {
					L.error("error: {}", e.getMessage(), e);
					throw new RuntimeException(e);
				}
			};
		}
	}

	public interface ThrowingRunnable {
		void run() throws Exception;

		static void wrapAnyError(ThrowingRunnable runable) {
			try {
				runable.run();
			} catch (Exception e) {
				L.error("error: {}", e.getMessage(), e);
				throw new RuntimeException(e);
			}
		}
	}
}
