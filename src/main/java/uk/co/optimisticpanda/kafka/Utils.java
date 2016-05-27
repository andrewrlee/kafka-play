package uk.co.optimisticpanda.kafka;

import java.util.function.Supplier;

public class Utils {

	public interface ThrowingSupplier<T> {
		T get() throws Exception;
		
		static <T> Supplier<T> wrapAnyError(ThrowingSupplier<T> supplier) {
				return () -> {
					try {
						return supplier.get();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				};
		} 
	}
}
