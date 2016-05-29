package uk.co.optimisticpanda.kafka;

import static java.util.stream.IntStream.range;
import static uk.co.optimisticpanda.kafka.Utils.ThrowingRunnable.swallowAnyError;
import static uk.co.optimisticpanda.kafka.Utils.ThrowingSupplier.propagateAnyErrorSupplier;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

	private static final Logger L = LoggerFactory.getLogger(Utils.class);

	public static int[] getFreePorts(int number) {
	    List<ServerSocket> servers = new ArrayList<ServerSocket>(number);
	    try {
		    return range(0, number)
		    	.mapToObj(i -> propagateAnyErrorSupplier(() -> new ServerSocket(0)).get())
		    	.peek(servers::add)
		    	.mapToInt(ServerSocket::getLocalPort)
		    	.toArray();
        } finally {
            servers.forEach(server -> swallowAnyError(() -> server.close()));
        }
	}
	
	public interface ThrowingSupplier<T> {

		T get() throws Exception;

		static <T> Supplier<T> propagateAnyErrorSupplier(ThrowingSupplier<T> supplier) {
			return () -> {
				try {
					return supplier.get();
				} catch (Exception e) {
					L.error("error: {}", e.getMessage(), e);
					throw new RuntimeException(e);
				}
			};
		}
		
		static <T> T propagateAnyError(ThrowingSupplier<T> supplier) {
			return propagateAnyErrorSupplier(supplier).get();
		}
	}

	public interface ThrowingRunnable {
		void run() throws Exception;

		static void propagateAnyError(ThrowingRunnable runable) {
			try {
				runable.run();
			} catch (Exception e) {
				L.error("error: {}", e.getMessage(), e);
				throw new RuntimeException(e);
			}
		}
		
		static void swallowAnyError(ThrowingRunnable runable) {
			try {
				runable.run();
			} catch (Exception e) {
				L.warn("error: {}", e.getMessage(), e);
			}
		}
	}
	
	public static class MemoizeSupplier<T> implements Supplier<T> {

		final Supplier<T> delegate;
		final ConcurrentMap<Class<?>, T> map = new ConcurrentHashMap<>(1);

		public MemoizeSupplier(Supplier<T> delegate) {
			this.delegate = delegate;
		}

		@Override
		public T get() {
			return this.map.computeIfAbsent(MemoizeSupplier.class, k -> this.delegate.get());
		}

		public static <T> Supplier<T> of(Supplier<T> provider) {
			return new MemoizeSupplier<>(provider);
		}
	}
}
