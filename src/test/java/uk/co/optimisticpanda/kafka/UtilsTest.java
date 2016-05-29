package uk.co.optimisticpanda.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static uk.co.optimisticpanda.kafka.Utils.ThrowingSupplier.propagateAnyError;
import static uk.co.optimisticpanda.kafka.Utils.ThrowingSupplier.propagateAnyErrorSupplier;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;

import uk.co.optimisticpanda.kafka.Utils.MemoizeSupplier;
import uk.co.optimisticpanda.kafka.Utils.ThrowingRunnable;
import uk.co.optimisticpanda.kafka.Utils.ThrowingSupplier;

public class UtilsTest {

	@Test
	public void getFreePorts() {
		assertThat(Utils.getFreePorts(10))
			.hasSize(10)
			.doesNotHaveDuplicates();
	}
	
	@Test
	public void checkThrowingSupplier() {
		assertThat(propagateAnyError(() -> 2)).isEqualTo(2);
		assertThat(propagateAnyErrorSupplier(() -> 2).get()).isEqualTo(2);
		
		ThrowingSupplier<Void> thrower = () -> {
			throw new IOException("error");
		}; 
		
		assertThatThrownBy(() ->  propagateAnyError(thrower)).hasCauseInstanceOf(IOException.class);
		assertThatThrownBy(() ->  propagateAnyErrorSupplier(thrower).get()).hasCauseInstanceOf(IOException.class);
	}
	
	@Test
	public void checkSwallowAnyException() {
		AtomicBoolean flag = new AtomicBoolean();
		
		ThrowingRunnable.swallowAnyError(() -> {
			flag.set(true);
			throw new RuntimeException("error");
		});
		
		assertThat(flag.get()).isTrue();
	}
	
	@Test
	public void checkPropagateAnyException() {
		
		AtomicBoolean flag = new AtomicBoolean();
		
		ThrowingRunnable.propagateAnyError(() -> {
			flag.set(true);
		});

		assertThat(flag.get()).isTrue();

		assertThatThrownBy(() -> 
			ThrowingRunnable.propagateAnyError(() -> { 
				throw new IOException("error");
			})
		).hasCauseExactlyInstanceOf(IOException.class);
	}

	@Test
	public void memoize() {
		AtomicInteger count = new AtomicInteger(); 
		Supplier<Integer> supplier = MemoizeSupplier.of(count::getAndIncrement);
		assertThat(supplier.get()).isEqualTo(0);
		assertThat(supplier.get()).isEqualTo(0);
	}
}
