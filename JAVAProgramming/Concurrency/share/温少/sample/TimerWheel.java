package com.kingdee;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TimerWheel {
	private static final long NANO_ORIGIN = System.nanoTime();

	final static long now() {
		return System.nanoTime() - NANO_ORIGIN;
	}

	static class Item {
		long expired;

		Callable<?> callable;
	}

	private final int size;

	private ConcurrentLinkedQueue<Item>[] wheel;

	private final long timespan;

	private ScheduledExecutorService scheduler;

	public TimerWheel(final int size, final long timespan, final TimeUnit unit,
			final ScheduledExecutorService scheduler) {
		this.size = size;
		wheel = new ConcurrentLinkedQueue[size];
		for (int i = 0; i < size; ++i)
			wheel[i] = new ConcurrentLinkedQueue<Item>();

		this.timespan = TimeUnit.NANOSECONDS.convert(timespan, unit);
		this.scheduler = scheduler;
		Runnable task = new Runnable() {
			public void run() {
				check();
			}
		};

		scheduler.scheduleAtFixedRate(task, this.timespan, this.timespan, TimeUnit.NANOSECONDS);
	}

	private void check() {
		long now = now();
		long unitExpire = now / timespan;
		int wheelIndex = (int) (unitExpire % size);
		ConcurrentLinkedQueue<Item> q = wheel[wheelIndex];

		List<Item> expiredNodeList = new ArrayList<Item>();
		for (Item item : q)
			if (item.expired <= now)
				expiredNodeList.add(item);

		for (Item item : expiredNodeList)
			if (q.remove(item))
				scheduler.submit(item.callable);
	}

	public void add(Callable callable, long expired, TimeUnit unit) {
		Item node = new Item();

		node.callable = callable;

		long nanoExpire = now() + TimeUnit.NANOSECONDS.convert(expired, unit);
		long unitExpire = nanoExpire / timespan;
		int wheelIndex = (int) (unitExpire % size);

		node.expired = nanoExpire;
		wheel[wheelIndex].add(node);
	}

	public static void main(String[] args) throws Exception {
		final int size = 10;
		final long timespan = 1;
		final TimeUnit unit = TimeUnit.SECONDS;
		final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

		TimerWheel wheel = new TimerWheel(size, timespan, unit, scheduler);

		final long start = System.currentTimeMillis();

		class CallabeItem implements Callable {
			private final long time;

			public CallabeItem(final long time) {
				this.time = time;
			}

			public Object call() throws Exception {
				long timespan = System.currentTimeMillis() - start;
				System.out.println(timespan + " : " + time);
				return time;
			}
		};

		SecureRandom random = new SecureRandom();

		for (int i = 0; i < 50; ++i) {
			long r = random.nextInt(20000);
			wheel.add(new CallabeItem(r), r, TimeUnit.MILLISECONDS);
		}
	}
}
