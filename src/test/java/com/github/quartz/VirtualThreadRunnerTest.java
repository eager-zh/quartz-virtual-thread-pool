package com.github.quartz;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;
import org.quartz.SchedulerConfigException;

public class VirtualThreadRunnerTest {
	
	private static final int MAX_SLEEP = 10; // sec
	private static final int THREAD_COUNT = 20;
	private static final int POOL_THREAD_COUNT = 3;
	
	private static void sleep(int seconds) {
		try {
			Thread.sleep(Duration.ofSeconds(seconds));
		} catch (InterruptedException e) {
		}
	}
	
	@Test
	public void testTenaciousSubmitters() throws SchedulerConfigException, InterruptedException {
		final CountDownLatch shutdownLatch = new CountDownLatch(1);
		
		final VirtualThreadRunner pool = new VirtualThreadRunner();
		pool.setThreadCount(POOL_THREAD_COUNT);
		pool.initialize();
		
		final AtomicInteger executedJobCount = new AtomicInteger();
		final AtomicInteger virtualThreadCount = new AtomicInteger();
		
		final Random randomizer = new Random();
		
		final Runnable job = () -> {
			if (Thread.currentThread().isVirtual()) {
				virtualThreadCount.incrementAndGet();
			}
			sleep(randomizer.nextInt(MAX_SLEEP));
			if (executedJobCount.incrementAndGet() == THREAD_COUNT) {
				shutdownLatch.countDown();
			}
		};
		
		IntStream.range(0, THREAD_COUNT).forEach(i -> Thread.ofPlatform().start(() -> {
			sleep(randomizer.nextInt(MAX_SLEEP));
			boolean ran = pool.runInThread(job);
			while (!ran) {
				Assert.assertTrue("Positive value expected", pool.blockForAvailableThreads() > 0);
				ran = pool.runInThread(job);
			}
		}));
		
		Assert.assertTrue("Test hangs", shutdownLatch.await(THREAD_COUNT*MAX_SLEEP, TimeUnit.SECONDS));
		final Thread shutdowner = Thread.ofPlatform().start(() -> {
			pool.shutdown(true);
		});
		shutdowner.join(Duration.ofSeconds(POOL_THREAD_COUNT*MAX_SLEEP));
		Assert.assertFalse("Shutdown call hangs", shutdowner.isAlive());

		Assert.assertEquals("Not all Jobs have been executed", THREAD_COUNT, executedJobCount.get());
		Assert.assertEquals("Not all threads executing Jobs were virtual", THREAD_COUNT, virtualThreadCount.get());
	}
	
	@Test
	public void testWaivingSubmitters() throws SchedulerConfigException, InterruptedException {
		final CountDownLatch shutdownLatch = new CountDownLatch(1);
		
		final VirtualThreadRunner pool = new VirtualThreadRunner();
		pool.setThreadCount(POOL_THREAD_COUNT);
		pool.initialize();
		
		final AtomicInteger executedJobCount = new AtomicInteger();
		final AtomicInteger rejectedJobCount = new AtomicInteger();
		final AtomicInteger virtualThreadCount = new AtomicInteger();
		final AtomicInteger attemptedThreadCount = new AtomicInteger();
		
		final Random randomizer = new Random();
		
		final Runnable job = () -> {
			if (Thread.currentThread().isVirtual()) {
				virtualThreadCount.incrementAndGet();
			}
			sleep(randomizer.nextInt(MAX_SLEEP));
			executedJobCount.incrementAndGet();
		};
		
		IntStream.range(0, THREAD_COUNT).forEach(i -> Thread.ofPlatform().start(() -> {
			sleep(randomizer.nextInt(MAX_SLEEP));
			if (!pool.runInThread(job)) {
				rejectedJobCount.incrementAndGet();
			} 
			if (attemptedThreadCount.incrementAndGet() == THREAD_COUNT) {
				shutdownLatch.countDown();
			}
		}));

		Assert.assertTrue("Test hangs", shutdownLatch.await(THREAD_COUNT*MAX_SLEEP, TimeUnit.SECONDS));
		final Thread shutdowner = Thread.ofPlatform().start(() -> {
			pool.shutdown(true);
		});
		shutdowner.join(Duration.ofSeconds(POOL_THREAD_COUNT*MAX_SLEEP));
		Assert.assertFalse("Shutdown call hangs", shutdowner.isAlive());
		
		Assert.assertEquals("Not all Jobs have been either executed or rejected", THREAD_COUNT, executedJobCount.get() + rejectedJobCount.get());
		Assert.assertEquals("Not all Jobs were executed on virtual threads", executedJobCount.get(), virtualThreadCount.get());
	}

	@Test
	public void testThreadFactory() throws SchedulerConfigException, InterruptedException {
		final CountDownLatch shutdownLatch = new CountDownLatch(1);
		
		final String threadPrefix = "custom-thread-name-";
		
		final VirtualThreadRunner pool = new VirtualThreadRunner();
		pool.setThreadCount(POOL_THREAD_COUNT);
		pool.setThreadFactory(Thread.ofVirtual()
				.name(threadPrefix, 1)
				.factory());
		pool.initialize();
		
		final AtomicInteger executedJobCount = new AtomicInteger();
		final AtomicInteger virtualThreadCount = new AtomicInteger();
		final AtomicBoolean wrongThreadName = new AtomicBoolean(); 
		
		final Random randomizer = new Random();
		
		final Runnable job = () -> {
			if (Thread.currentThread().isVirtual()) {
				virtualThreadCount.incrementAndGet();
			}
			sleep(randomizer.nextInt(MAX_SLEEP));
			if (!Thread.currentThread().getName().contains(threadPrefix)) {
				wrongThreadName.set(true);
			}
			if (executedJobCount.incrementAndGet() == THREAD_COUNT) {
				shutdownLatch.countDown();
			}
		};
		
		IntStream.range(0, THREAD_COUNT).forEach(i -> Thread.ofPlatform().start(() -> {
			sleep(randomizer.nextInt(MAX_SLEEP));
			boolean ran = pool.runInThread(job);
			while (!ran) {
				Assert.assertTrue("Positive value expected", pool.blockForAvailableThreads() > 0);
				ran = pool.runInThread(job);
			}
		}));
		

		Assert.assertTrue("Test hangs", shutdownLatch.await(THREAD_COUNT*MAX_SLEEP, TimeUnit.SECONDS));
		final Thread shutdowner = Thread.ofPlatform().start(() -> {
			pool.shutdown(true);
		});
		shutdowner.join(Duration.ofSeconds(POOL_THREAD_COUNT*MAX_SLEEP));
		Assert.assertFalse("Shutdown call hangs", shutdowner.isAlive());

		Assert.assertFalse("Custom thread factory has not been used", wrongThreadName.get());
		Assert.assertEquals("Not all Jobs have been executed", THREAD_COUNT, executedJobCount.get());
		Assert.assertEquals("Not all threads executing Jobs were virtual", THREAD_COUNT, virtualThreadCount.get());
	}
	
	@Test
	public void testVirtualSubmitterThreads() throws SchedulerConfigException, InterruptedException {
		final CountDownLatch shutdownLatch = new CountDownLatch(1);
		
		final VirtualThreadRunner pool = new VirtualThreadRunner();
		pool.setThreadCount(POOL_THREAD_COUNT);
		pool.initialize();
		
		final AtomicInteger executedJobCount = new AtomicInteger();
		final AtomicInteger virtualThreadCount = new AtomicInteger();
		
		final Random randomizer = new Random();
		
		final Runnable job = () -> {
			if (Thread.currentThread().isVirtual()) {
				virtualThreadCount.incrementAndGet();
			}
			sleep(randomizer.nextInt(MAX_SLEEP));
			if (executedJobCount.incrementAndGet() == THREAD_COUNT) {
				shutdownLatch.countDown();
			}
		};
		
		IntStream.range(0, THREAD_COUNT).forEach(i -> Thread.ofVirtual().start(() -> {
			sleep(randomizer.nextInt(MAX_SLEEP));
			boolean ran = pool.runInThread(job);
			while (!ran) {
				Assert.assertTrue("Positive value expected", pool.blockForAvailableThreads() > 0);
				ran = pool.runInThread(job);
			}
		}));
		
		Assert.assertTrue("Test hangs", shutdownLatch.await(THREAD_COUNT*MAX_SLEEP, TimeUnit.SECONDS));
		final Thread shutdowner = Thread.ofPlatform().start(() -> {
			pool.shutdown(true);
		});
		shutdowner.join(Duration.ofSeconds(POOL_THREAD_COUNT*MAX_SLEEP));
		Assert.assertFalse("Shutdown call hangs", shutdowner.isAlive());

		Assert.assertEquals("Not all Jobs have been executed", THREAD_COUNT, executedJobCount.get());
		Assert.assertEquals("Not all threads executing Jobs were virtual", THREAD_COUNT, virtualThreadCount.get());
	}
	
	@Test
	public void testShutdown() throws SchedulerConfigException, InterruptedException {
		final CountDownLatch shutdownLatch = new CountDownLatch(1);
		
		final VirtualThreadRunner pool = new VirtualThreadRunner();
		pool.setThreadCount(POOL_THREAD_COUNT);
		pool.initialize();
		
		final AtomicInteger executedJobCount = new AtomicInteger();
		final AtomicInteger executingJobCount = new AtomicInteger();
		final AtomicInteger rejectedJobCount = new AtomicInteger();
		
		final Runnable job = () -> {
			if (executingJobCount.incrementAndGet() == POOL_THREAD_COUNT) {
				shutdownLatch.countDown();
			}
			sleep(MAX_SLEEP);
			executedJobCount.incrementAndGet();
		};
		
		final Random randomizer = Random.from(ThreadLocalRandom.current());

		IntStream.range(0, THREAD_COUNT).forEach(i -> Thread.ofPlatform().start(() -> {
			sleep(randomizer.nextInt(MAX_SLEEP));
			if (!pool.runInThread(job)) {
				rejectedJobCount.incrementAndGet();
			}
		}));
		
		Assert.assertTrue("Test hangs", shutdownLatch.await(THREAD_COUNT*MAX_SLEEP, TimeUnit.SECONDS));
		final Thread shutdowner = Thread.ofPlatform().start(() -> {
			pool.shutdown(true);
		});
		shutdowner.join(Duration.ofSeconds(POOL_THREAD_COUNT*MAX_SLEEP));
		Assert.assertFalse("Shutdown call hangs", shutdowner.isAlive());

		Assert.assertEquals("Wrong amount of executed Jobs", POOL_THREAD_COUNT, executedJobCount.get());
		Assert.assertEquals("Wrong amount of rejected Jobs", THREAD_COUNT - POOL_THREAD_COUNT, rejectedJobCount.get());
	}

}
