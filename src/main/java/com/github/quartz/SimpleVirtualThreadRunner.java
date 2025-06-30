package com.github.quartz;

import java.util.concurrent.atomic.AtomicLong;

import org.quartz.SchedulerConfigException;
import org.quartz.spi.ThreadPool;

/**
 * Simple implementation of Quartz' {@link ThreadPool} for virtual threads.
 * As virtual threads are not recommended to be pooled, 
 * the implementation is not actually a pool in a sense 
 * that new virtual thread gets started for each invocation 
 * of {@link #runInThread(Runnable)} method.
 * 
 * <p/>
 * The implementation assumes that unlimited amounts of virtual threads can be created 
 * simultaneously, therefore {@link #runInThread(Runnable)} always 
 * executes a job and always returns {@code true}, 
 * and {@link #blockForAvailableThreads()} never blocks and
 * always returns unchanging amount of available threads.
 * 
 * <p/>
 * If that assumption is not acceptable, please use {@link VirtualThreadRunner} 
 * implementation of {@link ThreadPool}.
 */
public class SimpleVirtualThreadRunner implements ThreadPool {
	
	private final AtomicLong threadOrdinal = new AtomicLong();
	private String schedName;

	@Override
	public boolean runInThread(Runnable runnable) {
		Thread.ofVirtual().name(schedName + "-virtual-#" + Long.valueOf(threadOrdinal.incrementAndGet()))
				.start(runnable);
		return true;
	}

	@Override
	public int blockForAvailableThreads() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void initialize() throws SchedulerConfigException {
	}

	@Override
	public void shutdown(boolean waitForJobsToComplete) {
	}

	@Override
	public int getPoolSize() {
		return Integer.MAX_VALUE;
	}

	@Override
	public void setInstanceId(String schedInstId) {
	}

	@Override
	public void setInstanceName(String schedName) {
		this.schedName = schedName;
	}

}
