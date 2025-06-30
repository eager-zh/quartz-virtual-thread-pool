package com.github.quartz;

import java.util.concurrent.ThreadFactory;

import org.quartz.SchedulerConfigException;
import org.quartz.spi.ThreadPool;

/**
 * Implementation of Quartz {@link ThreadPool} for virtual threads.
 * As virtual threads are not recommended to be pooled, 
 * the implementation is not actually a pool in a sense 
 * that new virtual thread gets started for each invocation 
 * of {@link #runInThread(Runnable)} method.
 * 
 * <p/>
 * The implementation, however, does honor the "poolSize" attribute,
 * returned by {@link ThreadPool#getPoolSize()} method:
 * if the amount of currently executed threads exceeds this amount,
 * the {@link #blockForAvailableThreads} method will block until 
 * this amount decreases, and {@link #runInThread(Runnable)}} method 
 * will return false - exactly as {@link ThreadPool} prescribes.
 * 
 * <p/>
 * Default "poolSize"  is {@link #DEFAULT_THREAD_COUNT},
 * its high value corresponds to the promises of virtual threads developers
 * that many thousands of virtual threads can be simultaneously created and used 
 * (provided, of course, that most of them are waiting for completion 
 * of I/O operations and similar).   
 * "poolSize" can be configured by {@link #setThreadCount(int)} method
 * prior to invocation of {@link #initialize()} method.
 *  
 * <p/>
 * {@link #setThreadFactory(ThreadFactory)} method sets up custom {@link ThreadFactory},
 * thus allowing to override thread name pattern, which is by default Quartz' Instance name,
 * concatenated with "-virtual-" and thread ordinal, to set up thread exception handler and so on.
 * Note that the implementation does not check if the {@link ThreadFactory} 
 * indeed produces virtual threads or platform ones, but please keep in mind 
 * that in either case threads will be created on per-task basis and 
 * will not be pooled.       
  * 
 */
public class VirtualThreadRunner implements ThreadPool {
	
	private static final int DEFAULT_THREAD_COUNT = 10_000;
	
	private QuartzSemaphore semaphore;
	private String schedName = "";
	private ThreadFactory threadFactory;
	private int threadCount = DEFAULT_THREAD_COUNT;
	private volatile boolean isShutdown;

	/**
	 * @implNote returns {@code false} if 
	 * the amount currently executed threads is equal to "poolSize" 
	 * or {@link #isShutdown} method was previously called, 
	 * otherwise - {@code true}.
	 */
	@Override
	public boolean runInThread(Runnable runnable) {
		if (!isShutdown && semaphore.tryAcquire()) {
			threadFactory.newThread(() -> {
				try {
					runnable.run();
				} finally {
					semaphore.release();
				}
			}).start();
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * @implNote returns approximate amount of available threads,
	 * which, depending on the synchronization of invocations of this and 
	 * {@link #runInThread} methods may or may not reflect 
	 * current value of available threads. 
	 * Upon unblocking, never return non-positive value.
	 * Always does not block and return 0 after 
	 * {@link #isShutdown} method was previously called.  
	 */
	@Override
	public int blockForAvailableThreads() {
		return isShutdown ? 0 : semaphore.blockIfNoPermits();
	}

	@Override
	public void initialize() throws SchedulerConfigException {
		semaphore = new QuartzSemaphore(threadCount);
		if (threadFactory == null) {
			threadFactory = Thread.ofVirtual()
				.name(schedName + "-virtual-", 1)
				.factory(); 
		}
	}

	/**
	 * @implNote sets {@link #isShutdown} flag to true, 
	 * which influences the behavior of 
	 * {@link #runInThread(Runnable)} and {@link #blockForAvailableThreads()}
	 * methods.
	 * Does not make any attempts to interrupt running threads.
	 */
	@Override
	public void shutdown(boolean waitForJobsToComplete) {
		isShutdown = true;
		if (waitForJobsToComplete) {
			semaphore.waitForFullRelease();
		}
	}

	@Override
	public int getPoolSize() {
		return threadCount;
	}

    /**
     * Sets "poolSize". 
     * 
     * @throws {@link IllegalStateException} if {@link #initialize()} has been previously called
     */
	public void setThreadCount(int count) {
    	if (semaphore != null) {
    		throw new IllegalStateException("Thread count cannot be set after initialization");
    	}
        this.threadCount = count;
    }
	
	public void setThreadFactory(ThreadFactory threadFactory) {
		this.threadFactory = threadFactory;
	}

	@Override
	public void setInstanceId(String schedInstId) {
	}

	@Override
	public void setInstanceName(String schedName) {
		this.schedName = schedName;
	}
	
}
