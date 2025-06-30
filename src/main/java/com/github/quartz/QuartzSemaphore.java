package com.github.quartz;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * An {@link java.util.concurrent.locks.AbstractQueuedSynchronizer AQS}-based 
 * version of {@link java.util.concurrent.Semaphore Semaphore},
 * tuned for the logic of Quartz' {@link org.quartz.spi.ThreadPool ThreadPool}.
 * Functionality of {@link #tryAcquire()} and {@link #release()} methods
 * is equivalent to the one of Semaphore.
 * {@link #blockIfNoPermits()} method actually invokes  
 * {@link java.util.concurrent.locks.AbstractQueuedSynchronizer#acquireShared(int) AQS.acquireShared(int)}
 * method, but {@link com.github.quartz.QuartzSemaphore.Sync#tryAcquireShared(int) Sync.tryAcquireShared(int)}
 * is implemented in such a way that it only checks the State 
 * and the thread will be blocked if the State indicates unavailability of the resource.
 * {@link VirtualThreadRunner} uses this class to implement 
 * {@link org.quartz.spi.ThreadPool ThreadPool} contract.  
 * 
 * @see AbstractQueuedSynchronizer
 * @see org.quartz.spi.ThreadPool ThreadPool
 * @see VirtualThreadRunner  
 */
public class QuartzSemaphore {

	private static final int NO_MATTER_ARG = 0;
	
	private static class Sync extends AbstractQueuedSynchronizer {

		private static final long serialVersionUID = 1L;
		
		private final int maxPermits;

		public Sync(int permits) {
			setState(permits);
			maxPermits = permits;
		}

		@Override
		protected boolean tryReleaseShared(int arg) {
           for (;;) {
                int current = getState();
                int next = current + 1;
                if (next < current) // overflow
                    throw new Error("Maximum permit count exceeded");
                if (compareAndSetState(current, next))
                    return true;
            }
		}

		@Override
		protected int tryAcquireShared(int arg) {
			return getState() > 0 ? 1 : -1;
		}

		public int tryAcquire() {
			for (;;) {
				int available = getState();
				int remaining = available - 1;
				if (remaining < 0 || compareAndSetState(available, remaining)) {
					return remaining;
				}
			}
		}

		public int test() {
			acquireShared(NO_MATTER_ARG);
			final int state = getState();
			return state > 0 ? state : 1;
		}
		
		public void waitForFullRelease() {
			for (;;) {
				acquireShared(NO_MATTER_ARG);
				if (getState() >= maxPermits && !hasQueuedThreads())
					break;
			}			
		}

	}

	private final Sync sync;

	public QuartzSemaphore(int permits) {
		sync = new Sync(permits);
	}

	public boolean tryAcquire() {
		return sync.tryAcquire() >= 0;
	}

	public void release() {
		sync.releaseShared(NO_MATTER_ARG);
	}

	public int blockIfNoPermits() {
		return sync.test();
	}
	
	public void waitForFullRelease() {
		sync.waitForFullRelease();
	}

}
