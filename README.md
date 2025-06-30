# quartz-virtual-thread-pool

Implementation of Quartz' `ThreadPool` interface for virtual threads. This is a complimentary project to a discussion on the Stack Overflow thread [How to execute Quartz jobs in virtual threads](https://stackoverflow.com/questions/79652435/how-to-execute-quartz-jobs-in-virtual-threads).

`SimpleVirtualThreadRunner` is a simple implementation of `ThreadPool` which assumes that unlimited amount of virtual threads could be created; it always executes a job and never blocks. 

If that assumption is not acceptable, `VirtualThreadRunner` offers implementation that operates on restricted amount of virtual threads. It also implements graceful shutdown of virtual threads, which might be quite important taking into consideration the fact that virtual thread are daemons and will be terminated if the virtual machine terminates, which may not be suitable for scheduled jobs.

To test, configure `SimpleVirtualThreadRunner` or `VirtualThreadRunner` as `org.quartz.threadPool.class` property of `quartz.properties` Quartz' configuration file and run any of the [examples suggested by Quartz documentation](https://www.quartz-scheduler.org/documentation/quartz-2.2.2/examples/). Small suite of JUnit tests, which are focused on concurrent capabilities of the offered implementation, is also available in the project.

