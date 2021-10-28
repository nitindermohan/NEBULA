package de.tum.i11.bcsim.task;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ConstantRateTask implements RepeatingTask {

    private final long initialDelay;
    private final long delay;
    private final Runnable task;
    private final ScheduledExecutorService executor;

    public ConstantRateTask(Runnable task, long initalDelay, long delay, int priority) {
        this.task = task;
        this.initialDelay = initalDelay;
        this.delay = delay;
        this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            private ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(@NotNull Runnable r) {
                Thread t = defaultThreadFactory.newThread(r);
                t.setPriority(priority);
                return t;
            }
        });

    }

    public ConstantRateTask(Runnable task, long initalDelay, long delay) {
        this(task, initalDelay, delay, Thread.NORM_PRIORITY);
    }

    @Override
    public RepeatingTask begin(long startTime) {
        long init = startTime-System.currentTimeMillis()+initialDelay;
        init = Math.max(0, init);
        executor.scheduleAtFixedRate(task, init, delay, TimeUnit.MILLISECONDS);
        return this;
    }

    @Override
    public void end() {
        executor.shutdownNow();
    }
}
