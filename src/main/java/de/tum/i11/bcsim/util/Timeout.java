package de.tum.i11.bcsim.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Timeout {

    private Runnable task;
    private long ms;
    private ScheduledExecutorService exec;
    private final Object updateLock = new Object();
    private long lastUpdate;
    private boolean running;

    public Timeout(Runnable task, long ms) {
        this.exec = Executors.newSingleThreadScheduledExecutor();
        this.task = task;
        this.ms = ms;
        this.running = false;
    }

    public void start() {
        running = true;
        exec.scheduleAtFixedRate(() -> {
            synchronized (updateLock) {
                if(System.currentTimeMillis() - lastUpdate >= ms) {
                    task.run();
                    exec.shutdown();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
        lastUpdate = System.currentTimeMillis();
    }

    public void restart() {
        if(running) {
            synchronized (updateLock) {
                lastUpdate = System.currentTimeMillis();
            }
        }
    }

    public void cancel() {
        exec.shutdownNow();
    }
}
