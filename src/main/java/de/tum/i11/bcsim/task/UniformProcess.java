package de.tum.i11.bcsim.task;

import java.util.concurrent.ThreadLocalRandom;

public class UniformProcess extends Thread implements RepeatingTask {
    private boolean stopped;
    private final int time;
    private final Runnable task;
    private long startTime;

    public UniformProcess(Runnable task, int time) {
        this.task = task;
        this.time = time;
        this.startTime = System.currentTimeMillis();
    }

    public UniformProcess(Runnable task, int time, int priority) {
        this(task, time);
        this.setPriority(priority);
    }

    private static int waitingTime(int time) {
        return ThreadLocalRandom.current().nextInt(0, 2*time);
    }

    @Override
    public void run() {
        if(time <= 0)
            return;
        long wait = waitingTime(time);
        long toStart = startTime - System.currentTimeMillis();
        if(toStart+wait > 0) {
            try {
                Thread.sleep(toStart+wait);
            } catch (InterruptedException ignored){}
        }
        if(!stopped) {
            task.run();
        }
        while(!stopped) {
            try {
                wait = waitingTime(time);
                Thread.sleep(wait);
            } catch (InterruptedException ignored) {}
            if(!stopped) {
                task.run();
            }
        }
    }

    @Override
    public UniformProcess begin(long startTime) {
        this.startTime = startTime;
        start();
        return this;
    }

    @Override
    public void end() {
        stopped = true;
        this.interrupt();
    }
}
