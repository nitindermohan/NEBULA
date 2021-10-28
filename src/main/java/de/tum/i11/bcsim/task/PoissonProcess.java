package de.tum.i11.bcsim.task;

import java.security.SecureRandom;
import java.util.Random;

public class PoissonProcess extends Thread implements RepeatingTask {
    private boolean stopped;
    private final double rate;
    private final Runnable task;
    private final Random rnd;
    private long startTime;

    public PoissonProcess(Runnable task, double rate) {
        this.task = task;
        this.rate = rate/1000.0;
        this.rnd = new SecureRandom();
        this.startTime = System.currentTimeMillis();
    }

    public PoissonProcess(Runnable task, double rate, int priority) {
        this(task,rate);
        this.setPriority(priority);
    }

    private double waitingTime(double lambda) {
        return -Math.log(rnd.nextDouble())/lambda;
    }

    @Override
    public void run() {
        if(rate <= 0)
            return;

        long wait = (long) waitingTime(rate);
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
                wait = (long) waitingTime(rate);
                Thread.sleep(wait);
            } catch (InterruptedException ignored) {}
            if(!stopped) {
                task.run();
            }
        }
    }

    @Override
    public PoissonProcess begin(long startTime) {
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
