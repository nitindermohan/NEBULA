package de.tum.i11.bcsim.util;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CPULoadMeasure {

    private final CentralProcessor cpu;
    private ScheduledExecutorService exec;
    private final int ticksMs;
    private boolean running;

    private double max;
    private int count;
    private double avg;

    public CPULoadMeasure(int ticksMs) {
        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        this.cpu = hal.getProcessor();
        this.running = false;
        this.ticksMs = ticksMs;
    }

    public void startAfter(int ms) {
        if(running) {
            stop();
        }
        count = -1;
        max = Double.MIN_VALUE;
        avg = Double.MIN_VALUE;
        exec = Executors.newSingleThreadScheduledExecutor();

        final long[][] prevTicks = new long[1][1];
        prevTicks[0] = new long[CentralProcessor.TickType.values().length];
        exec.scheduleAtFixedRate(() -> {
            if(++count == 0) {
                return;
            }
            double cpuLoad = cpu.getSystemCpuLoadBetweenTicks( prevTicks[0] );
            if(count == 1) {
                max = cpuLoad;
                avg = cpuLoad;
            } else {
                max = Math.max(max, cpuLoad);
                avg = (avg*(count-1)+cpuLoad)/count;
            }
            prevTicks[0] = cpu.getSystemCpuLoadTicks();
        }, ms, ticksMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        exec.shutdownNow();
        running = false;
    }

    public double getMaxLoad() {
        return max*100;
    }

    public double getAvgLoad() {
        return avg*100;
    }
}
