package de.tum.i11.bcsim.task;

public interface RepeatingTask {
    RepeatingTask begin(long startTime);
    void end();
}
