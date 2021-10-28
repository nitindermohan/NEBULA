package de.tum.i11.bcsim.util;

public class ThroughputMeasure {
    private int packets;
    private int firstPacket;
    private double throughput;
    private long tsFirstPacket;

    public void registerPackets(int amount) {
        registerPackets(amount, System.currentTimeMillis());
    }

    public synchronized void registerPackets(int amount, long time) {
        // first packet starts throughput timer but data is ignored
        if(tsFirstPacket == 0) {
            tsFirstPacket = time;
            firstPacket = amount;
            return;
        }
        packets += amount;
        throughput = packets/((time- tsFirstPacket)/1000.0);
    }

    public double getThroughput() {
        return throughput;
    }

    public int getPackets() {
        return firstPacket+packets;
    }

    public String toString() {
        return getThroughput()+"/s ("+getPackets()+" total)";
    }
}
