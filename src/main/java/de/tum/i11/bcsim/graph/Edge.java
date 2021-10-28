package de.tum.i11.bcsim.graph;

public class Edge {
    public int from;
    public int to;
    public final int latency;

    public Edge(int from, int to, int latency) {
        this.from = from;
        this.to = to;
        this.latency = latency;
    }

    public String toString() {
        return "(from: "+from+", to: "+to+", lat: "+latency+")";
    }
}
