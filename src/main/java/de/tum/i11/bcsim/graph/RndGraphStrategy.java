package de.tum.i11.bcsim.graph;

import java.util.ArrayList;
import java.util.List;

public class RndGraphStrategy implements GraphStrategy {

    private final int nodes;
    private final int latency;
    private final double density;
    private final double bandwidth;
    public RndGraphStrategy(int nodes, int latency, double density, double bandwidth) {
        this.nodes = nodes;
        this.latency = latency;
        this.density = Math.min(1.0, density);
        this.bandwidth = bandwidth;
    }

    @Override
    public ArrayList<List<Edge>> getEdges() {
        return GraphUtil.toAdjList(nodes, latency, GraphUtil.create(nodes, density));
    }

    public int getNodes() {
        return nodes;
    }

    public double getMaxLatency() {
        return latency;
    }

    @Override
    public double getBandWidth() {
        return bandwidth;
    }
}
