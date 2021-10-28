package de.tum.i11.bcsim.graph;

import java.util.*;
import java.util.logging.Logger;

public class ScaleFreeGraphWithAvgPropagationDelay implements GraphStrategy {
    private static final Logger LOGGER = Logger.getLogger(GraphStrategy.class.getName());

    private final int nodes;
    private final int avgPropagationDelay;
    private final int m;
    private int maxlatency;
    private final double bandwidth;

    public ScaleFreeGraphWithAvgPropagationDelay(int nodes, int m, int avgPropagationDelay, double bandwidth) {
        this.nodes = nodes;
        this.avgPropagationDelay = avgPropagationDelay;
        this.m = m;
        this.bandwidth = bandwidth;
    }

    @Override
    public ArrayList<List<Edge>> getEdges() {
        ArrayList<List<Edge>> graph;
        double avg = 0;

        int left = 0;
        int right = avgPropagationDelay;

        HashMap<Double, ArrayList<List<Edge>>> found = new HashMap<>();

        LOGGER.info("Looking for Graph with average propagation delay of "+avgPropagationDelay+"ms");

        while(Math.abs(avgPropagationDelay-avg) > 1 && left < right) {
            double mid = (left+right)/2.0;

            ScaleFreeStrategy s = new ScaleFreeStrategy(nodes, m, (int) Math.round(mid), bandwidth);
            graph = s.getEdges();
            avg = GraphUtil.getAvgPropDelay(graph);
            found.put(avg, graph);

            if(avg > avgPropagationDelay) {
                right = (int) mid;
            } else if (avg < avgPropagationDelay) {
                left = (int) Math.ceil(mid);
            } else break;

        }

        var entry = found.entrySet().stream().min(Comparator.comparingDouble(o -> Math.abs(avgPropagationDelay - o.getKey())));

        var val = entry.map(Map.Entry::getValue).orElse(null);

        maxlatency = GraphUtil.getMaxLatency(val);

        LOGGER.info("Max Latency: "+maxlatency);
        LOGGER.info("Avg Latency: "+GraphUtil.getAvgPropDelay(val));
        LOGGER.info("Avg Hops: "+GraphUtil.getAvgHops(val));
        LOGGER.info("Max Hops: "+GraphUtil.getMaxHops(val));

        return val;
    }

    @Override
    public int getNodes() {
        return nodes;
    }

    @Override
    public double getMaxLatency() {
        return maxlatency/(1.0*getNodes());
    }

    @Override
    public double getBandWidth() {
        return bandwidth;
    }
}
