package de.tum.i11.bcsim.graph;

import java.util.*;
import java.util.logging.Logger;

public class RndGraphWithAvgPropagationDelay implements GraphStrategy {
    private static final Logger LOGGER = Logger.getLogger(GraphStrategy.class.getName());

    private final int nodes;
    private final int avgPropagationDelay;
    private final double density;
    private final double bandwidth;

    public RndGraphWithAvgPropagationDelay(int nodes, int avgPropagationDelay, double density, double bandwidth) {
        this.nodes = nodes;
        this.avgPropagationDelay = avgPropagationDelay;
        this.density = density;
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
            double m = (left+right)/2.0;

            RndGraphStrategy s = new RndGraphStrategy(nodes, (int) Math.round(m), density, bandwidth);
            graph = s.getEdges();
            avg = GraphUtil.getAvgPropDelay(graph);
            found.put(avg, graph);

            if(avg > avgPropagationDelay) {
                right = (int) m;
            } else if (avg < avgPropagationDelay) {
                left = (int) Math.ceil(m);
            } else break;

        }

        var entry = found.entrySet().stream().min(Comparator.comparingDouble(o -> Math.abs(avgPropagationDelay - o.getKey())));

        LOGGER.info("Best avg: "+entry.map(Map.Entry::getKey).orElse(null));

        return entry.map(Map.Entry::getValue).orElse(null);
    }

    @Override
    public int getNodes() {
        return nodes;
    }

    @Override
    public double getMaxLatency() {
        return (int) (avgPropagationDelay*density);
    }

    @Override
    public double getBandWidth() {
        return bandwidth;
    }
}
