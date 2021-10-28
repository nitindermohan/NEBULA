package de.tum.i11.bcsim.graph;

import de.tum.i11.bcsim.util.Util;

import java.util.*;

public class ScaleFreeStrategy implements GraphStrategy {

    private final int nodes;
    private final int m;
    private final int latency;
    private final Random rnd;
    private final double bandwidth;

    public ScaleFreeStrategy(int nodes, int m, int latency, double bandwidth) {
        this.nodes = nodes;
        this.m = Math.min(nodes, m);
        this.latency = latency;
        this.rnd = new Random();
        this.bandwidth = bandwidth;
    }

    @Override
    public ArrayList<List<Edge>> getEdges() {
        var graph = GraphUtil.toDiGraph(GraphUtil.toAdjList(m, latency, GraphUtil.rndSpanningTree(m)));
        for(int i = m; i < nodes; i++) {
            int final_i = i;
            HashSet<Integer> toSet = new HashSet<>(m);
            for(int j = 0; j < m; j++) {
                int next;
                do {
                    next = pickNode(graph);
                } while(toSet.contains(next));
                toSet.add(next);
                graph.get(next).add(new Edge(next, final_i, (int) Util.nextGaussian(latency)));
            }
            LinkedList<Edge> edges = new LinkedList<>();
            toSet.forEach(e -> edges.add(new Edge(final_i, e, (int) Util.nextGaussian(latency))));
            graph.add(edges);
        }
        GraphUtil.toUGraph(graph);
        shuffle(graph);
        return graph;
    }

    private List<Double> ps(ArrayList<List<Edge>> graph) {
        ArrayList<Double> ps = new ArrayList<>();
        int totalK = graph.stream().mapToInt(List::size).sum();
        double[] counter = {0};
        graph.forEach(l -> {
            double p = l.size()*1.0/totalK;
            counter[0] += p;
            ps.add(counter[0]);
        });
        return ps;
    }

    private void shuffle(ArrayList<List<Edge>> graph) {
        for(int i = 0; i < 2*graph.size(); i++) {
            int a = Util.getRndInt(0, graph.size());
            int b = Util.getRndInt(0, graph.size());
            GraphUtil.swap(a, b, graph);
        }
    }

    private int pickNode(ArrayList<List<Edge>> graph) {
        List<Double> ps = ps(graph);
        double r = rnd.nextDouble();
        for(int i = 0; i < ps.size(); i++) {
            if(r <= ps.get(i))
                return i;
        }
        return graph.size()-1;
    }

    @Override
    public int getNodes() {
        return nodes;
    }

    @Override
    public double getMaxLatency() {
        return latency;
    }

    @Override
    public double getBandWidth() {
        return bandwidth;
    }
}
