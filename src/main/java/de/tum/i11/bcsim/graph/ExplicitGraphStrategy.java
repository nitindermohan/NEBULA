package de.tum.i11.bcsim.graph;

import de.tum.i11.bcsim.config.ConfigYAML;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class ExplicitGraphStrategy implements GraphStrategy {
    private static final Logger LOGGER = Logger.getLogger(ExplicitGraphStrategy.class.getName());

    private final HashMap<Integer, LinkedList<ConfigYAML.Explicit.Peer.Edge>> peers;
    private int maxLatency;
    private final double bandwidth;

    public ExplicitGraphStrategy(List<ConfigYAML.Explicit.Peer> peers, double bandwidth) {
        this.peers = new HashMap<>();
        this.bandwidth = bandwidth;
        peers.forEach(p -> {
            this.peers.put(p.id, new LinkedList<>(p.edges));
            for(ConfigYAML.Explicit.Peer.Edge e : p.edges) {
                maxLatency = Math.max(maxLatency, e.latency);
            }
        });

    }

    public ExplicitGraphStrategy(String fileName, double bandwidth) {
        class Edge {
            String from, to;
            int latency;
            private Edge(String[] a) {
                this.from = a[0];
                this.to = a[1];
                this.latency = Integer.parseInt(a[2]);
            }
        }

        HashMap<String, Integer> ids = new HashMap<>();
        int[] count = new int[]{0};
        LinkedList<Edge> edges = new LinkedList<>();

        try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
            stream.forEach(l -> {
                Edge next = new Edge(l.split("\\s"));
                if(!ids.containsKey(next.from)) {
                    ids.put(next.from, count[0]++);
                }
                if(!ids.containsKey(next.to)) {
                    ids.put(next.to, count[0]++);
                }
                edges.add(next);
            });
        } catch (IOException e) {
            LOGGER.throwing(getClass().getName(), "constructor", e);
            System.exit(1);
        }

        HashMap<Integer, ConfigYAML.Explicit.Peer> peers = new HashMap<>(ids.size());
        ids.values().forEach(i -> {
            var p = new ConfigYAML.Explicit.Peer();
            p.id=i;
            peers.put(i, p);
        });

        edges.forEach(e -> {
            peers.get(ids.get(e.from)).edges.add(new ConfigYAML.Explicit.Peer.Edge(ids.get(e.to), e.latency));
        });

        this.peers = new HashMap<>();
        this.bandwidth = bandwidth;
        peers.values().forEach(p -> {
            this.peers.put(p.id, new LinkedList<>(p.edges));
            for(ConfigYAML.Explicit.Peer.Edge e : p.edges) {
                maxLatency = Math.max(maxLatency, e.latency);
            }
        });
    }


    @Override
    public ArrayList<List<Edge>> getEdges() {
        ArrayList<List<Edge>> adjList = new ArrayList<>(getNodes());
        for(int i = 0; i < getNodes(); i++) {
            var adj = peers.get(i);
            if(adj == null)
                throw new IllegalArgumentException("Node Ids in explicit network strategy have to be numbered from 0 to n");
            LinkedList<Edge> edges = new LinkedList<>();
            for(ConfigYAML.Explicit.Peer.Edge e : adj) {
                if(e.id == i || e.id < 0 || e.id >= getNodes())
                    throw new IllegalArgumentException("Node Ids in explicit network strategy have to be numbered from 0 to n");
                edges.add(new Edge(i, e.id, e.latency));
            }
            adjList.add(edges);
        }
        GraphUtil.toUGraph(adjList);
        if(!GraphUtil.isConnected(adjList)) {
            throw new IllegalArgumentException("Edges in explicit graph strategy need to form connected, undirected graph");
        }
        return adjList;
    }

    @Override
    public int getNodes() {
        return peers.size();
    }

    public double getMaxLatency() {
        return maxLatency;
    }

    @Override
    public double getBandWidth() {
        return bandwidth;
    }
}
