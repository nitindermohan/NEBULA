package de.tum.i11.bcsim.graph;

import de.tum.i11.bcsim.config.ConfigYAML;
import de.tum.i11.bcsim.util.Pair;
import de.tum.i11.bcsim.util.Util;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Logger;

public class GraphUtil {

    private static final Logger LOGGER = Logger.getLogger(GraphUtil.class.getName());

    /**
     * Create an adjacency list based on the given set of edges
     * @param nodes the number of nodes
     * @param latency the (gaussian) latency mean of edges
     * @param edges the edges
     * @return the adjacency list
     */
    public static ArrayList<List<Edge>> toAdjList(int nodes, int latency, Collection<Pair<Integer, Integer>> edges) {
        ArrayList<List<Edge>> list = new ArrayList<>(nodes);
        for(int i = 0; i < nodes; i++) {
            list.add(new LinkedList<>());
        }
        for(Pair<Integer, Integer> e : edges) {
            list.get(e._1).add(new Edge(e._1, e._2, (int) Util.nextGaussian(latency)));
        }
        return list;
    }

    /**
     * Transform the given graph into a directed graph (two edges between any two nodes)
     * @param ugraph the (undirected) graph
     * @return a new adjacency list of the directed graph
     */
    public static ArrayList<List<Edge>> toDiGraph(ArrayList<List<Edge>> ugraph) {
        ArrayList<List<Edge>> graph = new ArrayList<>();
        for(int i = 0; i < ugraph.size(); i++) {
            graph.add(new LinkedList<>());
        }
        for(List<Edge> l : ugraph) {
            for(Edge e : l) {
                graph.get(e.from).add(new Edge(e.from, e.to, e.latency));
                graph.get(e.to).add(new Edge(e.to, e.from, e.latency));
            }
        }
        return graph;
    }

    /**
     * Transform the given graph into an undirected graph (only one edge between any two nodes) by removing edges
     * @param digraph the (directed) graph
     */
    public static void toUGraph(ArrayList<List<Edge>> digraph) {
        for(int i = 0; i < digraph.size(); i++) {
            for(Edge e : digraph.get(i)) {
                digraph.get(e.to).removeIf(edge -> edge.to == e.from);
            }
        }
    }

    /**
     * Select n nodes from the given graph according to a selection strategy.
     * @param adL the graph
     * @param selection the node selection strategy
     * @param n the number of nodes to be selected
     * @return the selected nodes
     */
    public static LinkedList<Integer> selectNodes(ArrayList<Pair<Integer, List<Edge>>> adL, ConfigYAML.DPosBFT.NodeSelection selection, int n) {
        LinkedList<Integer> nodes = new LinkedList<>();
        if(selection == ConfigYAML.DPosBFT.NodeSelection.RANDOM) {
            nodes.addAll(Util.getUniqueRndInts(0, adL.size(), n));
            return nodes;
        }

        if(selection == ConfigYAML.DPosBFT.NodeSelection.BEST) {
            adL.sort((l1, l2) -> l2._2.size() - l1._2.size());
        }else if(selection == ConfigYAML.DPosBFT.NodeSelection.WORST) {
            adL.sort((l1, l2) -> l1._2.size() - l2._2.size());
        }
        for(int i = 0; i < n; i++) {
            nodes.add(adL.get(i)._1);
        }
        return nodes;
    }

    public static double getAvgPropDelayBetweenNodes(Collection<Integer> nodes, ArrayList<List<Edge>> graph) {
        var apsp = GraphUtil.apsp(GraphUtil.toDiGraph(graph));
        double total = 0;
        double count = 0;
        for(Integer i : nodes) {
            for(Integer j : nodes) {
                if(!i.equals(j)) {
                    total += apsp[i][j];
                    count++;
                }
            }
        }
        return total/count;
    }

    public static LinkedList<Integer> getPropDelayDistribution( ArrayList<List<Edge>> graph) {
        var apsp = GraphUtil.apsp(GraphUtil.toDiGraph(graph));
        LinkedList<Integer> l = new LinkedList<>();
        for(int i = 0; i<apsp.length;i++) {
            for(int j = i+1; j<apsp.length;j++) {
                l.add(apsp[i][j]);
            }
        }
        return  l;
    }

    public static double getMeanEdgeLatency(ArrayList<List<Edge>> graph) {
        LinkedList<Double> li = new LinkedList<>();
        graph.forEach(l -> l.forEach(e -> li.add((double) e.latency)));
        return li.stream().mapToDouble(e -> e).average().orElse(0);
    }

    public static double getAvgDegree(ArrayList<List<Edge>> graph) {
        return graph.stream().mapToDouble(List::size).sum()/graph.size();
    }

    public static int getMaxDegree(ArrayList<List<Edge>> graph) {
        return graph.stream().mapToInt(List::size).max().orElse(0);
    }

    public static HashMap<Integer, Integer> getDegreeDistribution(ArrayList<List<Edge>> graph) {
        HashMap<Integer, Integer> map = new HashMap<>();
        graph.stream().mapToInt(List::size).forEach(i -> map.merge(i, 1, Integer::sum));
        return map;
    }

    public static double getLocalClustering(ArrayList<List<Edge>> graph, int k) {
        int count = 0;
        double sum = 0;
        for(int i = 0; i < graph.size(); i++) {
            var next = graph.get(i);
            if(next.size() == k) {
                double nLinks = 0;
                for(Edge e : next) {
                    var links = graph.get(e.to);
                    for(Edge nl : links) {
                        nLinks += next.stream().filter(el -> el.to == nl.to).count();
                    }
                }
                nLinks /= k;
                count ++;
                sum += nLinks;
            }
        }
        if(count > 0 && k > 1) {
            return sum/count/(k-1);
        }
        return 0;
    }

    public static double getMeanLocalClustering(ArrayList<List<Edge>> graph) {
        var dist = getDegreeDistribution(graph);
        var kSum = dist.values().stream().mapToDouble(e -> e).sum();
        return dist.entrySet().stream().mapToDouble(e -> getLocalClustering(graph, e.getKey())*e.getValue()/kSum).sum();
    }

    public static double getAvgNeighborDegree(ArrayList<List<Edge>> graph, int k) {
        int count = 0;
        double sum = 0;
        for(int i = 0; i < graph.size(); i++) {
            var next = graph.get(i);
            if(next.size() == k) {
                double degSum = 0;
                for(Edge e : next) {
                    degSum += graph.get(e.to).size();
                }
                sum += degSum/k;
                count++;
            }
        }
        if(count>0) {
            return sum/count;
        }
        return 0;
    }

    public static double getNormalizedAvgAvgNeighborDegree(ArrayList<List<Edge>> graph) {
        var dist = getDegreeDistribution(graph);
        return dist.keySet().stream().mapToDouble(e -> getAvgNeighborDegree(graph, e)).sum()/dist.size()/(graph.size()-1);
    }

    public static boolean isConnected(ArrayList<List<Edge>> ugraph) {

        var graph = toDiGraph(ugraph);
        Boolean[] visited = new Boolean[graph.size()];
        Arrays.fill(visited, false);
        LinkedList<Integer> toVisit = new LinkedList<>();
        toVisit.add(0);

        while(!toVisit.isEmpty()) {
            int next = toVisit.removeFirst();
            visited[next] = true;
            for(Edge e : graph.get(next)) {
                if(!visited[e.to]) {
                    toVisit.add(e.to);
                }
            }
        }

        return Arrays.stream(visited).allMatch(b -> b);
    }

    public static LinkedList<Pair<Integer, Integer>> rndSpanningTree(int n) {
        ArrayList<ArrayList<Integer>> cliques = new ArrayList<>(n);
        LinkedList<Pair<Integer, Integer>> edges = new LinkedList<>();
        for(int i = 0; i < n; i++) {
            ArrayList<Integer> next = new ArrayList<>();
            next.add(i);
            cliques.add(next);
        }

        for(int i = 0; i < n-1; i ++) {
            int a = Util.rndInt(cliques.size());
            int b = Util.rndInt(cliques.size(), a);
            var cliqueA = cliques.get(a);
            var cliqueB = cliques.remove(b);
            var edge = new Pair<>(cliqueA.get(Util.rndInt(cliqueA.size())), cliqueB.get(Util.rndInt(cliqueB.size())));
            edges.add(edge);
            cliqueA.addAll(cliqueB);
        }
        return edges;
    }

    public static Collection<Pair<Integer, Integer>> create(int n, double d) {
        int es = (int) (d*n*(n-1))/2;
        LOGGER.finer("Creating graph with "+n+" nodes and density "+d+" ("+es+" edges)");

        LinkedList<Pair<Integer, Integer>> edges = rndSpanningTree(n);
        LOGGER.finer("Created random spanning tree ("+edges.size()+" edges)");

        int left = es-edges.size();
        LOGGER.finer("Adding "+left+" remaining edges randomly");

        var all = allEdges(n);
        all.removeAll(edges);
        all.removeAll(reversed(edges));
        LinkedList<Pair<Integer, Integer>> aa = new LinkedList<>(all);
        Collections.shuffle(aa);

        while(left --> 0) {
            edges.add(aa.removeFirst());
        }

        return edges;
    }

    private static HashSet<Pair<Integer, Integer>> allEdges(int n) {
        HashSet<Pair<Integer, Integer>> edges = new HashSet<>(n*n);
        for(int i = 0; i < n; i++) {
            for(int j = i+1; j < n; j++) {
                edges.add(new Pair<>(i, j));
            }
        }
        return edges;
    }

    private static ArrayList<Pair<Integer, Integer>> reversed(Collection<Pair<Integer, Integer>> edges) {
        ArrayList<Pair<Integer, Integer>> rev = new ArrayList<>(edges.size());
        for(Pair<Integer, Integer> p : edges) {
            rev.add(new Pair<>(p._2, p._1));
        }
        return rev;
    }

    public static int[] dijkstra(ArrayList<List<Edge>> adj, int start) {
        return dijkstra(adj, start, edge -> edge.latency);
    }

    public static int[] dijkstraHops(ArrayList<List<Edge>> adj, int start) {
        return dijkstra(adj, start, edge -> 1);
    }

    public static int[] dijkstra(ArrayList<List<Edge>> adj, int start, Function<Edge, Integer> weightFunc) {
        int n = adj.size();
        int[] d = new int[n];
        Arrays.fill(d, -1);
        d[start] = 0;
        PriorityQueue<Edge> pq = new PriorityQueue<>(n, Comparator.comparingInt((Edge o) -> o.latency));
        pq.offer(new Edge(0, start, 0));
        while(!pq.isEmpty()){
            Edge v = pq.poll();
            for(Edge w : adj.get(v.to)){
                int newDist = d[v.to] + weightFunc.apply(w);
                if(d[w.to] == -1 || newDist < d[w.to]){
                    if(d[w.to] != -1){
                        pq.removeIf(x -> x.to == w.to);
                    }
                    pq.offer(new Edge(0, w.to, newDist));
                    d[w.to] = newDist;
                }
            }
        }
        return d;
    }

    public static int[][] apsp(ArrayList<List<Edge>> adj) {
        int[][] weights = new int[adj.size()][];
        for(int i = 0; i < adj.size(); i++) {
            weights[i] = dijkstra(adj, i);
        }
        return weights;
    }

    public static int[][] apspHops(ArrayList<List<Edge>> adj) {
        int[][] weights = new int[adj.size()][];
        for(int i = 0; i < adj.size(); i++) {
            weights[i] = dijkstraHops(adj, i);
        }
        return weights;
    }

    public static List<Integer> tsm(int[][] weights, List<Integer> ids) {
        double startingTemperature = 10;
        int numberOfIterations = 10000;
        double coolingRate = 0.9;
        ArrayList<Integer> travel = new ArrayList<>(ids);

        if(travel.size() <= 2)
            return travel;

        Collections.shuffle(travel);

        double t = startingTemperature;

        double bestDistance = getDistance(weights, travel);

        for (int i = 0; i < numberOfIterations; i++) {
            if (t > 0.1) {
                int a = Util.rndInt(travel.size());
                int b = Util.rndInt(travel.size(), a);
                swap(travel, a, b);
                double currentDistance = getDistance(weights, travel);
                if (currentDistance < bestDistance) {
                    bestDistance = currentDistance;
                } else if (Math.exp((bestDistance - currentDistance) / t) < Math.random()) {
                    swap(travel, a, b);
                }
                t *= coolingRate;
            }
        }
        return travel;
    }

    public static int getDistance(int[][] weights, List<Integer> ids) {
        int distance = 0;
        for(int i = 0; i < ids.size(); i++) {
            int from = ids.get(i);
            int to = ids.get(i+1>=ids.size()?0:i+1);
            distance += weights[from][to];
        }
        return distance;
    }

    private static void swap(List l, int a, int b) {
        Object x = l.get(a);
        Object y = l.get(b);
        l.set(b, x);
        l.set(a, y);
    }

    public static void swap(int a, int b, ArrayList<List<Edge>> graph) {
        if(a == b)
            return;
        var listA = graph.get(a);
        var listB = graph.get(b);
        graph.set(a, listB);
        graph.set(b, listA);

        graph.forEach(l -> l.forEach(edge -> {
            if(edge.to == a) {
                edge.to = b;
            } else if(edge.to == b) {
                edge.to = a;
            }
            if(edge.from == a) {
                edge.from = b;
            }else if(edge.from == b) {
                edge.from = a;
            }
        }));
    }

    public static String toGraphviz(ArrayList<List<Edge>> adjList) {
        StringBuilder b = new StringBuilder("graph G {");

        for(int i = 0; i < adjList.size(); i++) {
            b.append(i).append(";");
            for(Edge e : adjList.get(i)) {
                b.append(e.from).append("--").append(e.to).append(" [label=\"").append(e.latency).append("\"];");
            }
        }

        return b.append("}").toString();
    }

    public static void renderGraphiz(String fileName, ArrayList<List<Edge>> adjList) throws IOException {
        Graphviz.fromString(toGraphviz(adjList)).height(1000).width(2000).render(Format.SVG).toFile(new File(fileName));
    }

    public static double getAvgPropDelay(ArrayList<List<Edge>> adjList) {
        var apsp = GraphUtil.apsp(GraphUtil.toDiGraph(adjList));
        double total = 0;
        for(int[] ar : apsp) {
            total += 1.0* Arrays.stream(ar).sum()/(ar.length-1);
        }
        return total/apsp.length;
    }

    public static double getMedianPropDelay(ArrayList<List<Edge>> adjList) {
        var apsp = GraphUtil.apsp(GraphUtil.toDiGraph(adjList));
        ArrayList<Integer> latencies = new ArrayList<>(apsp.length*apsp[0].length);
        for(int[] ar : apsp) {
            for(int e : ar) {
                if(e != 0) {
                    latencies.add(e);
                }
            }
        }
        latencies.sort(Comparator.comparingInt(e -> e));
        if(latencies.size() % 2 == 0) {
            return (latencies.get(latencies.size()/2-1)+latencies.get(latencies.size()/2))/2.0;
        } else {
            return latencies.get(latencies.size()/2);
        }
    }

    public static double getAvgHops(ArrayList<List<Edge>> adjList) {
        var apsp = GraphUtil.apspHops(GraphUtil.toDiGraph(adjList));
        double total = 0;
        for(int[] ar : apsp) {
            total += 1.0* Arrays.stream(ar).sum()/(ar.length-1);
        }
        return total/apsp.length;
    }

    public static int getMaxLatency(ArrayList<List<Edge>> adjList) {
        var apsp = GraphUtil.apsp(GraphUtil.toDiGraph(adjList));
        return Arrays.stream(apsp).mapToInt(ar -> Arrays.stream(ar).max().orElse(0)).max().orElse(0);
    }

    public static int getMaxHops(ArrayList<List<Edge>> adjList) {
        var apsp = GraphUtil.apspHops(GraphUtil.toDiGraph(adjList));
        return Arrays.stream(apsp).mapToInt(ar -> Arrays.stream(ar).max().orElse(0)).max().orElse(0);
    }
}
