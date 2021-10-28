package de.tum.i11.bcsim;

import de.tum.i11.bcsim.coordinator.Coordinator;
import de.tum.i11.bcsim.graph.*;
import de.tum.i11.bcsim.peer.Peer;
import de.tum.i11.bcsim.proto.Messages;
import de.tum.i11.bcsim.config.Config;
import de.tum.i11.bcsim.config.ConfigYAML;
import de.tum.i11.bcsim.util.Util;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class TestNetwork {
    public static class TestPeer extends Peer {
        public TestPeer(int id, InetAddress addr, Coordinator<? extends Peer> coordinator) {
            super(id, addr, coordinator);
        }
        @Override
        protected void onMessage(ChannelHandlerContext ctx, Messages.P2PMessage msg) {}
        @Override
        protected void onConnect(ChannelHandlerContext ctx, int id) {}
        @Override
        public void onStart(long startTime) {}
    }
    public static class TestCoordinator extends Coordinator<TestPeer> {
        public TestCoordinator(InetSocketAddress addr, Config config) {
            super(addr, config, TestPeer::new);
        }
        @Override
        protected CompletableFuture<Void> onPreReady() {
            return CompletableFuture.completedFuture(null);
        }
        @Override
        protected void onStart(long startTime) {}
        @Override
        protected CompletableFuture<Void> onPreClose() {
            return CompletableFuture.completedFuture(null);
        }
        @Override
        protected void onStop() {}
        @Override
        protected void onMessage(ChannelHandlerContext ctx, Messages.CoordinatorMessage message) {}
    }

    @Test
    void testSingle() {
        assertDoesNotThrow(() -> testGraphStrategy(new RndGraphStrategy(1, 10, 0.8, 2000), 1));
    }

    @Test
    void testRandom() {
        GraphStrategy gs3 = new RndGraphStrategy(3, 10, 0.8, 2000);
        GraphStrategy gs7 = new RndGraphStrategy(7, 10, 0.8, 2000);
        GraphStrategy gs14 = new RndGraphStrategy(14, 10, 0.8, 2000);
        GraphStrategy gs23 = new RndGraphStrategy(23, 10, 0, 2000);

        int[] coordNums = {1, 3, 6};

        assertDoesNotThrow(() -> testGraphStrategy(gs3, coordNums[0]));
        assertDoesNotThrow(() -> testGraphStrategy(gs3, coordNums[1]));

        for(int i = 0; i < coordNums.length; i++) {
            final int final_i = i;
            assertDoesNotThrow(() -> testGraphStrategy(gs7, coordNums[final_i]));
            assertDoesNotThrow(() -> testGraphStrategy(gs14, coordNums[final_i]));
            assertDoesNotThrow(() -> testGraphStrategy(gs23, coordNums[final_i]));
        }
    }

    @Test
    void testScaleFree() {
        GraphStrategy gs3 = new ScaleFreeStrategy(3, 2, 10, 2000);
        GraphStrategy gs7 = new ScaleFreeStrategy(7, 2, 10, 2000);
        GraphStrategy gs14 = new ScaleFreeStrategy(14, 3, 10, 2000);
        GraphStrategy gs23 = new ScaleFreeStrategy(23, 4, 10, 2000);

        int[] coordNums = {1, 3, 6};

        assertDoesNotThrow(() -> testGraphStrategy(gs3, coordNums[0]));
        assertDoesNotThrow(() -> testGraphStrategy(gs3, coordNums[1]));

        for(int i = 0; i < coordNums.length; i++) {
            final int final_i = i;
            assertDoesNotThrow(() -> testGraphStrategy(gs7, coordNums[final_i]));
            assertDoesNotThrow(() -> testGraphStrategy(gs14, coordNums[final_i]));
            assertDoesNotThrow(() -> testGraphStrategy(gs23, coordNums[final_i]));
        }
    }

    @Test
    void testExplicit() {
        GraphStrategy gs = new ExplicitGraphStrategy(List.of(
                new ConfigYAML.Explicit.Peer(0, List.of(
                        new ConfigYAML.Explicit.Peer.Edge(1, 1),
                        new ConfigYAML.Explicit.Peer.Edge(2, 2),
                        new ConfigYAML.Explicit.Peer.Edge(3, 3)
                )),
                new ConfigYAML.Explicit.Peer(1, List.of()),
                new ConfigYAML.Explicit.Peer(2, List.of(new ConfigYAML.Explicit.Peer.Edge(1,2))),
                new ConfigYAML.Explicit.Peer(3, List.of())
        ), 2000);

        int[] coordNums = {1, 3};

        for(int i = 0; i < coordNums.length; i++) {
            final int final_i = i;
            assertDoesNotThrow(() -> testGraphStrategy(gs, coordNums[final_i]));
        }
    }

    @Test
    void testComputingShares() {
        GraphStrategy gs14 = new RndGraphStrategy(10, 10, 0.8, 2000);

        List<Config.CoordinatorEntry> coords = List.of(
                new Config.CoordinatorEntry("127.0.0.1:5151", 2.0/3),
                new Config.CoordinatorEntry("127.0.0.1:5152", 1.0/6),
                new Config.CoordinatorEntry("127.0.0.1:5153", 1.0/6)
        );

        assertDoesNotThrow(() -> testGraphStrategy(gs14, 3, coords));
    }

    void testGraphStrategy(GraphStrategy gs, int coordNum) throws IOException {
        testGraphStrategy(gs, coordNum, null);
    }

    public static int getRndFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            e.printStackTrace();
            return 5153;
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                    Thread.sleep(100);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    void testGraphStrategy(GraphStrategy gs, int coordNum, List<Config.CoordinatorEntry> coords) throws IOException {
        Config c = new Config("src/test/java/de/tum/i11/bcsim/config.yaml");
        c.setGraphStrategy(gs);

        assertDoesNotThrow(() -> GraphUtil.toGraphviz(gs.getEdges()));

        if(coords != null) {
            c.setCoordinatorEntries(coords);
        } else {

            InetSocketAddress[] ads = new InetSocketAddress[coordNum];
            for (int i = 0; i < coordNum; i++) {
                ads[i] = new InetSocketAddress("127.0.0.1", getRndFreePort());
            }
            c.setCoordinatorAddresses(List.of(ads));

        }

        TestCoordinator[] cs = new TestCoordinator[coordNum];
        CompletableFuture[] sfs = new CompletableFuture[coordNum];
        CompletableFuture[] cfs = new CompletableFuture[coordNum];

        for(int i = 0; i < coordNum; i++) {
            cs[i] = new TestCoordinator(c.getCoordinatorAddresses().get(i).address, c);
            sfs[i] = cs[i].startedFuture();
            cfs[i] = cs[i].closedFuture();
        }

        cs[0].startAsOrchestrator();

        CompletableFuture.allOf(sfs).join();

        var graph  = cs[0].getAdjList();
        assertTrue(GraphUtil.isConnected(graph));

        HashMap<Integer, TestPeer> peers = new HashMap<>();
        for(TestCoordinator tc : cs) {
            tc.getPeers().forEach((key, value) -> {
                assertEquals((int) key, value.getId());
                assertFalse(peers.containsKey(key));
                assertFalse(value.isClosed());
                peers.put(key, value);
            });
        }

        Peer pe = peers.values().iterator().next();
        assertThrows(IllegalArgumentException.class, () -> pe.setLatency(0, -1));
        pe.broadcastNow(Messages.P2PMessage.newBuilder().build());
        var adj = GraphUtil.toDiGraph(graph).get(pe.getId());
        for(int i = 0; i < gs.getNodes(); i++) {
            if(i != pe.getId()) {
                final int final_i = i;
                if(adj.stream().noneMatch(e -> e.to == final_i)) {
                    assertFalse(pe.isConnectedTo(i));
                    assertTrue(pe.sendNow(i, Messages.P2PMessage.newBuilder().build()).isDone());
                }
            }
        }

        assertEquals(graph.size(), peers.size());
        assertEquals(graph.size(), gs.getNodes());
        for(int i = 0; i < peers.size(); i++) {
            assertTrue(peers.containsKey(i));
        }

        int[] totalChannels = {0};
        peers.values().forEach(p -> totalChannels[0]+=p.getConnections().size());
        int[] totalEdges = {0};
        graph.forEach(n -> totalEdges[0]+=n.size());
        assertEquals(totalChannels[0], 2 * totalEdges[0]);

        for(int i = 0; i < peers.size(); i++) {
            var adjacency = graph.get(i);

            for(Edge edge : adjacency) {
                assertTrue(peers.get(edge.from).isConnectedTo(edge.to));
                assertTrue(peers.get(edge.to).isConnectedTo(edge.from));
                assertEquals((int) peers.get(edge.from).getLatencyMap().get(edge.to), edge.latency);
                assertEquals((int) peers.get(edge.to).getLatencyMap().get(edge.from), edge.latency);
            }
        }

        cs[0].stop(null, "Main", "Test finished", true);

        CompletableFuture.allOf(cfs).join();

        for(TestCoordinator co : cs) {
            assertTrue(co.isClosed());
            co.getPeers().values().forEach(p -> assertTrue(p.isClosed()));
            co.getPeers().values().forEach(p -> assertTrue(p.close().isDone()));
        }
    }

    @Test
    void testDijkstra() {
        ArrayList<List<Edge>> adj = new ArrayList<>();
        adj.add(List.of(new Edge(0, 1, 1)));
        adj.add(List.of(new Edge(1, 2, 2), new Edge(1, 3, 4)));
        adj.add(List.of(new Edge(1, 3, 1)));
        adj.add(List.of());
        int[] expected = {0, 1, 3, 4};
        int[] result = GraphUtil.dijkstra(adj, 0);
        for(int i = 0; i < adj.size(); i++) {
            assertEquals(expected[i], result[i]);
        }
    }

    @Test
    void testTSM() {
        RndGraphStrategy s = new RndGraphStrategy(100, 10, 0.3, 2000);
        var graph = s.getEdges();
        ArrayList<List<Edge>> digraph = GraphUtil.toDiGraph(graph);
        int[][] weights = GraphUtil.apsp(digraph);
        var l = new ArrayList<>(List.of(0,1,2,3,4));

        int min = Integer.MAX_VALUE;
        for(int i = 0; i < 1000000; i++) {
            min = Math.min(min, GraphUtil.getDistance(weights, l));
            Collections.shuffle(l);
        }

        assertEquals(min, GraphUtil.getDistance(weights, GraphUtil.tsm(weights, l)));
    }

    @RepeatedTest(10)
    void testAvgPropagationDelayRndGraph() {
        int avgLat = Util.getRndInt(100, 10000);
        double density = 1.0/Util.getRndInt(1, 20);
        int nodes = Util.getRndInt(50, 500);

        RndGraphWithAvgPropagationDelay s = new RndGraphWithAvgPropagationDelay(nodes, avgLat, density, 2000);
        var graph = s.getEdges();
        double lat = GraphUtil.getAvgPropDelay(graph);

        // allow 1% error
        assertTrue(Math.abs(avgLat-lat) < avgLat*0.05);
    }

    @RepeatedTest(10)
    void testAvgPropagationDelayScaleFreeGraph() {
        int avgLat = Util.getRndInt(100, 10000);
        int m = Util.getRndInt(2, 6);
        int nodes = Util.getRndInt(50, 500);

        ScaleFreeGraphWithAvgPropagationDelay s = new ScaleFreeGraphWithAvgPropagationDelay(nodes, m, avgLat, 2000);
        var graph = s.getEdges();
        double lat = GraphUtil.getAvgPropDelay(graph);

        // allow 2% error
        assertTrue(Math.abs(avgLat-lat) < avgLat*0.05);
    }


    @Test
    void testGraphUtil1() {
        GraphStrategy gs = new ScaleFreeStrategy(50, 2, 100, 2000);
        double avgDeg=0, maxDeg=0, normAvg=0, meanLoc=0;
        for(int i = 0; i < 100; i++) {
            var g = GraphUtil.toDiGraph(gs.getEdges());
            avgDeg += GraphUtil.getAvgDegree(g);
            maxDeg += GraphUtil.getMaxDegree(g);
            normAvg += GraphUtil.getNormalizedAvgAvgNeighborDegree(g);
            meanLoc += GraphUtil.getMeanLocalClustering(g);
        }
        System.out.println("Avg Degree: "+avgDeg/100);
        System.out.println("Max Degree: "+maxDeg/100);
        System.out.println("NormAvgAvgNeighDegr: "+normAvg/100);
        System.out.println("MeanLocalClustering: "+meanLoc/100);
    }

    @Test
    void testGraphUtil2() {
        var gs = new ScaleFreeGraphWithAvgPropagationDelay(50, 2, 44000, 2000);
        LinkedList<Double> lats = new LinkedList<>();
        for(int i = 0; i < 10; i++) {
            lats.add(GraphUtil.getMeanEdgeLatency(gs.getEdges()));
        }
        System.out.println(GraphUtil.getMedianPropDelay(GraphUtil.toDiGraph(gs.getEdges())));
        System.out.println(lats);
        System.out.println(lats.stream().mapToDouble(e -> e).sum()/lats.size());
    }

    @Test
    void testGraphUtil3() {
        var gs = new ScaleFreeStrategy(50, 2, 17564, 2000);
        LinkedList<Double> lats = new LinkedList<>();
        LinkedList<Double> hops = new LinkedList<>();
        for(int i = 0; i < 100; i++) {
            var e = gs.getEdges();
            lats.add(GraphUtil.getAvgPropDelay(e));
            hops.add(GraphUtil.getAvgHops(e));
        }
        System.out.println(hops);
        System.out.println(hops.stream().mapToDouble(e -> e).sum()/hops.size());
        System.out.println(lats);
        System.out.println(lats.stream().mapToDouble(e -> e).sum()/lats.size());
    }

    @Test
    void testExplicitFile() throws IOException {
        var gs = new ExplicitGraphStrategy("src/test/java/de/tum/i11/bcsim/cloudGraph.txt", 2000);
        var graph = gs.getEdges();
        assertTrue(GraphUtil.isConnected(graph));
        assertEquals(graph.size(), 2695);
        System.out.println(GraphUtil.getAvgDegree(graph));
        System.out.println(GraphUtil.getMedianPropDelay(graph));
        System.out.println(GraphUtil.getAvgPropDelay(graph));
        System.out.println(GraphUtil.getAvgHops(graph));
        System.out.println(GraphUtil.getMaxHops(graph));
        System.out.println(GraphUtil.getMaxLatency(graph));
//        var l = GraphUtil.getPropDelayDistribution(graph);
//        FileWriter fw = new FileWriter("dist.txt");
//
//        fw.write("prop<-c(");
//        int c = 0;
//        int c2 = 0;
//        for(Integer i : l) {
//
//            if(c++%100==0) {
//                if(c2++%300==0){
//                    fw.write("\n");
//                }
//            if(c==l.size()) {
//                fw.write(i+")");
//            } else {
//                fw.write(i+",");
//            }}
//        }
//        fw.flush();
//        fw.close();
    }



//    @Test
//    public void main() {
////        ScaleFreeGraphWithAvgPropagationDelay s = new ScaleFreeGraphWithAvgPropagationDelay(2000, 2, 2300/3, 2000);
////        s.getEdges();
//
//
//        StringBuilder xBPs = new StringBuilder("xBPs<-c(");
//        StringBuilder yN500M2SBest = new StringBuilder("yN50M2SBest<-c(");
//        StringBuilder yN500M2SWorst = new StringBuilder("yN50M2SWorst<-c(");
//        StringBuilder yN500M2SRandom = new StringBuilder("yN50M2SRandom<-c(");
//
//        AtomicBoolean first = new AtomicBoolean(true);
//        Arrays.stream("3.0,5.0,7.0,9.0,13.0,17.0,21.0,25.0,29.0,33.0,37.0,41.0,45.0,49.0".split(",")).mapToInt(s -> ((int)Double.parseDouble(s))).forEach(n -> {
//            if(!first.get()) {
//                xBPs.append(",");yN500M2SBest.append(",");yN500M2SWorst.append(",");yN500M2SRandom.append(",");
//            }
//            first.set(false);
//            ScaleFreeGraphWithAvgPropagationDelay s = new ScaleFreeGraphWithAvgPropagationDelay(50, 2, 44438, 2000);
//            xBPs.append(n);
//            yN500M2SBest.append(get(s, ConfigYAML.DPosBFT.NodeSelection.BEST,n,20));
//            yN500M2SWorst.append(get(s, ConfigYAML.DPosBFT.NodeSelection.WORST,n,20));
//            yN500M2SRandom.append(get(s, ConfigYAML.DPosBFT.NodeSelection.RANDOM,n,20));
//        });
//        xBPs.append(")");yN500M2SBest.append(")");yN500M2SWorst.append(")");yN500M2SRandom.append(")");
//        System.out.println(xBPs.toString());
//        System.out.println(yN500M2SBest.toString());
//        System.out.println(yN500M2SWorst.toString());
//        System.out.println(yN500M2SRandom.toString());
//    }

//    public static double get(GraphStrategy s, ConfigYAML.DPosBFT.NodeSelection selection, int n, int repeat) {
//        double count = 0;
//        double total = 0;
//        for(int i = 0; i < repeat; i++) {
//            ArrayList<List<Edge>> digraph = GraphUtil.toDiGraph(s.getEdges());
//            ArrayList<Pair<Integer, List<Edge>>> graph = new ArrayList<>(digraph.size());
//            for(int j = 0; j < digraph.size(); j++) {
//                graph.add(new Pair<>(j, digraph.get(j)));
//            }
//            total += GraphUtil.getAvgPropDelayBetweenNodes(GraphUtil.selectNodes(graph, selection, n), digraph);
//            count++;
//        }
//        return total/count;
//    }
}
