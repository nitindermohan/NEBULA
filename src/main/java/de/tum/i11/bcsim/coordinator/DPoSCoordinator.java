package de.tum.i11.bcsim.coordinator;

import de.tum.i11.bcsim.graph.Edge;
import de.tum.i11.bcsim.graph.GraphUtil;
import de.tum.i11.bcsim.peer.DPoSPeer;
import de.tum.i11.bcsim.peer.Peer;
import de.tum.i11.bcsim.proto.Messages;
import de.tum.i11.bcsim.config.Config;
import de.tum.i11.bcsim.util.Pair;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

public class DPoSCoordinator extends BlockchainCoordinator<DPoSPeer> {
    private static final Logger LOGGER = Logger.getLogger(DPoSCoordinator.class.getName());
    private List<Integer> consensusNodes;
    private final CompletableFuture<Void> consensusNodesReceived;

    public DPoSCoordinator(InetSocketAddress addr, Config config) {
        super(addr, config, (id, address, coordinator) -> new DPoSPeer(id, address, coordinator, config));
        this.consensusNodesReceived = new CompletableFuture<>();
    }

    public List<Integer> getConsensusNodes() {
        return consensusNodes;
    }

    private double getAvgPropDelayInConsensus() {
        return GraphUtil.getAvgPropDelayBetweenNodes(consensusNodes, adjList);
    }

    @Override
    protected void sendAdjList() {
        // before sending the network graph, select and store consensus nodes (BPS)
        if(isOrchestrator) {
            calcConsensusNodes(adjList);
            if(config.getDPoSStrat().consensusOnOrchestrator) {
                ArrayList<Integer> oldConsensus = new ArrayList<>(consensusNodes);
                LinkedList<Integer> newConsensus = new LinkedList<>();
                for(int i = 0; i < consensusNodes.size(); i++) {
                    newConsensus.add(i);
                    GraphUtil.swap(i,oldConsensus.get(i),adjList);
                    for(int j = i+1; j < consensusNodes.size(); j++) {
                        if(oldConsensus.get(j) == i) {
                            oldConsensus.set(j, oldConsensus.get(i));
                        }
                    }
                }
                consensusNodes = newConsensus;
            }
        }
        super.sendAdjList();
    }

    private void calcConsensusNodes(ArrayList<List<Edge>> adL) {
        if(config.getDPoSStrat().consensusNodes != null && config.getDPoSStrat().consensusNodes.size() > 0) {
            // consensus nodes were explicitly defined in config
            this.consensusNodes = config.getDPoSStrat().consensusNodes;
        } else {
            // consensus nodes are to be selected automatically
            // create a copy of the graph as an edge list
            ArrayList<List<Edge>> digraph = GraphUtil.toDiGraph(adL);
            ArrayList<Pair<Integer, List<Edge>>> adLcopy = new ArrayList<>(digraph.size());
            for(int i = 0; i < digraph.size(); i++) {
                adLcopy.add(new Pair<>(i, digraph.get(i)));
            }

            // select the configured number of BPs according to the configured strategy
            this.consensusNodes = GraphUtil.selectNodes(adLcopy, config.getDPoSStrat().getNodeSelection(), config.getDPoSStrat().consensusNodeNum);
            LOGGER.info("Automatically picked consensus Nodes with Strategy "+config.getDPoSStrat().getNodeSelection()+": "+consensusNodes);

            LOGGER.info("Avg Prop delay between consensus Nodes: "+getAvgPropDelayInConsensus());
            if(!config.getDPoSStrat().randomShuffle) {
                consensusNodes = GraphUtil.tsm(GraphUtil.apsp(digraph), consensusNodes);
                LOGGER.info("Sorted nodes according to TSM: "+consensusNodes);
            }
        }
    }

    @Override
    protected void connectPeers(List<Messages.Node> nodes, ArrayList<List<Edge>> adL) {
        LOGGER.info("Connecting peers");
        if(isOrchestrator) {
            // before connecting peers, send consensusNodes
            var msg = Messages.CoordinatorMessage.newBuilder().setConsensusNodes(Messages.ConsensusNodes.newBuilder().addAllNode(consensusNodes)).build();
            LOGGER.info("Sending consensus Nodes");
            channels.values().forEach(c -> c.writeAndFlush(msg));
            doConnectPeers(nodes, adL);
        } else {
            // wait until consensus nodes received, then connect peers
            consensusNodesReceived.whenComplete((s, e) -> doConnectPeers(nodes, adL));
        }
    }

    private void doConnectPeers(List<Messages.Node> nodes, ArrayList<List<Edge>> adL) {
        for(Integer id : consensusNodes) {
            DPoSPeer peer = peers.get(id);
            if(peer != null) {
                peer.setBlockProducer();
            }
        }
        super.connectPeers(nodes, adL);
        LOGGER.info("Connecting consensus nodes directly");

        var digraph = GraphUtil.toDiGraph(adL);
        for(int i = 0; i < consensusNodes.size(); i++ ) {
            int a = consensusNodes.get(i);
            Peer p = peers.get(a);
            if(p == null)
                continue;
            int[] weights = GraphUtil.dijkstra(digraph, a);
            for (int b : consensusNodes) {
                if (a >= b) {
                    p.setLatency(b, weights[b]);
                    continue;
                }

                if (!p.isConnectedTo(b)) {
                    p.setLatency(b, weights[b]);
                    p.connect(peerAddresses[b]).syncUninterruptibly();
                }
            }
        }
    }

    @Override
    protected CompletableFuture<Void> onPreReady() {
        // assert that consensus nodes are connected with each other
        for(Integer from : consensusNodes) {
            for(Integer to : consensusNodes) {
                if(!from.equals(to)) {
                    Peer p = peers.get(from);
                    if(p != null && !p.isConnectedTo(to)) {
                        return CompletableFuture.failedFuture(new Exception("Did not connect consensus node "+from+" to "+to));
                    }
                }
            }
        }
        LOGGER.info("Avg Prop delay between consensus Nodes: "+getAvgPropDelayInConsensus());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected void onMessage(ChannelHandlerContext ctx, Messages.CoordinatorMessage msg) {
        super.onMessage(ctx, msg);
        if(msg.hasConsensusNodes()) {
            this.consensusNodes = msg.getConsensusNodes().getNodeList();
            LOGGER.info("Received consensus Nodes: "+consensusNodes);
            consensusNodesReceived.complete(null);
        }
    }
}
