package de.tum.i11.bcsim.coordinator;

import de.tum.i11.bcsim.graph.Edge;
import de.tum.i11.bcsim.graph.GraphUtil;
import de.tum.i11.bcsim.node.Node;
import de.tum.i11.bcsim.peer.Peer;
import de.tum.i11.bcsim.peer.PeerSupplier;
import de.tum.i11.bcsim.proto.Messages;
import de.tum.i11.bcsim.config.Config;
import de.tum.i11.bcsim.util.CPULoadMeasure;
import de.tum.i11.bcsim.util.Pair;
import de.tum.i11.bcsim.util.Timeout;
import io.netty.channel.*;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@ChannelHandler.Sharable
public abstract class Coordinator<P extends Peer> extends SimpleChannelInboundHandler<Messages.CoordinatorMessage> {

    private static final Logger LOGGER = Logger.getLogger(Coordinator.class.getName());

    protected final Config config;
    private final Node node;
    protected boolean isOrchestrator;

    protected final CPULoadMeasure cpuMeasure;

    protected final List<Config.CoordinatorEntry> coordinators;     // list of all coordinator addresses (including this one's)
    protected final HashMap<InetSocketAddress, Channel> channels;
    protected final HashMap<Integer, P> peers;             // all peers coordinated by this coordinator
    protected final InetSocketAddress[] peerAddresses;        // addresses of all peers in the network

    protected Pair<Integer, Integer> peerIdBounds;              // inclusive bounds of nodeIds this coordinator is responsible for

    protected ArrayList<List<Edge>> adjList;               // adjacency list of peers this coordinator is responsible for (or all peers if this is the orchestrator)

    private final Object nodesLock = new Object();
    private List<Messages.Node> nodeList;                   // list maintained by orchestrator to aggregate all node assignment responses

    private AtomicInteger readyCount; // counter to keep track of started coordinators
    private volatile boolean closing;
    private Timeout announceTimeout; // timeout to receive peer announcements

    private PeerSupplier<P, Coordinator<P>> peerSupplier;

    private final CompletableFuture<Void> startedFuture;
    private final CompletableFuture<Boolean> closedFuture;
    private final long startWait; // time in ms to wait for network to synchronize
    protected long executionTime; // hold timestamp to calculate total execution time

    protected final double bandwidth;

    protected final InetSocketAddress address;

    public Coordinator(InetSocketAddress addr, Config config, PeerSupplier<P, Coordinator<P>> peerSupplier) {
        this.channels = new HashMap<>();
        this.config = config;
        this.peerSupplier = peerSupplier;
        this.coordinators = config.getCoordinatorAddresses();
        this.isOrchestrator = false;
        this.peers = new HashMap<>();
        this.peerAddresses = new InetSocketAddress[config.getGraphStrategy().getNodes()];
        this.readyCount = new AtomicInteger(0);
        this.closedFuture = new CompletableFuture<>();
        this.startedFuture = new CompletableFuture<>();

        LOGGER.info("Starting coordinator");
        this.node = new Node(this, Messages.CoordinatorMessage::getDefaultInstance);
        node.bind(addr);
        this.address = addr;

        this.startWait = config.getNetworkDelay();
        this.announceTimeout = new Timeout(this::onAllPeersAnnounced, config.getNetworkDelay());
        this.bandwidth = config.getGraphStrategy().getBandWidth();

        this.cpuMeasure = new CPULoadMeasure(5000);
    }

    public CompletableFuture<Boolean> closedFuture() {
        return closedFuture;
    }
    public CompletableFuture<Void> startedFuture() {
        return startedFuture;
    }

    public ArrayList<List<Edge>> getAdjList() {
        return adjList;
    }

    public HashMap<Integer, P> getPeers() {
        return peers;
    }

    public double getBandwidth() {
        return bandwidth;
    }

    /**
     * First operation to be called on a coordinator. The callee assumes the orchestrator role for the remaining coordinators
     */
    public void startAsOrchestrator() {
        this.isOrchestrator = true;
        LOGGER.info("Assuming Orchestrator role");
        for(Config.CoordinatorEntry coord : coordinators) {
            if(!coord.address.equals(node.getListenAddr())) {
                LOGGER.info("Connecting to Coordinator at "+coord.address);
                node.connect(coord.address).syncUninterruptibly();
            }
        }
        if(coordinators.size() <= 1) {
            // if this is the only coordinator, immediately continue with starting up peer nodes
            assignNodes();
        }
    }

    /**
     * Called at the orchestrator once all coordinators were started and connected
     */
    private void assignNodes() {
        // Compute P2P network graph
        adjList = config.getGraphStrategy().getEdges();

        int totalPeers = adjList.size();
        int[] peersForCoordinator = calcPeersPerCoordinator(totalPeers, coordinators);

        // Send assignments of peerID ranges to remote peers
        int last = 0;
        for(int i = 0; i < peersForCoordinator.length; i++) {
            int start = last;
            int end = last+peersForCoordinator[i]-1;

            Channel channel = channels.get(coordinators.get(i).address);

            if(channel == null) {
                LOGGER.info("Nodes handled on this Coordinator: "+start+ " - "+end);
                initPeers(start, end);
            } else {
                Messages.AssignNodes m = Messages.AssignNodes.newBuilder().setFrom(start).setTo(end).build();
                Messages.CoordinatorMessage msg = Messages.CoordinatorMessage.newBuilder().setAssignNodes(m).build();

                LOGGER.info("Sending assignment ("+start+"-"+end+") to Coordinator "+coordinators.get(i).address);

                channel.writeAndFlush(msg);
            }

            last += peersForCoordinator[i];
        }
    }

    /**
     * Calculate the number of peers for each coordinator according to the configured computing shares
     * @param peers the number of total peers
     * @param coordinators the coordinator config list
     * @return an Array containing the number of peers to be started at each coordinator
     */
    private int[] calcPeersPerCoordinator(int peers, List<Config.CoordinatorEntry> coordinators) {
        double totalComputingShare = coordinators.stream().mapToDouble(e -> e.computingShare).sum();
        int[] peersForCoordinator = new int[coordinators.size()];
        int nonZeroCoords = (int) coordinators.stream().filter(e -> e.computingShare > 0).count();
        int peersLeft = peers;
        for(int i = 0; i < coordinators.size(); i++) {
            if(coordinators.get(i).computingShare <= 0) {
                peersForCoordinator[i] = 0;
                continue;
            }
            int maxPeers = peersLeft - (--nonZeroCoords);
            peersForCoordinator[i] = (int)Math.max(1, Math.min((peers*1.0)*coordinators.get(i).computingShare/totalComputingShare, maxPeers));
            peersLeft -= peersForCoordinator[i];
            if(peersLeft <= 0)
                break;
        }
        int i = 0;
        while(peersLeft > 0) {
            if(coordinators.get(i%coordinators.size()).computingShare <= 0) {
                i++;
                continue;
            }
            peersForCoordinator[i%coordinators.size()]++;
            peersLeft--;
            i++;
        }

        return peersForCoordinator;
    }

    /**
     * Initializes peers with IDs within the given range
     * @param from the first Peer to be initialized (inclusive)
     * @param to the last Peer to be initialized (inclusive)
     */
    private void initPeers(int from, int to) {
        // store range of peers handled on this coordinator
        peerIdBounds = new Pair<>(from, to);
        LOGGER.info("Starting peers: "+ peerIdBounds);
        for(int i = from; i <= to; i++) {
            P peer = peerSupplier.get(i, node.getListenAddr().getAddress(), this);
            peers.put(i, peer);
            LOGGER.finer("Started new peer "+i+": "+peer.getAddr());
        }
        if(isOrchestrator && coordinators.size() <= 1) {
            // if this is the only coordinator, immediately continue with connecting the started peers
            sendAdjList();
        }
    }

    // Called on remote Coordinator once a node assignment was received
    private void onNodes(ChannelHandlerContext ctx, int from, int to) {
        LOGGER.info("Called onNodes "+from+" - "+to);

        initPeers(from, to);

        // Build response to orchestrator to indicate successful start of assigned nodes
        Messages.NodesAssigned.Builder b = Messages.NodesAssigned.newBuilder();
        for(Map.Entry<Integer, P> e : peers.entrySet()) {
            Messages.Node n = Messages.Node.newBuilder().setNodeId(e.getKey()).setAddress(e.getValue().getAddr().toString()).build();
            b.addNode(n);
        }

        LOGGER.info("Sending assignment response");
        ctx.writeAndFlush(Messages.CoordinatorMessage.newBuilder().setNodesAssigned(b.build()).build());
    }


    // Called on Orchestrator once NodeAssignment response was received
    private void onAssigned(List<Messages.Node> nodes) {
        synchronized (nodesLock) {
            if(nodeList == null) {
                nodeList = new LinkedList<>();
            }

            nodeList.addAll(nodes);

            if(nodeList.size()+ peerIdBounds._2- peerIdBounds._1+1 == adjList.size()) {
                LOGGER.info("All peers initialized, sending adjLists");
                sendAdjList();
            }
        }
    }

    // All peers were initialized, send adjacency lists to coordinators to connect peers among each other
    protected void sendAdjList() {
        HashMap<Integer, String> addresses = new HashMap<>(adjList.size());
        if(nodeList != null) {
            for (Messages.Node n : nodeList) {
                addresses.put(n.getNodeId(), n.getAddress());
            }
        }
        for(Map.Entry<Integer, P> e : peers.entrySet()) {
            addresses.put(e.getKey(), e.getValue().getAddr().toString());
        }
        LOGGER.finest("Complete IP mapping: "+addresses);

        LinkedList<Messages.Node> nodes = new LinkedList<>();
        for(Map.Entry<Integer, String> addr : addresses.entrySet()) {
            nodes.add(Messages.Node.newBuilder().setNodeId(addr.getKey()).setAddress(addr.getValue()).build());
        }
        var initBuilder = Messages.InitP2POverlay.newBuilder();
        initBuilder.addAllNode(nodes);
        for(int i = 0; i < adjList.size(); i++) {
            var edgeListBuilder = Messages.Edge.newBuilder().setNode(i);
            // add all edges from that peer
            for(Edge n : adjList.get(i)) {
                edgeListBuilder.addEdge(Messages.Latency.newBuilder().setTo(n.to).setLatency(n.latency).build());
            }
            initBuilder.addAdjacency(edgeListBuilder.build());
        }
        var msg = Messages.CoordinatorMessage.newBuilder().setInitP2P(initBuilder.build()).build();

        // for each remote coordinator
        channels.values().forEach(c -> c.writeAndFlush(msg));

        connectPeers(nodes, adjList);
    }

    /**
     * Establish connections among peer nodes
     * @param nodes the list of node ids and their corresponding IP addresses
     * @param adL the adjacency list indicating how to connect peers
     */
    protected void connectPeers(List<Messages.Node> nodes, ArrayList<List<Edge>> adL) {
        LOGGER.info("Connecting peers");
        int numNodes = peerIdBounds._2- peerIdBounds._1+1;

        for(Messages.Node n : nodes) {
            peerAddresses[n.getNodeId()] = toInetSocketAddress(n.getAddress());
        }

        announceTimeout.start();

        // populate peers' latency map according to graph
        for(List<Edge> edges : adL) {
            for(Edge e : edges) {
                if(e.from>= peerIdBounds._1 && e.from<= peerIdBounds._2) {
                    peers.get(e.from).getLatencyMap().put(e.to, e.latency);
                }
                if(e.to>= peerIdBounds._1 && e.to<= peerIdBounds._2) {
                    peers.get(e.to).getLatencyMap().put(e.from, e.latency);
                }
            }
        }

        // connect peers according to graph
        for(int i = 0; i < numNodes; i++) {
            Peer peer = peers.get(peerIdBounds._1+i);
            for(Edge e : adL.get(peerIdBounds._1+i)) {
                peer.connect(peerAddresses[e.to]).syncUninterruptibly();
            }
        }

        if(adjList == null) {
            adjList = adL;
        }
    }

    /**
     * Reset the announceTimout once a new peer was started up and announced (in order to wait for all peers to be started)
     */
    public void peerAnnounced() {
        announceTimeout.restart();
    }

    // called once announceTimeout runs out
    private void onAllPeersAnnounced() {
        // ensure that all peers were connected properly to their neighbors
        var digraph = GraphUtil.toDiGraph(adjList);
        for(int i = peerIdBounds._1; i <= peerIdBounds._2; i++) {
            Peer p = peers.get(i);
            List<Edge> adjacency = digraph.get(i);
            for(Edge e : adjacency) {
                if(!p.isConnectedTo(e.to)) {
                    stop(null, getListenAddress().toString(), "Failed to connect peers according to graph.", false);
                    return;
                }
            }
        }
        onPreReady().whenComplete((success, error) -> {
            if(error != null) {
                stop(null, getListenAddress().toString(), error.getMessage(), false);
            } else if(!isOrchestrator) {
                LOGGER.info("Sending Ready");
                // Non-Orchestrator coordinators inform orchestrator that they are ready to start
                channels.values().iterator().next().writeAndFlush(Messages.CoordinatorMessage.newBuilder()
                        .setReady(Messages.P2PReady.newBuilder()).build());
            } else {
                onReady();
            }
        });
    }

    /**
     * Opportunity for subclasses to insert additional operations before declaring "Ready"
     * @return A future to be completed once the "PreReady" step is finished
     */
    protected abstract CompletableFuture<Void> onPreReady();


    // Called on Orchestrator once "Ready" was received from remote Coordinator
    private void onReady() {
        if(readyCount.incrementAndGet() == coordinators.size()) {
            // All coordinators declared "Ready"
            long startTime = System.currentTimeMillis()+startWait;
            var msg = Messages.CoordinatorMessage.newBuilder()
                    .setStart(Messages.Start.newBuilder().setTime(startTime)).build();
            // Issue "Start" command with the given startTime to other Coordinators
            for(Channel c : channels.values()) {
                c.writeAndFlush(msg);
            }
            // call start for this coordinator
            start(startTime);
        }
    }

    private void start(long startTime) {
        startedFuture.complete(null);
        executionTime = System.currentTimeMillis();
        // Operations after "Start" are delegated to subclasses
        onStart(startTime);
        cpuMeasure.startAfter((int) (Math.max(0, startTime-executionTime)+startWait));
    }

    /**
     * Called once all peers are ready and coordinators received the "Start" signal
     * @param startTime the time to start the next emulation
     */
    protected abstract void onStart(long startTime);

    /**
     * Eventually stops the execution of ALL Coordinators and their simulated Peers. Use onStop() and onPreClose() to
     * add custom procedures to be don during the stopping phase.
     * @param from the Coordinator channel the stop message was received from or null if this coordinator initiated the stop
     * @param initiator the string representation of the party initiating the stopping phase
     * @param reason the human readable reason for stopping
     */
    public void stop(Channel from, String initiator, String reason, boolean fatal) {
        if(closing)
            return;
        cpuMeasure.stop();
        var msg = Messages.CoordinatorMessage.newBuilder()
                .setStop(Messages.Stop.newBuilder().setInitiator(initiator).setReason(reason).setFatal(fatal)).build();
        // Coordinators are arranged in a star topology around the orchestrator. If this is the initiator, send "stop" to
        // all other coordinators. If this is the orchestrator, send "stop" to all other coordinators, except the initiator.
        // If this is neither the orchestrator or the initiator send no stop messages.
        for(Channel channel : channels.values()) {
            if(!channel.equals(from) && channel.isActive()) {
                channel.writeAndFlush(msg);
            }
        }
        LOGGER.severe("Coordinator received "+(fatal?"FATAL ":"")+"stop from "+initiator+": "+reason);
        // close all Peers and this Coordinator
        close(fatal);
    }

    private CompletableFuture<Void> closeAllPeers() {
        CompletableFuture[] futures = new CompletableFuture[peers.size()];
        int i = 0;
        for(Map.Entry<Integer, P> p : peers.entrySet()) {
            futures[i++] = p.getValue().close();
        }
        return CompletableFuture.allOf(futures);
    }

    public void close(boolean fatal) {
        if(closing)
            return;
        closing = true;
        LOGGER.info("Close in Coordinator "+node.getListenAddr());
        announceTimeout.cancel();
        executionTime = System.currentTimeMillis() - executionTime;
        //node.close();
        closeAllPeers().thenCompose(v -> onPreClose()).whenComplete((s, e) -> {
            LOGGER.info("Finally closing Coordinator "+node.getListenAddr());
            for(Channel c : channels.values()) {
                c.close();
            }
            node.close().whenComplete((success, error) -> {
                LOGGER.info("Coordinator closed "+node.getListenAddr());
                // Pass control flow to implementing classes after closing
                onStop();
                closedFuture.complete(fatal);
            });
        });
    }

    public boolean isClosing() {
        return closing;
    }

    public boolean isClosed() {
        return node.isClosed();
    }

    /**
     * Executed after all peers were closed but connections to other coordinators still exist.
     * @return A future which is completed once the implementing class is ready for this coordinator to be closed.
     */
    protected abstract CompletableFuture<Void> onPreClose();

    protected abstract void onStop();

    private InetSocketAddress toInetSocketAddress(String s) {
        String[] ar = s.replace("/", "").split(":");
        return new InetSocketAddress(ar[0], Integer.parseInt(ar[1]));
    }

    public InetSocketAddress getListenAddress() {
        return node.getListenAddr();
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
        LOGGER.info("Coordinator channel active: "+ctx);
        synchronized (channels) {
            channels.put((InetSocketAddress) ctx.channel().remoteAddress(), ctx.channel());
            if(isOrchestrator && channels.size() == coordinators.size()-1) {
                LOGGER.info("Connection to all coordinators established, assigning peers");
                assignNodes();
            }
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
        LOGGER.warning("Coordinator connection lost: "+ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Messages.CoordinatorMessage message) {
//        LOGGER.info("Coordinator received: "+message);
        if(message.hasAssignNodes()) {
            onNodes(ctx, message.getAssignNodes().getFrom(), message.getAssignNodes().getTo());
        } else if(message.hasNodesAssigned()) {
            onAssigned(message.getNodesAssigned().getNodeList());
        } else if(message.hasInitP2P()) {
            var edges = message.getInitP2P().getAdjacencyList();
            ArrayList<List<Edge>> adL = new ArrayList<>(edges.size());
            int from = 0;
            for(Messages.Edge e : edges) {
                LinkedList<Edge> l = new LinkedList<>();
                for(Messages.Latency lat : e.getEdgeList()) {
                    l.add(new Edge(from, lat.getTo(), lat.getLatency()));
                }
                adL.add(l);
                from++;
            }
            connectPeers(message.getInitP2P().getNodeList(), adL);
        } else if(message.hasStop()) {
            stop(ctx.channel(), message.getStop().getInitiator(), message.getStop().getReason(), message.getStop().getFatal());
        } else if(message.hasReady()) {
            onReady();
        } else if(message.hasStart()) {
            start(message.getStart().getTime());
        } else {
            onMessage(ctx, message);
        }
    }

    protected abstract void onMessage(ChannelHandlerContext ctx, Messages.CoordinatorMessage message);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        LOGGER.throwing(getClass().getName(), "exceptionCaught: "+ctx, cause);
        LOGGER.warning("Closing Coordinator connection");
        ctx.close();
    }

}

