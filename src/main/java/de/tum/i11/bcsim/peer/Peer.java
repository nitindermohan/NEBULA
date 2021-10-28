package de.tum.i11.bcsim.peer;

import de.tum.i11.bcsim.coordinator.Coordinator;
import de.tum.i11.bcsim.node.Node;
import de.tum.i11.bcsim.proto.Messages;
import io.netty.channel.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@ChannelHandler.Sharable
public abstract class Peer extends SimpleChannelInboundHandler<Messages.P2PMessage> {
    private static final Logger LOGGER = Logger.getLogger(Peer.class.getName());

    private final Node node; // underlying TCP node
    protected final int id; // unique id of this peer
    protected final ConcurrentHashMap<Integer, Channel> connections; // mapping remote peer ids to channels with this peer
    protected final Coordinator<? extends Peer> coordinator; // this peer's coordinator
    protected final ConcurrentHashMap<Integer, Integer> latencyMap; // mapping remote peer ids to this peer's latency (in microseconds) to them

    protected ScheduledExecutorService executor;
    private CompletableFuture<Void> closedFuture; // future completed when this peer is closed

    public Peer(int id, InetAddress addr, Coordinator<? extends Peer> coordinator) {
        this.id = id;
        this.coordinator = coordinator;
        this.latencyMap = new ConcurrentHashMap<>();
        this.connections = new ConcurrentHashMap<>();
        this.node = new Node(this, Messages.P2PMessage::getDefaultInstance);
        this.executor = Executors.newSingleThreadScheduledExecutor();
        log(Level.FINER, "Running peer");
        node.bind(addr);
        closedFuture = new CompletableFuture<>();
    }

    public CompletableFuture<Void> closedFuture() {
        return closedFuture;
    }

    public ConcurrentHashMap<Integer, Channel> getConnections() {
        return connections;
    }

    public ConcurrentHashMap<Integer, Integer> getLatencyMap() {
        return latencyMap;
    }

    public ChannelFuture connect(InetSocketAddress addr) {
        return node.connect(addr);
    }

    public void setLatency(int id, int latency) {
        if(latency < 0) {
            throw new IllegalArgumentException("Negative latency");
        }
        latencyMap.put(id, latency);
    }

    public InetSocketAddress getAddr() {
        return node.getListenAddr();
    }

    public int getId() {
        return id;
    }

    public CompletableFuture<Void> close() {
        log(Level.FINER, "Closing in super");
        connections.values().forEach(ChannelOutboundInvoker::close);
        node.close().whenComplete((s, e) -> {
            executor.shutdownNow();
            closedFuture.complete(null);
        });
        return closedFuture;
    }

    public boolean isClosed() {
        return node.isClosed() && executor.isShutdown();
    }

    public boolean isConnectedTo(int nodeId) {
        return connections.containsKey(nodeId)
                && connections.get(nodeId).isActive()
                && latencyMap.containsKey(nodeId)
                && latencyMap.get(nodeId) >= 0;
    }

    /**
     * Send a message to the peer with the given id if a channel was established
     * @param id the id of the remote peer
     * @param msg the message to be sent
     * @return a future in which the message was sent
     */
    public Future<?> sendNow(int id, Messages.P2PMessage msg) {
        if(!connections.containsKey(id)) {
            LOGGER.warning("Tried to send message from "+this.id+" to "+id+" without active channel");
            return CompletableFuture.completedFuture(null);
        }
        return connections.get(id).writeAndFlush(msg);
    }

    /**
     * Send a message to the peer with the given id after the specified latency
     * @param id the id of the remote peer
     * @param msg the message to be sent
     * @param microseconds the latency after which the message is sent
     * @return a future in which the message was sent
     */
    public ScheduledFuture<?> sendAfter(int id, Messages.P2PMessage msg, long microseconds) {
        return executor.schedule(() -> sendNow(id, msg), microseconds, TimeUnit.MICROSECONDS);
    }

    /**
     * Send a message to the peer with the given id after simulating the latency to the remote peer
     * @param id the id of the remote peer
     * @param msg the message to be sent
     * @param msgSize the simulated size of the message (might be different to actual size)
     * @return a future in which the message was sent
     */
    public ScheduledFuture<?> sendAfterLatency(int id, Messages.P2PMessage msg, int msgSize) {
        return sendAfter(id, msg, calcDelayInMicroseconds(latencyMap.get(id), msgSize, coordinator.getBandwidth()));
    }

    /**
     * Send the given message to all channels established by this peer
     * @param msg the message to be sent
     */
    public void broadcastNow(Messages.P2PMessage msg) {
        for(Channel c : connections.values()) {
            c.writeAndFlush(msg);
        }
    }

    /**
     * Send the given message to all channels excluding the given channel
     * @param msg the message to be sent
     * @param msgSize the simulated size of the message (might be different to actual size)
     * @param channel the channel to not receive the message
     */
    public void broadcastAfterLatencyExcluding(Messages.P2PMessage msg, int msgSize, Channel channel) {
        for(Map.Entry<Integer, Channel> e: connections.entrySet()) {
            Channel c = e.getValue();
            if(!c.equals(channel)) {
                executor.schedule(() -> c.writeAndFlush(msg),
                        calcDelayInMicroseconds(latencyMap.get(e.getKey()), msgSize, coordinator.getBandwidth()), TimeUnit.MICROSECONDS);
            }
        }
    }

    /**
     * Send the given message to all channels established by this peer after simulating their respective latency times
     * @param msg the message to be sent
     * @param msgSize the simulated size of the message (might be different to actual size)
     */
    public void broadcastAfterLatency(Messages.P2PMessage msg, int msgSize) {
        for(Map.Entry<Integer, Channel> e: connections.entrySet()) {
            Channel c = e.getValue();
            executor.schedule(() -> c.writeAndFlush(msg),
                    calcDelayInMicroseconds(latencyMap.get(e.getKey()), msgSize, coordinator.getBandwidth()), TimeUnit.MICROSECONDS);
        }
    }

    /**
     * Calculate the added, simulated delay before a message is sent.
     * @param latency the latency in microseconds
     * @param msgSize the message size in byte
     * @param bandwidth the bandwidth in MB/s (= byte/microsecond)
     * @return the propagation delay in microseconds
     */
    private long calcDelayInMicroseconds(int latency, int msgSize, double bandwidth) {
        return (long) (latency+msgSize/bandwidth);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Messages.P2PMessage p2pMessage)  {
//        log(Level.FINE, "Received: "+p2pMessage);
        if(p2pMessage.hasAnnounce()) {
            // once a channel was established and the opposing peer was announced, add channel to active connections
            connections.put(p2pMessage.getAnnounce().getNodeId(), ctx.channel());
            // inform coordinator that a P2P connection was established successfully
            coordinator.peerAnnounced();
            onConnect(ctx, p2pMessage.getAnnounce().getNodeId());
        } else {
            onMessage(ctx, p2pMessage);
        }
    }

    protected abstract void onMessage(ChannelHandlerContext ctx, Messages.P2PMessage msg);

    protected abstract void onConnect(ChannelHandlerContext ctx, int id);

    public abstract void onStart(long startTime);

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
        log(Level.FINER, "Peer connection established: "+ctx);
        log(Level.FINER, "Sending P2PAnnounce");
        // Peers announce themselves once a new channel is established
        Messages.P2PMessage msg = Messages.P2PMessage.newBuilder()
                .setAnnounce(Messages.AnnouncePeer.newBuilder().setNodeId(id)).build();
        ctx.writeAndFlush(msg);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
        if(!coordinator.isClosing()) {
            log(Level.WARNING, "Peer connection lost: " + ctx);
            coordinator.stop(null, "Peer#" + id, "Channel Inactive", false);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        LOGGER.throwing(getClass().getName(), "[Peer#"+id+"] exceptionCaught: "+ctx, cause);
        log(Level.WARNING, "Closing Peer connection");
        ctx.close();
    }

    protected void log(Level level, String msg) {
        LOGGER.log(level, "[Peer#"+id+"] "+msg);
    }

}
