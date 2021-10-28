package de.tum.i11.bcsim;

import de.tum.i11.bcsim.coordinator.Coordinator;
import de.tum.i11.bcsim.peer.Peer;
import de.tum.i11.bcsim.proto.Messages;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetAddress;

public class GraphPeer extends Peer {

    public GraphPeer(int id, InetAddress addr, Coordinator<GraphPeer> coordinator) {
        super(id, addr, coordinator);
    }

    @Override
    protected void onMessage(ChannelHandlerContext ctx, Messages.P2PMessage msg) {

    }

    @Override
    protected void onConnect(ChannelHandlerContext ctx, int id) {

    }

    @Override
    public void onStart(long startTime) {

    }
}
