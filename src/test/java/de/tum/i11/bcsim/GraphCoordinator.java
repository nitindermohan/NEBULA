package de.tum.i11.bcsim;

import de.tum.i11.bcsim.coordinator.Coordinator;
import de.tum.i11.bcsim.proto.Messages;
import de.tum.i11.bcsim.config.Config;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import io.netty.channel.ChannelHandlerContext;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GraphCoordinator extends Coordinator<GraphPeer> {
    private LinkedList<String> graphiz;

    public GraphCoordinator(InetSocketAddress addr, Config config) {
        super(addr, config, GraphPeer::new);

        this.graphiz = new LinkedList<>();
    }

    @Override
    protected CompletableFuture<Void> onPreReady() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected void onStart(long startTime) {
        StringBuilder sgb = new StringBuilder("subgraph \"cluster_"+getListenAddress().toString().substring(1)+"\" {");
        StringBuilder gb = new StringBuilder();
        for(Map.Entry<Integer, GraphPeer> e : peers.entrySet()) {
            sgb.append(e.getKey()).append(";");
            for(Integer to : e.getValue().getConnections().keySet()) {
                if(to > e.getKey()) continue;
                if(to >= peerIdBounds._1 && to <= peerIdBounds._2) {
                    sgb.append(e.getKey()).append("--").append(to).append(";");
                } else {
                    gb.append(e.getKey()).append("--").append(to).append(";");
                }
            }
        }
        sgb.append("label=\"").append(getListenAddress().toString().substring(1)).append("\";}");

        graphiz.add(sgb.toString()+gb.toString());

        if(isOrchestrator && graphiz.size() == coordinators.size()) {
            stop(null, getListenAddress().toString(), "Simulation finished", false);
            System.out.println("graph G {"+graphiz.stream().reduce("", String::concat) +"}");
        } else {
            channels.values().iterator().next().writeAndFlush(Messages.CoordinatorMessage.newBuilder()
                    .setGraph(Messages.Graphviz.newBuilder().setGraph(sgb.toString()+gb.toString()).build()));
        }
    }

    @Override
    protected CompletableFuture<Void> onPreClose() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected void onStop() {

    }

    @Override
    protected void onMessage(ChannelHandlerContext ctx, Messages.CoordinatorMessage message) {
        if(message.hasGraph()) {
            graphiz.add(message.getGraph().getGraph());
            if(isOrchestrator && graphiz.size() == coordinators.size()) {
                stop(null, getListenAddress().toString(),"Simulation finished", false);
                String graph = "graph G {"+graphiz.stream().reduce("", String::concat) +"}";
                System.out.println(graph);
                try {
                    Graphviz.fromString(graph).height(1000).width(2000).render(Format.SVG).toFile(new File("out.svg"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
