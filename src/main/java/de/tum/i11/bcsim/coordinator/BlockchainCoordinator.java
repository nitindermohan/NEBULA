package de.tum.i11.bcsim.coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.tum.i11.bcsim.blockchain.Blockchain;
import de.tum.i11.bcsim.blockchain.LCRBlockchain;
import de.tum.i11.bcsim.graph.GraphUtil;
import de.tum.i11.bcsim.peer.BlockchainPeer;
import de.tum.i11.bcsim.peer.PeerSupplier;
import de.tum.i11.bcsim.proto.Messages;
import de.tum.i11.bcsim.config.Config;
import de.tum.i11.bcsim.util.Result;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class BlockchainCoordinator<P extends BlockchainPeer> extends Coordinator<P> {
    private static final Logger LOGGER = Logger.getLogger(BlockchainCoordinator.class.getName());

    private ConcurrentHashMap<String, Messages.Result> results;
    private CompletableFuture<Void> readyToClose;

    public BlockchainCoordinator(InetSocketAddress addr, Config config, PeerSupplier<P, Coordinator<P>> peerSupplier) {
        super(addr, config, peerSupplier);
        results = new ConcurrentHashMap<>();
        readyToClose = new CompletableFuture<>();
    }

    @Override
    protected CompletableFuture<Void> onPreReady() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected void onStart(long startTime) {
        LOGGER.info("Starting peer threads!");
        for(P p : peers.values()) {
            p.onStart(startTime);
        }
    }

    @Override
    protected CompletableFuture<Void> onPreClose() {
        // called after peers were closed but coordinators still active
        Messages.Result.Builder rb = Messages.Result.newBuilder();
        rb.setAvgCPULoad(cpuMeasure.getAvgLoad());
        rb.setMaxCPULoad(cpuMeasure.getMaxLoad());
        rb.setCoordAddress(address.toString());
        for (P p : peers.values()) {
            rb.addEntry(p.getResultEntry());
        }

        if(isOrchestrator) {
            results.put(address.toString(), rb.build());
            if (results.size() == coordinators.size()) {
                readyToClose.complete(null);
            }
        } else {
            Messages.CoordinatorMessage.Builder b = Messages.CoordinatorMessage.newBuilder();
            b.setResult(rb.build());
            for(Channel c : channels.values()) {
                c.writeAndFlush(b.build()).syncUninterruptibly();
            }
            readyToClose.complete(null);
        }
        return readyToClose;
    }

    @Override
    protected void onStop() {
        if(isOrchestrator) {


            if(getPeers().isEmpty()) {
                LOGGER.severe("No peers on orchestrator: no Blockchain results included!");
            }
            if(cpuMeasure.getAvgLoad() >= 60 || cpuMeasure.getMaxLoad() >= 80) {
                LOGGER.severe("High CPU load detected, results may be inaccurate");
            }
            Blockchain b = getPeers().isEmpty()? new LCRBlockchain(0,0,0,0,false)
                    : getPeers().values().iterator().next().getBlockchain();

            Result r = new Result(b, config, results, executionTime);
            String s;
            boolean json = config.getExportAsJson();
            try {
                s = json?r.toJsonString():r.toString();
            } catch (JsonProcessingException e) {
                s = r.toString();
                json = false;
            }
            LOGGER.info(s);

            try {
                var p = new PrintWriter(new FileOutputStream(new File(config.prefix+(json?".json":".txt"))));
                p.write(s);
                p.flush();
                p.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            if(config.renderBlockchain()) {
                try {
                    b.renderGraphiz(config.prefix+"_bc.svg");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if(config.renderGraph()) {
                try {
                    GraphUtil.renderGraphiz(config.prefix+"_graph.svg", adjList);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public CompletableFuture<Result> getResult() {
        return closedFuture().thenCompose(fatal -> {
            Blockchain b = getPeers().isEmpty()? new LCRBlockchain(0,0,0,0,false)
                    : getPeers().values().iterator().next().getBlockchain();

            CompletableFuture<Result> future = new CompletableFuture<>();
            Result r = new Result(b, config, results, executionTime);
            future.complete(r);
            return future;
        });
    }

    @Override
    protected void onMessage(ChannelHandlerContext ctx, Messages.CoordinatorMessage message) {
        if(message.hasResult()) {
            results.put(message.getResult().getCoordAddress(), message.getResult());
            if (results.size() == coordinators.size()) {
                readyToClose.complete(null);
            }
        }
    }
}
