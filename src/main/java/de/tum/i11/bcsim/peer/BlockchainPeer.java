package de.tum.i11.bcsim.peer;

import com.google.protobuf.ByteString;
import de.tum.i11.bcsim.blockchain.Blockchain;
import de.tum.i11.bcsim.config.Config;
import de.tum.i11.bcsim.config.ConfigYAML;
import de.tum.i11.bcsim.coordinator.Coordinator;
import de.tum.i11.bcsim.proto.Messages;
import de.tum.i11.bcsim.task.ConstantRateTask;
import de.tum.i11.bcsim.task.PoissonProcess;
import de.tum.i11.bcsim.task.RepeatingTask;
import de.tum.i11.bcsim.task.UniformProcess;
import de.tum.i11.bcsim.util.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public abstract class BlockchainPeer extends Peer {

    protected RepeatingTask miningThread;
    protected RepeatingTask txThread;
    protected final double txRate;
    protected final ConfigYAML.BlockchainDefaults bcDefaults;
    protected final Config config;
    protected final int verificationTime;
    protected final Blockchain bc;
    protected final boolean pushBlocks;

    protected final Timeout endTimeout;
    protected final ConcurrentLinkedQueue<Pair<Messages.Transaction, Integer>> confirmationTimes;
    protected final byte[] txData;
    protected final List<ConfigYAML.TxFee> txFees;
    protected final String txDistribution;

    protected final ThroughputMeasure blocksPerSecond = new ThroughputMeasure();
    protected final ThroughputMeasure txPerSecond = new ThroughputMeasure();

    public BlockchainPeer(int id, InetAddress addr, Coordinator<? extends Peer> coordinator, Config config,
                          double txRate, int verificationTime, Blockchain blockchain, int txSize, List<ConfigYAML.TxFee> txFees, String txDistribution) {
        super(id, addr, coordinator);

        this.txRate = txRate;
        this.bcDefaults = config.getBlockchainDefaults();
        this.config = config;
        this.verificationTime = verificationTime;
        this.bc = blockchain;
        this.txData = Util.rndBytes(txSize);
        this.txFees = txFees;
        this.txDistribution = txDistribution;
        this.pushBlocks = config.getBlockchainDefaults().pushBlocks;

        this.endTimeout = new Timeout(super::close, config.getNetworkDelay());
        this.confirmationTimes = new ConcurrentLinkedQueue<>();
    }

    public Messages.ResultEntry getResultEntry() {
        var res = Messages.ResultEntry.newBuilder()
                .setNodeId(id)
                .setPoolSize(bc.getTxPool().inPoolSize())
                .setOrphans(bc.getOrphans().size())
                .setCreatedBlocksPerSec(blocksPerSecond.getThroughput())
                .setCreatedBlocks(blocksPerSecond.getPackets())
                .setCreatedTxPerSec(txPerSecond.getThroughput())
                .setCreatedTx(txPerSecond.getPackets())
                .setConfirmedBlocksPerSec(bc.getConfirmedBlockThroughput().getThroughput())
                .setConfirmedTxPerSec(bc.getConfirmedTxThroughput().getThroughput())
                .setConfirmedBytesPerSec(bc.getConfirmedByteThroughput().getThroughput())
                .setTotalBlocksPerSec(bc.getTotalBlockThroughput().getThroughput())
                .setTotalTxPerSec(bc.getTotalTxThroughput().getThroughput())
                .setUnconfirmedTx(bc.getNumberOfUnconfirmedTx());
        for(Pair<Messages.Transaction, Integer> txLat : confirmationTimes) {
            res.addTxLatency(Messages.TxLatencyResult.newBuilder()
                    .setFee(txLat._1.getTxFee())
                    .setSize(txLat._1.getData().size())
                    .setLatency(txLat._2));
        }
        return res.build();
    }

    private void propagateBlock(ChannelHandlerContext ctx, Messages.P2PMessage msg) {
        if(pushBlocks) {
            broadcastExcluding(msg, ctx.channel());
        } else {
            broadcastExcluding(Messages.P2PMessage.newBuilder().setInv(
                    Messages.Inventory.newBuilder().setId(msg.getBlock().getBlockId()).setSender(id)).build()
                    , ctx.channel());
        }
    }

    protected void broadcastExcluding(Messages.P2PMessage msg, Channel channel) {
        broadcastAfterLatencyExcluding(msg, calcMsgSize(msg), channel);
    }

    protected void broadcast(Messages.P2PMessage msg) {
        broadcastAfterLatency(msg, calcMsgSize(msg));
    }

    protected void sendAfterDelay(int id, Messages.P2PMessage msg) {
        sendAfterLatency(id, msg, calcMsgSize(msg));
    }

    private int calcMsgSize(Messages.P2PMessage msg) {
        if(msg.hasBlock() && config.simulateFullBlocks()) {
            int realBlockSize = msg.getBlock().getSerializedSize();
            int simulatedBlockSize = Math.max(realBlockSize, config.getBlockchainDefaults().blockSize * config.getBlockchainDefaults().txSize);
            return msg.getSerializedSize() - realBlockSize + simulatedBlockSize;
        } else {
            return msg.getSerializedSize();
        }
    }

    @Override
    protected void onMessage(ChannelHandlerContext ctx, Messages.P2PMessage msg) {
        endTimeout.restart();
        if(msg.hasBlock()) {
            log(Level.FINER, "Received Block!");
            if(verificationTime > 0) {
                executor.schedule(() -> {
                    if (bc.addBlock(msg.getBlock())) {
                        propagateBlock(ctx, msg);
                        if (bc.getTotalBlockNum() >= bcDefaults.blocks) {
                            coordinator.stop(null, coordinator.getListenAddress().toString(), "Reached max block num", false);
                        }
                    }
                }, verificationTime, TimeUnit.MICROSECONDS);
            } else if (bc.addBlock(msg.getBlock())) {
                propagateBlock(ctx, msg);
                if (bc.getTotalBlockNum() >= bcDefaults.blocks) {
                    coordinator.stop(null, coordinator.getListenAddress().toString(), "Reached max block num", false);
                }
            }
        } else if (msg.hasTransaction()) {
            log(Level.FINEST, "Received Transaction!");
            if(bc.addTransaction(msg.getTransaction())) {
                broadcastExcluding(msg, ctx.channel());
            }
        } else if (msg.hasInv() && !bc.containsBlock(msg.getInv().getId())) {
            sendAfterDelay(msg.getInv().getSender(), Messages.P2PMessage.newBuilder().setGetData(
                    Messages.GetData.newBuilder().setId(msg.getInv().getId()).setSender(id)).build());
        } else if (msg.hasGetData() && bc.containsBlock(msg.getGetData().getId())) {
            sendAfterDelay(msg.getGetData().getSender(), Messages.P2PMessage.newBuilder().setBlock(
                    bc.getBlock(msg.getGetData().getId()).block).build());
        }
    }

    @Override
    protected void onConnect(ChannelHandlerContext ctx, int id) {

    }

    @Override
    public void onStart(long startTime) {
        log(Level.FINER, "Generating transactions with distribution: "+txDistribution);
        if (txRate > 0) {
            switch (txDistribution) {
                case "poisson":
                    txThread = new PoissonProcess(this::onTxCreated, txRate, Thread.NORM_PRIORITY).begin(startTime);
                    break;
                case "uniform":
                    txThread = new UniformProcess(this::onTxCreated, (int) (1000 / txRate), Thread.NORM_PRIORITY).begin(startTime);
                    break;
                case "constant":
                    txThread = new ConstantRateTask(this::onTxCreated, (int) (1000 / txRate), (int) (1000 / txRate), Thread.NORM_PRIORITY).begin(startTime);
                    break;
                default:
                    log(Level.WARNING, "Unknown txDistribution, choosing Poisson");
                    txThread = new PoissonProcess(this::onTxCreated, txRate, Thread.NORM_PRIORITY).begin(startTime);
            }
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        if(closedFuture().isDone()) {
            return closedFuture();
        }
        log(Level.FINER, "Closing in subclass");
        if(miningThread != null) {
            miningThread.end();
        }
        if(txThread != null) {
            txThread.end();
        }
        endTimeout.start();
        return closedFuture();
    }

    private void onTxCreated() {
        txPerSecond.registerPackets(1);
//        log(Level.INFO, "New Transaction Created! In Pool: "+bc.getTxPool().inPoolSize());
        Messages.Transaction t = Messages.Transaction.newBuilder().setTxId(Util.getID(id, txPerSecond.getPackets())).setTs(Util.getTimestamp())
                .setData(ByteString.copyFrom(txData)).setTxFee(Util.getFee(txFees)).build();
        boolean added = bc.addTransaction(t, tx -> {
            long now = System.currentTimeMillis();
            long created = tx.getTs().getSeconds()*1000+tx.getTs().getNanos() / 1000000;
            confirmationTimes.add(new Pair<>(tx, (int)(now-created)));
        });
        if(!added) {
            log(Level.WARNING, "TxPool full! In Pool: "+bc.getTxPool().inPoolSize());
        } else {
            broadcast(Messages.P2PMessage.newBuilder().setTransaction(t).build());
        }
    }

    public Blockchain getBlockchain() {
        return bc;
    }
}
