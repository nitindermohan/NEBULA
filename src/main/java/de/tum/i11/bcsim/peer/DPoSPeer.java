package de.tum.i11.bcsim.peer;

import de.tum.i11.bcsim.config.Config;
import de.tum.i11.bcsim.coordinator.Coordinator;
import de.tum.i11.bcsim.proto.Messages;
import de.tum.i11.bcsim.coordinator.DPoSCoordinator;
import de.tum.i11.bcsim.task.ConstantRateTask;
import de.tum.i11.bcsim.util.*;
import io.netty.channel.ChannelHandlerContext;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class DPoSPeer extends BlockchainPeer {
    private boolean isBlockProducer;

    private int blockDuration;
    private int blocksPerRound;
    private int roundDuration;

    private Config.DPoSPeerConfig peerConfig;

    private int[] currentBlockInRound = {1};
    private int[] last = {-1};
    private int lastBlock = -1;
    private LinkedList<Integer> ids = new LinkedList<>();
    private LinkedList<Integer> used = new LinkedList<>();
    private final Map<Integer, Pair<Messages.Block, AtomicInteger>> blockAcks = Collections.synchronizedMap(new LinkedHashMap<>());

    public DPoSPeer(int id, InetAddress addr, Coordinator<? extends Peer> coordinator, Config config) {
        super(id, addr, coordinator, config,
                config.getBlockchainDefaults().txRate*config.getDPoSPeerConfigs().get(id).txShare/config.getTotalTxShare(),
                config.getDPoSPeerConfigs().get(id).verificationTime,
                config.newBlockchain(config.getDPoSPeerConfigs().get(id).txPoolSize, config.getDPoSStrat().confirmations),
                config.getDPoSPeerConfigs().get(id).txSize,
                config.getDPoSPeerConfigs().get(id).txFees,
                config.getDPoSPeerConfigs().get(id).txDistribution
        );

        this.peerConfig = config.getDPoSPeerConfigs().get(id);
        log(Level.FINER, "Created Peer with config: "+peerConfig);
        this.blocksPerRound = config.getDPoSStrat().blocksPerNode;
        this.blockDuration = (int) (1000/bcDefaults.miningRate);
        this.roundDuration = (int) (1000/bcDefaults.miningRate*config.getDPoSStrat().blocksPerNode);

        this.isBlockProducer = false;
    }

    public void setBlockProducer() {
        this.isBlockProducer = true;
    }



    private void onBlockFound() {

        if(ids.isEmpty()) {
            ids.addAll(used);
            used.clear();
            lastBlock = -1;
        }

        int id;
        if(config.getDPoSStrat().randomShuffle) {
            if (used.isEmpty() && ids.size() > 1) {
                id = Util.getIdInRound(ids, blockDuration * blocksPerRound, last[0]);
            } else {
                id = Util.getIdInRound(ids, blockDuration * blocksPerRound);
            }
        } else {
            id = ids.getFirst();
        }

//        log(Level.INFO, "Block Created by: "+id+(id==this.id?" (THIS PEER)":""));

        if(id == this.id && currentBlockInRound[0] <= blocksPerRound-config.getDPoSStrat().skipLastBlocks) {
            log(Level.CONFIG, "Block Created by: "+id+"("+bc.getTotalBlockNum()+")");
            blocksPerSecond.registerPackets(1);
            Messages.Block.Builder b = Messages.Block.newBuilder().setTs(Util.getTimestamp()).setCreator(id).setBlockId(Util.getID(id, blocksPerSecond.getPackets()));
            b = bc.prepareBlock(b);

            if(lastBlock >= 0) {
                b.setParentId(lastBlock);
            }

            Messages.Block next = b.build();
            lastBlock = next.getBlockId();

            if(((DPoSCoordinator) coordinator).getConsensusNodes().size() == 1) {
                bc.addBlock(next);
                broadcast(Messages.P2PMessage.newBuilder().setBlock(next).build());
                if (bc.getTotalBlockNum() >= bcDefaults.blocks) {
                    coordinator.stop(null, coordinator.getListenAddress().toString(), "Reached max block num", false);
                }
            } else {

                blockAcks.put(next.getBlockId(), new Pair<>(next, new AtomicInteger(1)));

                var proposal = Messages.P2PMessage.newBuilder().setProposal(Messages.BlockProposal.newBuilder().setBlock(next)).build();
                for (Integer i : ((DPoSCoordinator) coordinator).getConsensusNodes()) {
                    if (i == id) continue;
//                    log(Level.INFO, "Sending proposal to " + i);
                    sendAfterDelay(i, proposal);
                }
            }

        }

        if(++currentBlockInRound[0] > blocksPerRound) {
            currentBlockInRound[0] = 1;
            ids.removeFirstOccurrence(id);
            used.add(id);
            last[0] = id;
        }

    }

    @Override
    protected void onMessage(ChannelHandlerContext ctx, Messages.P2PMessage msg) {
        super.onMessage(ctx, msg);
        if (msg.hasProposal()) {
//            log(Level.INFO, "Received Proposal for Block "+msg.getProposal().getBlock().getBlockId()+", sending Ack");
            executor.schedule(() -> {
                ctx.writeAndFlush(Messages.P2PMessage.newBuilder().setAck(Messages.BlockAck.newBuilder().setBlockId(msg.getProposal().getBlock().getBlockId())).build());
            }, peerConfig.verificationTime, TimeUnit.MICROSECONDS);
        } else if (msg.hasAck()) {
            log(Level.FINE, "Ack received for "+msg.getAck().getBlockId());
            boolean confirmed = false;
            Pair<Messages.Block, AtomicInteger> entry;
            synchronized (blockAcks) {
                entry = blockAcks.get(msg.getAck().getBlockId());
                //            log(Level.INFO, "Received Ack for Block "+msg.getAck().getBlockId());
                if (entry != null && entry._2.incrementAndGet() + 0.1 >= (2.0 / 3.0) * ((DPoSCoordinator) coordinator).getConsensusNodes().size()) {
                    log(Level.CONFIG, "Enough Acks received adding and Broadcasting Block");
                    blockAcks.remove(msg.getAck().getBlockId());
                    confirmed = true;
                }
            }
            if(confirmed) {
                bc.addBlock(entry._1);
                broadcast(Messages.P2PMessage.newBuilder().setBlock(entry._1).build());
            }
        }
    }

    @Override
    protected void onConnect(ChannelHandlerContext ctx, int id) {

    }

    @Override
    public void onStart(long startTime) {
        log(Level.FINER, "Starting mining and transaction threads");
        if(isBlockProducer) {
            ids.addAll(((DPoSCoordinator) coordinator).getConsensusNodes());
            miningThread = new ConstantRateTask(this::onBlockFound, Util.getMillisecondsToNextRoundStartAt(Math.min(100000,roundDuration), startTime), blockDuration).begin(startTime);
        }
        super.onStart(startTime+Util.getMillisecondsToNextRoundStartAt(Math.min(100000,roundDuration), startTime));
    }
}
