package de.tum.i11.bcsim.peer;

import de.tum.i11.bcsim.config.Config;
import de.tum.i11.bcsim.coordinator.Coordinator;
import de.tum.i11.bcsim.proto.Messages;
import de.tum.i11.bcsim.task.PoissonProcess;
import de.tum.i11.bcsim.util.*;

import java.net.InetAddress;
import java.util.logging.Level;

public class ProofBasedPeer extends BlockchainPeer {

    private double miningRate;

    public ProofBasedPeer(int id, InetAddress addr, Coordinator<ProofBasedPeer> coordinator, Config config) {
        super(id, addr, coordinator, config,
                config.getBlockchainDefaults().txRate*config.getProofBasedPeerConfigs().get(id).txShare/config.getTotalTxShare(),

                config.getProofBasedPeerConfigs().get(id).verificationTime,
                config.newBlockchain(config.getProofBasedPeerConfigs().get(id).txPoolSize, config.getProofBasedStrat().confirmations),
                config.getProofBasedPeerConfigs().get(id).txSize, config.getProofBasedPeerConfigs().get(id).txFees,
                config.getProofBasedPeerConfigs().get(id).txDistribution
        );
        log(Level.FINER, "Created Peer with config: "+config.getProofBasedPeerConfigs().get(id));
        this.miningRate = config.getBlockchainDefaults().miningRate*config.getProofBasedPeerConfigs().get(id).miningShare/config.getTotalMiningShare();
    }

    @Override
    public void onStart(long startTime) {
        log(Level.FINER, "Starting mining and transaction threads");
        super.onStart(startTime);
        miningThread = new PoissonProcess(this::onBlockFound, miningRate).begin(startTime);
    }

    private void onBlockFound() {
        blocksPerSecond.registerPackets(1);
        Messages.Block.Builder b = Messages.Block.newBuilder().setTs(Util.getTimestamp()).setCreator(id).setBlockId(Util.getID(id, blocksPerSecond.getPackets()));
        bc.addNewBlock(b);
        log(Level.CONFIG, "New Block Found! ("+bc.getTotalBlockNum()+")");
        if(bc.getTotalBlockNum() >= bcDefaults.blocks) {
            coordinator.stop(null, coordinator.getListenAddress().toString(), "Reached max block num", false);
        }
        broadcast(Messages.P2PMessage.newBuilder().setBlock(b).build());
    }
}
