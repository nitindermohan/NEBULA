package de.tum.i11.bcsim.coordinator;

import de.tum.i11.bcsim.config.Config;
import de.tum.i11.bcsim.peer.ProofBasedPeer;

import java.net.InetSocketAddress;

public class ProofBasedCoordinator extends BlockchainCoordinator<ProofBasedPeer> {
    public ProofBasedCoordinator(InetSocketAddress addr, Config config) {
        super(addr, config, (id, address, coordinator) -> new ProofBasedPeer(id, address, coordinator, config));
    }
}
