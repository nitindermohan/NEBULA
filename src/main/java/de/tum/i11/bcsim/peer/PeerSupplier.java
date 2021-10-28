package de.tum.i11.bcsim.peer;

import de.tum.i11.bcsim.coordinator.Coordinator;

import java.net.InetAddress;

@FunctionalInterface
public interface PeerSupplier<P extends Peer, C extends Coordinator<P>> {
    P get(int id, InetAddress address, C coordinator);
}
