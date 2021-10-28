package de.tum.i11.bcsim;

import de.tum.i11.bcsim.blockchain.Blockchain;
import de.tum.i11.bcsim.coordinator.DPoSCoordinator;
import de.tum.i11.bcsim.peer.BlockchainPeer;
import de.tum.i11.bcsim.peer.DPoSPeer;
import de.tum.i11.bcsim.coordinator.ProofBasedCoordinator;
import de.tum.i11.bcsim.peer.ProofBasedPeer;
import de.tum.i11.bcsim.config.Config;
import de.tum.i11.bcsim.util.LogSetup;
import de.tum.i11.bcsim.util.Result;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.jupiter.api.Assertions.*;

public class TestSim {
    private static final Logger LOGGER = Logger.getLogger(TestSim.class.getName());


    @BeforeAll
    static void initAll() {
        LogSetup.setupLogging(null, Level.INFO);
    }

    @BeforeEach
    void init(TestInfo testInfo) {
    }

    @Test
    void testProofBasedSim() throws IOException {
        Config c = new Config("src/test/java/de/tum/i11/bcsim/config.yaml");
        InetSocketAddress[] ads = new InetSocketAddress[] {
                new InetSocketAddress("127.0.0.1", 5151),
                new InetSocketAddress("127.0.0.1", 5152),
                new InetSocketAddress("127.0.0.1", 5153)
        };
        ProofBasedCoordinator[] cs = new ProofBasedCoordinator[]{
                new ProofBasedCoordinator(ads[0], c),
                new ProofBasedCoordinator(ads[1], c),
                new ProofBasedCoordinator(ads[2], c)
        };
        CompletableFuture[] cfs = new CompletableFuture[] {
                cs[0].closedFuture(),
                cs[1].closedFuture(),
                cs[2].closedFuture()
        };

        cs[0].startAsOrchestrator();

        CompletableFuture.allOf(cfs).join();

        Blockchain bc = cs[0].getPeers().values().stream().map(BlockchainPeer::getBlockchain).max(Comparator.comparingInt(Blockchain::getTotalBlockNum)).get();
        assertResult(bc, cs[0].getResult().join(), c, false);

        for(ProofBasedCoordinator coord : cs) {
            for(ProofBasedPeer peer : coord.getPeers().values()) {
                assertTrue(TestBlockchain.equals(bc, peer.getBlockchain()));
                assertTrue(TestBlockchain.isValidBlockchain(peer.getBlockchain()));
                assertDoesNotThrow(peer::getResultEntry);
                assertTrue(peer.isClosed());
                assertTrue(peer.close().isDone());
            }
            assertTrue(coord.isClosed());
        }
    }

    @Test
    void testProofBasedSimGHOST() throws IOException {
        Config c = new Config("src/test/java/de/tum/i11/bcsim/config.yaml");
        c.getBlockchainDefaults().miningRate = 10;
        c.getBlockchainDefaults().tieResolution = "random";
        c.getBlockchainDefaults().forkResolution = "GHOST";
        c.getBlockchainDefaults().txRate = 10;
        c.getBlockchainDefaults().blocks = 400;
        c.getConfigYAML().random.latency = 63000;
        c.getConfigYAML().random.perEdge = false;
        c.getConfigYAML().random.nodes = 30;
        c.getConfigYAML().renderBlockchain = true;
        c.getConfigYAML().exportAsJson = false;
        c.setGraphStrategy();
        c.createProofBasedPeerConfigs();

        InetSocketAddress[] ads = new InetSocketAddress[] {
                new InetSocketAddress("127.0.0.1", 5151),
                new InetSocketAddress("127.0.0.1", 5152),
                new InetSocketAddress("127.0.0.1", 5153)
        };
        ProofBasedCoordinator[] cs = new ProofBasedCoordinator[]{
                new ProofBasedCoordinator(ads[0], c),
                new ProofBasedCoordinator(ads[1], c),
                new ProofBasedCoordinator(ads[2], c)
        };
        CompletableFuture[] cfs = new CompletableFuture[] {
                cs[0].closedFuture(),
                cs[1].closedFuture(),
                cs[2].closedFuture()
        };

        cs[0].startAsOrchestrator();

        CompletableFuture.allOf(cfs).join();

        Blockchain bc = cs[0].getPeers().values().stream().map(BlockchainPeer::getBlockchain).max(Comparator.comparingInt(Blockchain::getTotalBlockNum)).get();
        assertResult(bc, cs[0].getResult().join(), c, false);

        for(ProofBasedCoordinator coord : cs) {
            for(ProofBasedPeer peer : coord.getPeers().values()) {
                assertTrue(TestBlockchain.equals(bc, peer.getBlockchain()));
                assertTrue(TestBlockchain.isValidBlockchain(peer.getBlockchain()));
                assertDoesNotThrow(peer::getResultEntry);
                assertTrue(peer.isClosed());
                assertTrue(peer.close().isDone());
            }
            assertTrue(coord.isClosed());
        }
    }

    @Test
    void testProofBasedSimLCR() throws IOException {
        Config c = new Config("src/test/java/de/tum/i11/bcsim/config.yaml");
        c.getBlockchainDefaults().miningRate = 10;
        c.getBlockchainDefaults().tieResolution = "random";
        c.getBlockchainDefaults().forkResolution = "LCR";
        c.getBlockchainDefaults().txRate = 10;
        c.getBlockchainDefaults().blocks = 400;
        c.getConfigYAML().random.latency = 63000;
        c.getConfigYAML().random.perEdge = false;
        c.getConfigYAML().random.nodes = 30;
        c.getConfigYAML().renderBlockchain = true;
        c.getConfigYAML().exportAsJson = false;
        c.setGraphStrategy();
        c.createProofBasedPeerConfigs();

        InetSocketAddress[] ads = new InetSocketAddress[] {
                new InetSocketAddress("127.0.0.1", 5151),
                new InetSocketAddress("127.0.0.1", 5152),
                new InetSocketAddress("127.0.0.1", 5153)
        };
        ProofBasedCoordinator[] cs = new ProofBasedCoordinator[]{
                new ProofBasedCoordinator(ads[0], c),
                new ProofBasedCoordinator(ads[1], c),
                new ProofBasedCoordinator(ads[2], c)
        };
        CompletableFuture[] cfs = new CompletableFuture[] {
                cs[0].closedFuture(),
                cs[1].closedFuture(),
                cs[2].closedFuture()
        };

        cs[0].startAsOrchestrator();

        CompletableFuture.allOf(cfs).join();

        Blockchain bc = cs[0].getPeers().values().stream().map(BlockchainPeer::getBlockchain).max(Comparator.comparingInt(Blockchain::getTotalBlockNum)).get();
        assertResult(bc, cs[0].getResult().join(), c, false);

        for(ProofBasedCoordinator coord : cs) {
            for(ProofBasedPeer peer : coord.getPeers().values()) {
                assertTrue(TestBlockchain.equals(bc, peer.getBlockchain()));
                assertTrue(TestBlockchain.isValidBlockchain(peer.getBlockchain()));
                assertDoesNotThrow(peer::getResultEntry);
                assertTrue(peer.isClosed());
                assertTrue(peer.close().isDone());
            }
            assertTrue(coord.isClosed());
        }
    }

    @Test
    void testDPoSSim() throws IOException {
        Config c = new Config("src/test/java/de/tum/i11/bcsim/config.yaml");

        InetSocketAddress[] ads = new InetSocketAddress[] {
                new InetSocketAddress("127.0.0.1", 5151),
                new InetSocketAddress("127.0.0.1", 5152),
                new InetSocketAddress("127.0.0.1", 5153)
        };
        DPoSCoordinator[] cs = new DPoSCoordinator[]{
                new DPoSCoordinator(ads[0], c),
                new DPoSCoordinator(ads[1], c),
                new DPoSCoordinator(ads[2], c)
        };
        CompletableFuture[] cfs = new CompletableFuture[] {
                cs[0].closedFuture(),
                cs[1].closedFuture(),
                cs[2].closedFuture()
        };

        cs[0].startAsOrchestrator();

        CompletableFuture.allOf(cfs).join();

        Blockchain bc = cs[0].getPeers().values().stream().map(BlockchainPeer::getBlockchain).max(Comparator.comparingInt(Blockchain::getTotalBlockNum)).get();
        assertResult(bc, cs[0].getResult().join(), c, true);

        for(DPoSCoordinator coord : cs) {
            for(DPoSPeer peer : coord.getPeers().values()) {
                assertTrue(TestBlockchain.equals(bc, peer.getBlockchain()));
                assertTrue(TestBlockchain.isValidBlockchain(peer.getBlockchain()));
                assertDoesNotThrow(peer::getResultEntry);
                assertTrue(peer.isClosed());
                assertTrue(peer.close().isDone());
            }
            assertTrue(coord.isClosed());
        }
    }

    @Test
    void testDPoSSimOneCoordinator() throws IOException {
        Config c = new Config("src/test/java/de/tum/i11/bcsim/config.yaml");

        InetSocketAddress[] ads = new InetSocketAddress[] {
                new InetSocketAddress("127.0.0.1", 5151)
        };
        c.setCoordinatorAddresses(List.of(ads[0]));
        c.getDPoSStrat().consensusNodeNum = 1;
        c.getDPoSStrat().consensusNodes = null;
        c.getDPoSStrat().nodeSelection = "best";
        c.getConfigYAML().networkType = "scaleFree";
        c.setGraphStrategy();
        c.createDPoSPeerConfigs();
        c.validate();

        DPoSCoordinator[] cs = new DPoSCoordinator[]{
                new DPoSCoordinator(ads[0], c)
        };
        CompletableFuture[] cfs = new CompletableFuture[] {
                cs[0].closedFuture()
        };

        cs[0].startAsOrchestrator();

        CompletableFuture.allOf(cfs).join();

        Blockchain bc = cs[0].getPeers().values().stream().map(BlockchainPeer::getBlockchain).max(Comparator.comparingInt(Blockchain::getTotalBlockNum)).get();
        assertResult(bc, cs[0].getResult().join(), c, true);

        for(DPoSCoordinator coord : cs) {
            for(DPoSPeer peer : coord.getPeers().values()) {
                assertTrue(TestBlockchain.equals(bc, peer.getBlockchain()));
                assertTrue(TestBlockchain.isValidBlockchain(peer.getBlockchain()));
                assertDoesNotThrow(peer::getResultEntry);
                assertTrue(peer.isClosed());
                assertTrue(peer.close().isDone());
            }
            assertTrue(coord.isClosed());
        }
    }

    @Test
    void testDPoSSimAutoPickConsensus() throws IOException {
        Config c = new Config("src/test/java/de/tum/i11/bcsim/config.yaml");
        c.getDPoSStrat().consensusNodes = null;
        c.getDPoSStrat().nodeSelection = "worst";
        c.getDPoSStrat().randomShuffle = false;
        c.getConfigYAML().exportAsJson = false;
        c.getConfigYAML().renderBlockchain = true;
        c.getConfigYAML().renderGraph = true;
        c.getDPoSStrat().skipLastBlocks = 1;
        c.getConfigYAML().networkType = "scaleFree";
        c.getConfigYAML().scaleFree.perEdge = false;
        c.setGraphStrategy();
        c.createDPoSPeerConfigs();
        c.validate();

        InetSocketAddress[] ads = new InetSocketAddress[] {
                new InetSocketAddress("127.0.0.1", 5151),
                new InetSocketAddress("127.0.0.1", 5152),
                new InetSocketAddress("127.0.0.1", 5153)
        };
        DPoSCoordinator[] cs = new DPoSCoordinator[]{
                new DPoSCoordinator(ads[0], c),
                new DPoSCoordinator(ads[1], c),
                new DPoSCoordinator(ads[2], c)
        };
        CompletableFuture[] cfs = new CompletableFuture[] {
                cs[0].closedFuture(),
                cs[1].closedFuture(),
                cs[2].closedFuture()
        };

        cs[0].startAsOrchestrator();

        CompletableFuture.allOf(cfs).join();

        Blockchain bc = cs[0].getPeers().values().stream().map(BlockchainPeer::getBlockchain).max(Comparator.comparingInt(Blockchain::getTotalBlockNum)).get();
        assertResult(bc, cs[0].getResult().join(), c, true);

        for(DPoSCoordinator coord : cs) {
            for(DPoSPeer peer : coord.getPeers().values()) {
                assertTrue(TestBlockchain.equals(bc, peer.getBlockchain()));
                assertTrue(TestBlockchain.isValidBlockchain(peer.getBlockchain()));
                assertDoesNotThrow(peer::getResultEntry);
                assertTrue(peer.isClosed());
                assertTrue(peer.close().isDone());
            }
            assertTrue(coord.isClosed());
        }
    }

    @Test
    void testDPoSSimRandomShuffleNotOnOrchestrator() throws IOException {
        Config c = new Config("src/test/java/de/tum/i11/bcsim/config.yaml");
        c.getDPoSStrat().consensusNodes = null;
        c.getDPoSStrat().nodeSelection = "random";
        c.getDPoSStrat().randomShuffle = true;
        c.getDPoSStrat().consensusOnOrchestrator = false;
        c.getConfigYAML().blockchainDefaults.txDistribution = "poisson";
        c.createDPoSPeerConfigs();
        c.validate();

        InetSocketAddress[] ads = new InetSocketAddress[] {
                new InetSocketAddress("127.0.0.1", 5151),
                new InetSocketAddress("127.0.0.1", 5152),
                new InetSocketAddress("127.0.0.1", 5153)
        };
        DPoSCoordinator[] cs = new DPoSCoordinator[]{
                new DPoSCoordinator(ads[0], c),
                new DPoSCoordinator(ads[1], c),
                new DPoSCoordinator(ads[2], c)
        };
        CompletableFuture[] cfs = new CompletableFuture[] {
                cs[0].closedFuture(),
                cs[1].closedFuture(),
                cs[2].closedFuture()
        };

        cs[0].startAsOrchestrator();

        CompletableFuture.allOf(cfs).join();

        Blockchain bc = cs[0].getPeers().values().stream().map(BlockchainPeer::getBlockchain).max(Comparator.comparingInt(Blockchain::getTotalBlockNum)).get();
        assertResult(bc, cs[0].getResult().join(), c, true);

        for(DPoSCoordinator coord : cs) {
            for(DPoSPeer peer : coord.getPeers().values()) {
                assertTrue(TestBlockchain.equals(bc, peer.getBlockchain()));
                assertTrue(TestBlockchain.isValidBlockchain(peer.getBlockchain()));
                assertDoesNotThrow(peer::getResultEntry);
                assertTrue(peer.isClosed());
                assertTrue(peer.close().isDone());
            }
            assertTrue(coord.isClosed());
        }
    }

    @Test
    void testProofBasedInvGetData() throws IOException {
        Config c = new Config("src/test/java/de/tum/i11/bcsim/config.yaml");
        c.getBlockchainDefaults().pushBlocks = false;
        c.getConfigYAML().blockchainDefaults.txDistribution = "uniform";
        c.getConfigYAML().proofBased.miningDistribution.type = "uniform";
        c.getConfigYAML().random.perEdge = false;
        c.setGraphStrategy();
        c.createProofBasedPeerConfigs();
        c.validate();

        InetSocketAddress[] ads = new InetSocketAddress[] {
                new InetSocketAddress("127.0.0.1", 5151),
                new InetSocketAddress("127.0.0.1", 5152),
                new InetSocketAddress("127.0.0.1", 5153)
        };
        ProofBasedCoordinator[] cs = new ProofBasedCoordinator[]{
                new ProofBasedCoordinator(ads[0], c),
                new ProofBasedCoordinator(ads[1], c),
                new ProofBasedCoordinator(ads[2], c)
        };
        CompletableFuture[] cfs = new CompletableFuture[] {
                cs[0].closedFuture(),
                cs[1].closedFuture(),
                cs[2].closedFuture()
        };

        cs[0].startAsOrchestrator();

        CompletableFuture.allOf(cfs).join();

        Blockchain bc = cs[0].getPeers().values().stream().map(BlockchainPeer::getBlockchain).max(Comparator.comparingInt(Blockchain::getTotalBlockNum)).get();
        assertResult(bc, cs[0].getResult().join(), c, false);

        for(ProofBasedCoordinator coord : cs) {
            for(ProofBasedPeer peer : coord.getPeers().values()) {
                assertTrue(TestBlockchain.equals(bc, peer.getBlockchain()));
                assertTrue(TestBlockchain.isValidBlockchain(peer.getBlockchain()));
                assertDoesNotThrow(peer::getResultEntry);
                assertTrue(peer.isClosed());
                assertTrue(peer.close().isDone());
            }
            assertTrue(coord.isClosed());
        }
    }

    @Test
    void testProofBasedSingleCoord() throws IOException {
        Config c = new Config("src/test/java/de/tum/i11/bcsim/config.yaml");
        c.getConfigYAML().blockchainDefaults.txDistribution = "constant";
        c.getConfigYAML().proofBased.miningDistribution.type = "exponential";
        c.getConfigYAML().blockchainDefaults.verificationTime = 0;
        c.getConfigYAML().blockchainDefaults.simulateFullBlocks = true;
        c.createProofBasedPeerConfigs();
        c.validate();

        InetSocketAddress[] ads = new InetSocketAddress[] {
                new InetSocketAddress("127.0.0.1", 5151)
        };
        c.setCoordinatorAddresses(List.of(ads[0]));
        ProofBasedCoordinator[] cs = new ProofBasedCoordinator[]{
                new ProofBasedCoordinator(ads[0], c)
        };
        CompletableFuture[] cfs = new CompletableFuture[] {
                cs[0].closedFuture()
        };

        cs[0].startAsOrchestrator();

        CompletableFuture.allOf(cfs).join();

        Blockchain bc = cs[0].getPeers().values().stream().map(BlockchainPeer::getBlockchain).max(Comparator.comparingInt(Blockchain::getTotalBlockNum)).get();
        assertResult(bc, cs[0].getResult().join(), c, false);

        for(ProofBasedCoordinator coord : cs) {
            for(ProofBasedPeer peer : coord.getPeers().values()) {
                assertTrue(TestBlockchain.equals(bc, peer.getBlockchain()));
                assertTrue(TestBlockchain.isValidBlockchain(peer.getBlockchain()));
                assertDoesNotThrow(peer::getResultEntry);
                assertTrue(peer.isClosed());
                assertTrue(peer.close().isDone());
            }
            assertTrue(coord.isClosed());
        }
    }

    @Test
    void testProofBasedSingleCoordExplicit() throws IOException {
        Config c = new Config("src/test/java/de/tum/i11/bcsim/config.yaml");
        c.getConfigYAML().blockchainDefaults.txDistribution = "constant";
        c.getConfigYAML().proofBased.miningDistribution.type = "exponential";
        c.getConfigYAML().blockchainDefaults.verificationTime = 0;
        c.getConfigYAML().blockchainDefaults.simulateFullBlocks = true;
        c.getConfigYAML().networkType = "explicit";
        c.setGraphStrategy();
        c.createProofBasedPeerConfigs();
        c.validate();

        InetSocketAddress[] ads = new InetSocketAddress[] {
                new InetSocketAddress("127.0.0.1", 5151)
        };
        c.setCoordinatorAddresses(List.of(ads[0]));
        ProofBasedCoordinator[] cs = new ProofBasedCoordinator[]{
                new ProofBasedCoordinator(ads[0], c)
        };
        CompletableFuture[] cfs = new CompletableFuture[] {
                cs[0].closedFuture()
        };

        cs[0].startAsOrchestrator();

        CompletableFuture.allOf(cfs).join();

        Blockchain bc = cs[0].getPeers().values().stream().map(BlockchainPeer::getBlockchain).max(Comparator.comparingInt(Blockchain::getTotalBlockNum)).get();
        assertResult(bc, cs[0].getResult().join(), c, false);

        for(ProofBasedCoordinator coord : cs) {
            for(ProofBasedPeer peer : coord.getPeers().values()) {
                assertTrue(TestBlockchain.equals(bc, peer.getBlockchain()));
                assertTrue(TestBlockchain.isValidBlockchain(peer.getBlockchain()));
                assertDoesNotThrow(peer::getResultEntry);
                assertTrue(peer.isClosed());
                assertTrue(peer.close().isDone());
            }
            assertTrue(coord.isClosed());
        }
    }

    public void assertResult(Blockchain bc, Result r, Config c, boolean dpos) {
        assertTrue(bc.getTotalBlockNum() >= c.getBlockchainDefaults().blocks);
        assertWithin(r.avgTotalBlocksPerPeerPerSec, c.getBlockchainDefaults().miningRate, 1.5+(dpos?c.getDPoSStrat().skipLastBlocks:0));
        assertWithin(r.avgTotalTxPerPeerPerSec, c.getBlockchainDefaults().txRate, 10);
    }

    public void assertWithin(double a, double b, double e) {
        assertThat(a, closeTo(b, e));
    }

    @AfterEach
    void tearDown() {

    }

    @AfterAll
    static void tearDownAll() throws IOException {
        Files.deleteIfExists(Path.of("null.txt"));
        Files.deleteIfExists(Path.of("null.json"));
        Files.deleteIfExists(Path.of("null_bc.svg"));
        Files.deleteIfExists(Path.of("null_graph.svg"));
    }
}