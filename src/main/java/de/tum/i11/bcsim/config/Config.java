package de.tum.i11.bcsim.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.tum.i11.bcsim.blockchain.Blockchain;
import de.tum.i11.bcsim.blockchain.GHOSTBlockchain;
import de.tum.i11.bcsim.blockchain.LCRBlockchain;
import de.tum.i11.bcsim.graph.*;
import de.tum.i11.bcsim.util.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class Config {

    public String prefix;

    private static Supplier<Double> parseDistribution(ConfigYAML.Distribution distribution) {
        switch (distribution.type) {
            case "uniform": return Util::nextUniform;
            case "exponential": return () -> Util.nextExponential(distribution.mean);
            default: return () -> distribution.mean;
        }
    }

    public static class ProofBasedPeerConfig {
        public int id;
        public double miningShare;
        public double txShare;
        public int txSize;
        public List<ConfigYAML.TxFee> txFees;
        public String txDistribution;
        public int txPoolSize;
        public int verificationTime;

        ProofBasedPeerConfig(int id, ConfigYAML.Distribution miningDistribution, ConfigYAML.Distribution txDistribution, ConfigYAML.ProofBased.Peer peer, ConfigYAML.BlockchainDefaults defaults) {
            this.id = id;
            if(peer == null) {
                peer = new ConfigYAML.ProofBased.Peer();
            }

            Supplier<Double> miningDistr = parseDistribution(miningDistribution);
            Supplier<Double> txDistr = parseDistribution(txDistribution);

            this.miningShare = peer.miningShare<0? miningDistr.get():peer.miningShare;
            this.txShare = peer.txShare<0? txDistr.get():peer.txShare;
            this.txSize = peer.txSize<0? defaults.txSize:peer.txSize;
            this.txFees = peer.txFees==null||peer.txFees.isEmpty()?defaults.txFees:peer.txFees;
            this.txDistribution = peer.txDistribution==null?defaults.txDistribution:peer.txDistribution;
            this.txPoolSize = peer.txPoolSize<0? defaults.txPoolSize:peer.txPoolSize;
            this.verificationTime = peer.verificationTime<0? defaults.verificationTime:peer.verificationTime;
        }

        @Override
        public String toString() {
            return "ProofBasedPeerConfig{" +
                    "id=" + id +
                    ", miningShare=" + miningShare +
                    ", txShare=" + txShare +
                    ", txSize=" + txSize +
                    ", txFees=" + txFees +
                    ", txDistribution='" + txDistribution + '\'' +
                    ", txPoolSize=" + txPoolSize +
                    ", verificationTime=" + verificationTime +
                    '}';
        }
    }

    public static class DPoSPeerConfig {
        public int id;
        public double txShare;
        public int txSize;
        public List<ConfigYAML.TxFee> txFees;
        public String txDistribution;
        public int txPoolSize;
        public int verificationTime;

        DPoSPeerConfig(int id, ConfigYAML.Distribution txDistribution, ConfigYAML.DPosBFT.Peer peer, ConfigYAML.BlockchainDefaults defaults) {
            this.id = id;
            if(peer == null) {
                peer = new ConfigYAML.DPosBFT.Peer();
            }
            Supplier<Double> txDistr = parseDistribution(txDistribution);

            this.txShare = peer.txShare<0? txDistr.get():peer.txShare;
            this.txSize = peer.txSize<0? defaults.txSize:peer.txSize;
            this.txFees = peer.txFees==null||peer.txFees.isEmpty()?defaults.txFees:peer.txFees;
            this.txDistribution = peer.txDistribution==null?defaults.txDistribution:peer.txDistribution;
            this.txPoolSize = peer.txPoolSize<0? defaults.txPoolSize:peer.txPoolSize;
            this.verificationTime = peer.verificationTime<0? defaults.verificationTime:peer.verificationTime;
        }

        @Override
        public String toString() {
            return "DPoSPeerConfig{" +
                    "id=" + id +
                    ", txShare=" + txShare +
                    ", txSize=" + txSize +
                    ", txFees=" + txFees +
                    ", txDistribution='" + txDistribution + '\'' +
                    ", txPoolSize=" + txPoolSize +
                    ", verificationTime=" + verificationTime +
                    '}';
        }
    }

    private HashMap<Integer, ProofBasedPeerConfig> proofBasedPeerConfigs;
    private HashMap<Integer, DPoSPeerConfig> dPoSPeerConfigs;

    private List<CoordinatorEntry> coordinatorAddresses;

    public static class CoordinatorEntry {
        public CoordinatorEntry(InetSocketAddress address, double computingShare) {
            this.address = address;
            this.computingShare = computingShare;
        }

        public CoordinatorEntry(String address, double computingShare) {
            String[] ar = address.split(":");
            this.address = new InetSocketAddress(ar[0], Integer.parseInt(ar[1]));
            this.computingShare = computingShare;
        }

        public InetSocketAddress address;
        public double computingShare;
    }

    private GraphStrategy graphStrategy;

    private ConfigYAML yaml;

    public Config(String filePath) throws IOException {
            yaml = ConfigYAML.parse(filePath);

            this.coordinatorAddresses = yaml.coordinators.stream().map(s -> {
                String[] ar = s.address.split(":");
                return new CoordinatorEntry(new InetSocketAddress(ar[0], Integer.parseInt(ar[1])), s.computingShare);
            }).collect(Collectors.toList());

            setGraphStrategy();

            createProofBasedPeerConfigs();
            createDPoSPeerConfigs();
    }

    public void setGraphStrategy() {
        switch (yaml.networkType) {
            case "random":
                if(yaml.random.perEdge)
                    this.graphStrategy = new RndGraphStrategy(yaml.random.nodes, yaml.random.latency, yaml.random.density, yaml.random.bandwidth);
                else
                    this.graphStrategy = new RndGraphWithAvgPropagationDelay(yaml.random.nodes, yaml.random.latency, yaml.random.density, yaml.random.bandwidth);
                break;
            case "explicit":
                if(yaml.explicit.fileName != null) {
                    this.graphStrategy = new ExplicitGraphStrategy(yaml.explicit.fileName, yaml.explicit.bandwidth);
                } else {
                    this.graphStrategy = new ExplicitGraphStrategy(yaml.explicit.peers, yaml.explicit.bandwidth);
                }
                break;
            case "scaleFree":
                if(yaml.scaleFree.perEdge)
                    this.graphStrategy = new ScaleFreeStrategy(yaml.scaleFree.nodes, yaml.scaleFree.m, yaml.scaleFree.latency, yaml.scaleFree.bandwidth);
                else
                    this.graphStrategy = new ScaleFreeGraphWithAvgPropagationDelay(yaml.scaleFree.nodes, yaml.scaleFree.m, yaml.scaleFree.latency, yaml.scaleFree.bandwidth);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public String getBlockchainType() {
        return yaml.blockchainType;
    }

    public void setGraphStrategy(GraphStrategy gs) {
        graphStrategy = gs;
    }

    public GraphStrategy getGraphStrategy() {
        return graphStrategy;
    }

    public List<CoordinatorEntry> getCoordinatorAddresses() {
        return coordinatorAddresses;
    }

    public void setCoordinatorEntries(List<CoordinatorEntry> entries) {
        this.coordinatorAddresses = entries;
    }

    public void setCoordinatorAddresses(List<InetSocketAddress> addresses) {
        this.coordinatorAddresses = addresses.stream().map(a -> new CoordinatorEntry(a, 1)).collect(Collectors.toList());
    }

    public Level getLogLevel() {
        return Level.parse(yaml.logLevel);
    }

    public boolean renderGraph() {
        return yaml.renderGraph;
    }

    public int getRuns() {
        return yaml.runs;
    }

    public boolean getManualStart() {
        return yaml.manualStart;
    }

    public boolean getExportAsJson() {
        return yaml.exportAsJson;
    }

    public String getFileName() {
        return yaml.fileName;
    }

    public boolean renderBlockchain() {
        return yaml.renderBlockchain;
    }

    public boolean simulateFullBlocks() {
        return yaml.blockchainDefaults.simulateFullBlocks;
    }

    public ConfigYAML.BlockchainDefaults getBlockchainDefaults() {
        return yaml.blockchainDefaults;
    }

    public ConfigYAML.ProofBased getProofBasedStrat() {
        return yaml.proofBased;
    }

    public ConfigYAML.DPosBFT getDPoSStrat() {
        return yaml.dPoSBFT;
    }

    public ConfigYAML getConfigYAML() {
        return yaml;
    }

    public int getNetworkDelay() {
        return yaml.networkDelay;
    }

    public int getSkipBlocks() {
        return yaml.skipBlocks;
    }

    public void validate() {
        yaml.validate();
    }

    public void createProofBasedPeerConfigs() {
        int n = getGraphStrategy().getNodes();
        HashMap<Integer, ProofBasedPeerConfig> configs = new HashMap<>(n);
        var yamlPeers = getProofBasedStrat().peers;
        var miningDistribution = getProofBasedStrat().miningDistribution;
        var txDistribution = getBlockchainDefaults().txPeerDistribution;
        for(int i = 0; i < n; i++) {
            final int final_i = i;
            ConfigYAML.ProofBased.Peer pe = null;
            if(yamlPeers != null) {
                pe = yamlPeers.stream().filter(peer -> peer.id == final_i).findFirst().orElse(null);
            }
            configs.put(i, new ProofBasedPeerConfig(i, miningDistribution, txDistribution, pe, getBlockchainDefaults()));
        }
        proofBasedPeerConfigs = configs;
    }

    public HashMap<Integer, ProofBasedPeerConfig> getProofBasedPeerConfigs() {
        return proofBasedPeerConfigs;
    }

    public void createDPoSPeerConfigs() {
        int n = getGraphStrategy().getNodes();
        HashMap<Integer, DPoSPeerConfig> configs = new HashMap<>(n);
        var yamlPeers = getDPoSStrat().peers;
        var txDistribution = getBlockchainDefaults().txPeerDistribution;
        for(int i = 0; i < n; i++) {
            final int final_i = i;
            ConfigYAML.DPosBFT.Peer pe = null;
            if(yamlPeers != null) {
                pe = yamlPeers.stream().filter(peer -> peer.id == final_i).findFirst().orElse(null);
            }
            configs.put(i, new DPoSPeerConfig(i, txDistribution, pe, getBlockchainDefaults()));
        }
        dPoSPeerConfigs = configs;
    }

    public HashMap<Integer, DPoSPeerConfig> getDPoSPeerConfigs() {
        return dPoSPeerConfigs;
    }

    public double getTotalMiningShare() {
        return proofBasedPeerConfigs.values().stream().mapToDouble(c -> c.miningShare).sum();
    }

    public double getTotalTxShare() {
        return proofBasedPeerConfigs.values().stream().mapToDouble(c -> c.txShare).sum();
    }

    public Blockchain newBlockchain(int poolSize, int confirmations) {
        boolean rndTieResolution = "random".equalsIgnoreCase(getBlockchainDefaults().tieResolution);
        switch(getBlockchainDefaults().forkResolution) {
            case "GHOST": return new GHOSTBlockchain(getBlockchainDefaults().blockSize, poolSize, confirmations, getBlockchainDefaults().blocks, rndTieResolution, getBlockchainDefaults().ghostDepth);
            default: return new LCRBlockchain(getBlockchainDefaults().blockSize, poolSize, confirmations, getBlockchainDefaults().blocks, rndTieResolution);
        }
    }

    public String toString() {
        return getConfigYAML().toString();
    }

    public String toJsonString() throws JsonProcessingException {
        return yaml.toJsonString();
    }
}
