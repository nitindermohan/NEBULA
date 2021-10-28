package de.tum.i11.bcsim.config;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import javax.validation.constraints.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigYAML {
    @JsonIgnore
    private static final Logger LOGGER = Logger.getLogger(ConfigYAML.class.getName());


    @NotNull(message = "Specify at least one coordinator")
    @Size(min=1, message="Specify at least one coordinator")
    public List<CoordinatorEntry> coordinators = new ArrayList<>(List.of(new CoordinatorEntry("127.0.0.1:5151", 1)));

    public static class CoordinatorEntry {
        @NotNull
        public String address;

        @PositiveOrZero(message="Computing share out of bounds")
        public double computingShare;

        public CoordinatorEntry() {}
        public CoordinatorEntry(String address, double computingShare) {
            this.address = address;
            this.computingShare = computingShare;
        }

        @Override
        public String toString() {
            return "CoordinatorEntry{" +
                    "address='" + address + '\'' +
                    ", computingShare=" + computingShare +
                    '}';
        }
    }

    public String fileName;

    public Random random = new Random();
    public Explicit explicit = new Explicit();
    public ScaleFree scaleFree = new ScaleFree();

    public ProofBased proofBased = new ProofBased();
    public DPosBFT dPoSBFT = new DPosBFT();

    @NotNull(message = "Invalid network type, choose any of: random|scaleFree|explicit")
    @Pattern(regexp = "random|scaleFree|explicit", message = "Invalid network type, choose any of: random|scaleFree|explicit")
    public String networkType = "random";

    @NotNull(message = "Invalid blockchain type, choose any of: proofBased|dPoSBFT")
    @Pattern(regexp = "proofBased|dPoSBFT", message = "Invalid blockchain type, choose any of: proofBased|dPoSBFT")
    public String blockchainType = "proofBased";

    @NotNull(message = "Missing blockchain defaults")
    public BlockchainDefaults blockchainDefaults = new BlockchainDefaults();

    public boolean renderGraph = false;

    @NotNull(message = "Invalid log level, choose any of: ALL|CONFIG|FINE|FINER|FINEST|INFO|OFF|SEVERE|WARNING")
    @Pattern(regexp = "ALL|CONFIG|FINE|FINER|FINEST|INFO|OFF|SEVERE|WARNING", message = "Invalid log level, choose any of: ALL|CONFIG|FINE|FINER|FINEST|INFO|OFF|SEVERE|WARNING")
    public String logLevel = "INFO";
    @PositiveOrZero(message = "networkDelay must be non-negative")
    public int networkDelay = 5000;
    @PositiveOrZero(message = "skipBlocks must be non-negative")
    public int skipBlocks = 10;
    @Min(value = 1, message = "At least 1 run required")
    public int runs = 1;
    public boolean manualStart = true;
    public boolean exportAsJson = true;
    public boolean renderBlockchain = false;


    public static class Random {
        @Min(value = 1, message = "At least 1 node required")
        public int nodes = 100;
        @Positive(message = "Density must be positive")
        public double density = 0.1;
        @PositiveOrZero(message = "latency must be non-negative")
        public int latency = 5000;
        public boolean perEdge = true;
        @Positive(message = "bandwidth must be positive")
        public double bandwidth = 2000;

        @Override
        public String toString() {
            return "Random{" +
                    "nodes=" + nodes +
                    ", density=" + density +
                    ", latency=" + latency +
                    ", perEdge=" + perEdge +
                    ", bandwidth=" + bandwidth +
                    '}';
        }
    }

    public static class ScaleFree {
        @Min(value = 1, message = "At least 1 node required")
        public int nodes = 100;
        @Min(value = 1, message = "Parameter m must be at least 1")
        public int m = 2;
        @PositiveOrZero(message = "latency must be non-negative")
        public int latency = 5000;
        public boolean perEdge = true;
        @Positive(message = "bandwidth must be positive")
        public double bandwidth = 2000;

        @Override
        public String toString() {
            return "ScaleFree{" +
                    "nodes=" + nodes +
                    ", m=" + m +
                    ", latency=" + latency +
                    ", perEdge=" + perEdge +
                    ", bandwidth=" + bandwidth +
                    '}';
        }
    }

    public static class Explicit {
        @Positive(message = "bandwidth must be positive")
        public double bandwidth = 2000;
        public String fileName;
        public List<Peer> peers = new ArrayList<>();

        public static class Peer {
            @PositiveOrZero(message = "Peers must be enumerated from 0 to n-1")
            public int id;
            @NotNull(message = "Explicit peer edge list must be defined")
            public List<Edge> edges = new ArrayList<>();

            public Peer() {}

            @Override
            public String toString() {
                return "Peer{" +
                        "id=" + id +
                        ", edges=" + edges +
                        '}';
            }

            public Peer(int id, List<Edge> edges) {
                this.id = id;
                this.edges = edges;
            }

            public static class Edge {
                @PositiveOrZero(message = "Peers must be enumerated from 0 to n-1")
                public int id;
                @PositiveOrZero(message = "latency must be non-negative")
                public int latency;

                public Edge() {}

                public Edge(int id, int latency) {
                    this.id = id;
                    this.latency = latency;
                }

                @Override
                public String toString() {
                    return "Edge{" +
                            "id=" + id +
                            ", latency=" + latency +
                            '}';
                }
            }
        }

        @Override
        public String toString() {
            return "Explicit{" +
                    "peers=" + peers +
                    ", bandwidth=" + bandwidth +
                    '}';
        }
    }

    public static class Distribution {
        @NotNull(message = "Invalid distribution, choose any of: constant|uniform|exponential")
        @Pattern(regexp = "constant|uniform|exponential", message = "Invalid distribution, choose any of: constant|uniform|exponential")
        public String type = "constant";
        @Positive(message = "Distribution mean must be positive")
        public double mean = 1;

        @Override
        public String toString() {
            return "MiningDistribution{" +
                    "type='" + type + '\'' +
                    ", mean=" + mean +
                    '}';
        }
    }

    public static class ProofBased {
        @Min(value = 1, message = "Confirmations must be at least 1")
        public int confirmations = 1;
        @NotNull(message = "Mining distribution must be defined")
        public Distribution miningDistribution = new Distribution();
        public List<Peer> peers = new ArrayList<>();

        public static class Peer {
            public int id = -1;
            public double miningShare = -1;
            public double txShare = -1;
            public int txSize = -1;
            public List<TxFee> txFees;
            public String txDistribution;
            public int txPoolSize = -1;
            public int verificationTime = -1;

            @Override
            public String toString() {
                return "Peer{" +
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

        @Override
        public String toString() {
            return "ProofBased{" +
                    "confirmations=" + confirmations +
                    ", miningDistribution=" + miningDistribution +
                    ", peers=" + peers +
                    '}';
        }
    }

    public static class DPosBFT {
        public List<Integer> consensusNodes = new ArrayList<>();
        public int consensusNodeNum = 3;

        @NotNull(message = "Invalid BP selection strategy, choose any of: best|worst|random")
        @Pattern(regexp = "best|worst|random", message = "Invalid BP selection strategy, choose any of: best|worst|random")
        public String nodeSelection = "best";
        @Min(value = 1, message = "At least 1 block must be created per BP")
        public int blocksPerNode = 3;
        @PositiveOrZero(message = "skipLastBlocks must non-negative")
        public int skipLastBlocks = 0;
        @Min(value = 1, message = "Confirmations must be at least 1")
        public int confirmations = 1;
        public boolean randomShuffle = false;
        public boolean consensusOnOrchestrator = true;
        public List<Peer> peers = new ArrayList<>();

        public enum NodeSelection {BEST, RANDOM, WORST}

        public NodeSelection getNodeSelection() {
            switch (nodeSelection) {
                case "random": return NodeSelection.RANDOM;
                case "worst": return NodeSelection.WORST;
                default: return NodeSelection.BEST;
            }
        }

        public static class Peer {
            public int id = -1;
            public double txShare = -1;
            public int txSize = -1;
            public List<TxFee> txFees;
            public String txDistribution;
            public int txPoolSize = -1;
            public int verificationTime = -1;

            @Override
            public String toString() {
                return "Peer{" +
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

        @Override
        public String toString() {
            return "DPosBFT{" +
                    "consensusNodes=" + consensusNodes +
                    ", consensusNodeNum=" + consensusNodeNum +
                    ", nodeSelection='" + nodeSelection + '\'' +
                    ", blocksPerNode=" + blocksPerNode +
                    ", skipLastBlocks=" + skipLastBlocks +
                    ", confirmations=" + confirmations +
                    ", randomShuffle=" + randomShuffle +
                    ", consensusOnOrchestrator=" + consensusOnOrchestrator +
                    ", peers=" + peers +
                    '}';
        }
    }

    public static class TxFee {
        @PositiveOrZero(message = "TxFee must be non-negative")
        public int fee = 1;
        @Positive(message = "TxFee share must be positive")
        public int share = 1;

        public TxFee(int fee, int share) {
            this.fee = fee;
            this.share = share;
        }

        public TxFee() {}

        @Override
        public String toString() {
            return "TxFee{" +
                    "fee=" + fee +
                    ", share=" + share +
                    '}';
        }
    }

    public static class BlockchainDefaults {
        @PositiveOrZero(message = "Mining rate must be non-negative")
        public double miningRate = 1;
        @PositiveOrZero(message = "Tx rate must be non-negative")
        public double txRate = 10;
        @PositiveOrZero(message = "Tx size must be non-negative")
        public int txSize = 100;
        @NotNull(message = "At least one tx fee must be defined")
        @Size(min = 1, message = "At least one tx fee must be defined")
        public List<TxFee> txFees = new ArrayList<>(List.of(new TxFee(1,1)));
        @NotNull(message = "Invalid tx distribution, choose any of: poisson|constant|uniform")
        @Pattern(regexp = "poisson|constant|uniform", message = "Invalid tx distribution, choose any of: poisson|constant|uniform")
        public String txDistribution = "poisson";
        @NotNull(message = "Transaction peer distribution must be defined")
        public Distribution txPeerDistribution = new Distribution();
        @PositiveOrZero(message = "Block size must be non-negative")
        public int blockSize = 5000;
        @NotNull(message = "Invalid fork resolution strategy, choose any of: LCR|GHOST")
        @Pattern(regexp = "LCR|GHOST", message = "Invalid fork resolution strategy, choose any of: LCR|GHOST")
        public String forkResolution = "LCR";
        @NotNull(message = "Invalid fork resolution strategy, choose any of: LCR|GHOST")
        @Pattern(regexp = "random|first", message = "Invalid tie resolution strategy, choose any of: random|first")
        public String tieResolution = "first";
        @Min(value = 1, message = "At least 1 block must be created per BP")
        public int ghostDepth = 8;
        @PositiveOrZero(message = "Tx pool size must be non-negative")
        public int txPoolSize = 200000;
        @PositiveOrZero(message = "Verification time must be non-negative")
        public int verificationTime = 100;
        @Positive(message = "Number of blocks to be created must be positive")
        public int blocks = 500;
        public boolean pushBlocks = true;
        public boolean simulateFullBlocks = false;

        @Override
        public String toString() {
            return "BlockchainDefaults{" +
                    "miningRate=" + miningRate +
                    ", txRate=" + txRate +
                    ", txSize=" + txSize +
                    ", txFees=" + txFees +
                    ", txDistribution='" + txDistribution + '\'' +
                    ", txPeerDistribution=" + txPeerDistribution +
                    ", blockSize=" + blockSize +
                    ", forkResolution='" + forkResolution + '\'' +
                    ", tieResolution='" + tieResolution + '\'' +
                    ", ghostDepth=" + ghostDepth +
                    ", txPoolSize=" + txPoolSize +
                    ", verificationTime=" + verificationTime +
                    ", blocks=" + blocks +
                    ", pushBlocks=" + pushBlocks +
                    ", simulateFullBlocks=" + simulateFullBlocks +
                    '}';
        }
    }

    public static ConfigYAML parse(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        File f = new File(filePath);
        ConfigYAML c = mapper.readValue(f, ConfigYAML.class);
        c.fileName = f.getName();
        c.validate();
        return c;
    }

    public void validate() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation> violations = new HashSet<>(validator.validate(this));
        boolean violationFound = false;

        for(CoordinatorEntry e : coordinators)
            violations.addAll(validator.validate(e));
        if(random != null) {
            violations.addAll(validator.validate(random));
        }
        if(explicit != null) {
            violations.addAll(validator.validate(explicit));
            for (Explicit.Peer p : explicit.peers) {
                violations.addAll(validator.validate(p));
                for (Explicit.Peer.Edge e : p.edges) {
                    violations.addAll(validator.validate(e));
                }
            }
        }
        if(scaleFree != null) {
            violations.addAll(validator.validate(scaleFree));
        }
        if(proofBased != null) {
            violations.addAll(validator.validate(proofBased));
            violations.addAll(validator.validate(proofBased.miningDistribution));
            if(proofBased.peers != null) {
                for (ProofBased.Peer p : proofBased.peers) {
                    violations.addAll(validator.validate(p));
                }
            }
        }
        if(dPoSBFT != null) {
            violations.addAll(validator.validate(dPoSBFT));
            if(dPoSBFT.peers != null) {
                for (DPosBFT.Peer p : dPoSBFT.peers) {
                    violations.addAll(validator.validate(p));
                }
            }
            if(dPoSBFT.skipLastBlocks >= dPoSBFT.blocksPerNode) {
                LOGGER.severe("Skipping more DPoS blocks per round than there are produced");
                violationFound = true;
            }
        }
        violations.addAll(validator.validate(blockchainDefaults));
        for(TxFee f : blockchainDefaults.txFees) {
            violations.addAll(validator.validate(f));
        }

        if(!violations.isEmpty() || violationFound) {
            for (ConstraintViolation violation : violations) {
                LOGGER.severe(violation.getMessage());
            }
            System.exit(1);
        }
    }

    public String toJsonString() throws JsonProcessingException {
        return new ObjectMapper().writeValueAsString(this);
    }

    @Override
    public String toString() {
        return "ConfigYAML{" +
                "\ncoordinators=" + coordinators +
                "\nfileName='" + fileName + '\'' +
                ("random".equals(networkType)? "\nrandom=" + random : "") +
                ("explicit".equals(networkType)? "\nexplicit=" + explicit : "") +
                ("scaleFree".equals(networkType)? "\nscaleFree=" + scaleFree : "") +
                ("proofBased".equals(blockchainType)? "\nproofBased=" + proofBased : "") +
                ("dPoSBFT".equals(blockchainType)? "\ndPoSBFT=" + dPoSBFT : "") +
//                "\nnetworkType='" + networkType + '\'' +
//                "\nblockchainType='" + blockchainType + '\'' +
                "\nblockchainDefaults=" + blockchainDefaults +
                "\nrenderGraph=" + renderGraph +
                "\nlogLevel='" + logLevel + '\'' +
                "\nnetworkDelay=" + networkDelay +
                "\nskipBlocks=" + skipBlocks +
                "\nruns=" + runs +
                "\nmanualStart=" + manualStart +
                "\nrenderBlockchain=" + renderBlockchain +
                '}';
    }
}
