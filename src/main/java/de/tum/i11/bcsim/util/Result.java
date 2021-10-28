package de.tum.i11.bcsim.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tum.i11.bcsim.blockchain.Blockchain;
import de.tum.i11.bcsim.config.Config;
import de.tum.i11.bcsim.config.ConfigYAML;
import de.tum.i11.bcsim.proto.Messages;

import java.util.*;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

public class Result {
    @JsonIgnore
    private List<Messages.ResultEntry> allEntries;

    @JsonIgnore
    String configString;
    public ConfigYAML config;
    public double executionTime;
    public List<CPULoad> cpuLoads;
    public double avgBlockSize;
    public long fullBlocks;
    public int staleBlocks;
    public List<ForkLength> forkLengths;
    public int confirmedStaleBlocks;
    public double avgPeerPoolSize;
    public double avgPeerOrphans;
    public double avgBlocksCreatedPerPeerPerSec;
    public double avgBlocksCreatedPerPeer;
    public double avgTxCreatedPerPeerPerSec;
    public double avgTxCreatedPerPeer;
    public double avgConfirmedBlocksPerPeerPerSec;
    public double avgConfirmedTxPerPeerPerSec;
    public double avgConfirmedBytePerPeerPerSec;
    public double avgTotalBlocksPerPeerPerSec;
    public double avgTotalTxPerPeerPerSec;
    public double avgUnconfirmedTxPerPeer;
    public double avgTxLat;
    public double medianTxLat;
    public double minTxLat;
    public double maxTxLat;
    public double avgTxSize;
    public double avgTxFee;
    public List<TxFee> txFees;
    public List<Double> allTxLats;

    public static class ForkLength {
        public double length;
        public double count;

        public String toString(){
            return "{"+length+"="+count+"}";
        }
    }

    public static class CPULoad {
        public String coordinator;
        public double maxCPULoad;
        public double avgCPULoad;

        @Override
        public String toString() {
            return "{" +
                    "coordinator='" + coordinator + '\'' +
                    ", maxCPULoad=" + maxCPULoad +
                    ", avgCPULoad=" + avgCPULoad +
                    '}';
        }
    }

    public static class TxFee {
        public double fee;
        public double count;

        public String toString(){
            return "{"+fee+"="+count+"}";
        }
    }

    public Result(Blockchain b, Config config, Map<String, Messages.Result> results, long executionTime) {

        this.allEntries = new LinkedList<>();
        results.values().forEach(r -> allEntries.addAll(r.getEntryList()));

        double avg = b.getBlocks().values().stream().mapToInt(bl -> bl.block.getTransactionCount()).sum()/(1.0*b.getBlocks().size());
        long fullCount = b.getBlocks().values().stream().filter(be -> be.block.getTransactionCount() >= config.getBlockchainDefaults().blockSize).count();

        HashSet<Double> fees = new HashSet<>();
        LinkedList<Messages.TxLatencyResult> allTxResults = new LinkedList<>();
        allEntries.forEach(r -> allTxResults.addAll(r.getTxLatencyList()));
        allTxResults.forEach(tx -> fees.add(tx.getFee()));
        HashMap<Double, Double> txFees = new HashMap<>();
        fees.forEach(fee -> txFees.put(fee, allTxResults.stream().filter(txRes -> txRes.getFee() == fee).mapToDouble(Messages.TxLatencyResult::getLatency).average().orElse(0)));

        LinkedList<Double> allLats = new LinkedList<>();
        allEntries.forEach(r -> r.getTxLatencyList().forEach(tx -> allLats.add(tx.getLatency())));

        this.configString = config.toString();
        this.config = config.getConfigYAML();
        this.executionTime = Math.round((executionTime/1000.0)/60.0);

        this.cpuLoads = results.values().stream().map(r -> {
            CPULoad l = new CPULoad();
            l.coordinator = r.getCoordAddress();
            l.avgCPULoad = r.getAvgCPULoad();
            l.maxCPULoad = r.getMaxCPULoad();
            return l;
        }).collect(Collectors.toList());

        avgBlockSize = avg;
        fullBlocks = fullCount;
        staleBlocks = b.getStaleBlockNum(config.getSkipBlocks());
        forkLengths = b.getForkLengths(config.getSkipBlocks()).entrySet().stream().map( e -> {
            ForkLength f = new ForkLength();
            f.count = e.getValue();
            f.length = e.getKey();
            return f;
        }).collect(Collectors.toList());
        confirmedStaleBlocks = b.getConfirmedStaleBlockNum(config.getSkipBlocks());
        avgPeerPoolSize = avgDouble(Messages.ResultEntry::getPoolSize);
        avgPeerOrphans = avgDouble(Messages.ResultEntry::getOrphans);
        avgBlocksCreatedPerPeerPerSec = avgDouble(Messages.ResultEntry::getCreatedBlocksPerSec);
        avgBlocksCreatedPerPeer = avgDouble(Messages.ResultEntry::getCreatedBlocks);
        avgTxCreatedPerPeerPerSec = avgDouble(Messages.ResultEntry::getCreatedTxPerSec);
        avgTxCreatedPerPeer = avgDouble(Messages.ResultEntry::getCreatedTx);
        avgConfirmedBlocksPerPeerPerSec = avgDouble(Messages.ResultEntry::getConfirmedBlocksPerSec);
        avgConfirmedTxPerPeerPerSec = avgDouble(Messages.ResultEntry::getConfirmedTxPerSec);
        avgConfirmedBytePerPeerPerSec = avgDouble(Messages.ResultEntry::getConfirmedBytesPerSec);
        avgTotalBlocksPerPeerPerSec = avgDouble(Messages.ResultEntry::getTotalBlocksPerSec);
        avgTotalTxPerPeerPerSec = avgDouble(Messages.ResultEntry::getTotalTxPerSec);
        avgUnconfirmedTxPerPeer = avgDouble(Messages.ResultEntry::getUnconfirmedTx);

        avgTxLat = allLats.stream().mapToDouble(e -> e).average().orElse(0);
        medianTxLat = allLats.stream().sorted().skip((long) Math.max(0, Math.ceil(allLats.size()/2.0)-1)).findFirst().orElse(-1.0);
        minTxLat = minDouble(r -> r.getTxLatencyList().stream().mapToDouble(Messages.TxLatencyResult::getLatency).min().orElse(Integer.MAX_VALUE));
        maxTxLat = maxDouble(r -> r.getTxLatencyList().stream().mapToDouble(Messages.TxLatencyResult::getLatency).max().orElse(Integer.MIN_VALUE));

        avgTxSize = allEntries.stream().filter(r -> !r.getTxLatencyList().isEmpty()).mapToDouble(r -> r.getTxLatencyList().stream().mapToDouble(Messages.TxLatencyResult::getSize).average().orElse(0)).average().orElse(0);
        avgTxFee = allEntries.stream().filter(r -> !r.getTxLatencyList().isEmpty()).mapToDouble(r -> r.getTxLatencyList().stream().mapToDouble(Messages.TxLatencyResult::getFee).average().orElse(0)).average().orElse(0);

        this.txFees = txFees.entrySet().stream().map( e -> {
            TxFee f = new TxFee();
            f.count = e.getValue();
            f.fee = e.getKey();
            return f;
        }).collect(Collectors.toList());
        allTxLats = allLats;
    }

    private double avgDouble(ToDoubleFunction<? super Messages.ResultEntry> mapper) {
        return allEntries.stream().mapToDouble(mapper).average().orElse(0);
    }

    private double minDouble(ToDoubleFunction<? super Messages.ResultEntry> mapper) {
        return allEntries.stream().mapToDouble(mapper).min().orElse(Integer.MAX_VALUE);
    }

    private double maxDouble(ToDoubleFunction<? super Messages.ResultEntry> mapper) {
        return allEntries.stream().mapToDouble(mapper).max().orElse(Integer.MIN_VALUE);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(config);

        sb.append("\nExecution time: "+executionTime);
        sb.append("\nCPU Loads: "+cpuLoads);
        sb.append("\nAvg Block Size: "+avgBlockSize);
        sb.append("\nFull Blocks Count: "+fullBlocks);
        sb.append("\nStale Blocks: "+staleBlocks);
        sb.append("\nFork Lengths: "+forkLengths);
        sb.append("\nConfirmed but Stale Blocks: "+confirmedStaleBlocks);
        sb.append("\nAvg Peer Pool Size: "+avgPeerPoolSize);
        sb.append("\nAvg Peer Orphans: "+avgPeerOrphans);
        sb.append("\nAvg Blocks created per peer per second: "+avgBlocksCreatedPerPeerPerSec);
        sb.append("\nAvg Blocks created per peer: "+avgBlocksCreatedPerPeer);
        sb.append("\nAvg Tx created per peer per second: "+avgTxCreatedPerPeerPerSec);
        sb.append("\nAvg Tx created per peer: "+avgTxCreatedPerPeer);
        sb.append("\nAvg Blocks confirmed per peer per second: "+avgConfirmedBlocksPerPeerPerSec);
        sb.append("\nAvg Tx confirmed per peer per second: "+avgConfirmedTxPerPeerPerSec);
        sb.append("\nAvg Bytes confirmed per peer per second: "+avgConfirmedBytePerPeerPerSec);
        sb.append("\nAvg total Blocks added per peer per second: "+avgTotalBlocksPerPeerPerSec);
        sb.append("\nAvg total Tx added per peer per second: "+avgTotalTxPerPeerPerSec);
        sb.append("\nAvg unconfirmed Tx per peer: "+avgUnconfirmedTxPerPeer);

        sb.append("\nAvg Tx Latency: "+avgTxLat);
        sb.append("\nMedian Tx Latency: "+medianTxLat);
        sb.append("\nMin Tx Latency: "+minTxLat);
        sb.append("\nMax Tx Latency: "+maxTxLat);

        sb.append("\nAvg Tx Size: "+avgTxSize);
        sb.append("\nAvg Tx Fee: "+avgTxFee);

        sb.append("\nAvg Tx Latency by Fee: "+txFees);
        sb.append("\nAll Tx latencies: "+allTxLats);

        return sb.toString();
    }

    public String toJsonString() throws JsonProcessingException {
        return new ObjectMapper().writeValueAsString(this);
    }
}
