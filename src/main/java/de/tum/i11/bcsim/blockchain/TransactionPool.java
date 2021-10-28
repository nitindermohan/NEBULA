package de.tum.i11.bcsim.blockchain;

import de.tum.i11.bcsim.proto.Messages;
import de.tum.i11.bcsim.proto.Messages.Transaction;
import de.tum.i11.bcsim.util.ThroughputMeasure;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.logging.Logger;

public class TransactionPool {
    private static final Logger LOGGER = Logger.getLogger(TransactionPool.class.getName());

    private final HashMap<Integer, Transaction> inChain; // mapping txIDs to transactions currently in the main chain
    private LinkedHashMap<Integer, Transaction> inPool; // mapping txIDs to transactions not currently in the main chain
    private final int maxSize; // maximum size of the txPool
    private final ThroughputMeasure totalTx = new ThroughputMeasure();

    TransactionPool(int maxSize) {
        double loadFactor = 0.75;
        int cap = (int) Math.ceil(maxSize / loadFactor) + 1;
        this.inChain = new HashMap<>(cap);
        this.inPool = new LinkedHashMap<>(cap, (float) loadFactor, true);
        this.maxSize = maxSize;
    }

    /**
     * @param t the transaction
     * @return true iff the given transaction exists either in the blockchain or the txPool
     */
    public synchronized boolean contains(Transaction t) {
        return inChain.containsKey(t.getTxId()) || inPool.containsKey(t.getTxId());
    }

    /**
     * @return total number of transactions in both blockchain and pool
     */
    public synchronized int size() {
        return inChain.size() + inPool.size();
    }

    public synchronized int inPoolSize() {
        return inPool.size();
    }

    public HashMap<Integer, Transaction> getChainMap() {
        return inChain;
    }

    public HashMap<Integer, Transaction> getPoolMap() {
        return inPool;
    }

    /**
     * Move tx content of the given block from pool to chain maps
     * @param block the block
     */
    synchronized void insertBlock(Messages.Block block) {
        LOGGER.finest("Removing "+block.getBlockId()+" content from pool");
        for(Transaction t : block.getTransactionList()) {
            inChain.put(t.getTxId(), t);
            inPool.remove(t.getTxId(), t);
        }
    }

    /**
     * Fill the given block builder with transactions up to the given maximum. Transactions with the highest fee are selected first.
     * @param builder the block builder to be filled
     * @param max the maximum number of transactions to be added
     * @return the block builder
     */
    synchronized Messages.Block.Builder fillBlock(Messages.Block.Builder builder, int max) {
        LOGGER.finest("Filling block with transactions");
        // sort txPool by their transaction fees and creation time (descending)
        LinkedList<Transaction> pool = new LinkedList<>(inPool.values());
        pool.sort((t1, t2) -> {
            if(t1.getTxFee() != t2.getTxFee()) {
                return t2.getTxFee() - t1.getTxFee();
            }
            if(t1.getTs().getSeconds() != t2.getTs().getSeconds()) {
                return Long.compare(t1.getTs().getSeconds(), t2.getTs().getSeconds());
            }
            return Integer.compare(t1.getTs().getNanos(), t2.getTs().getNanos());
        });
        LinkedList<Transaction> l = new LinkedList<>();
        // select transactions to be included until all are selected or the mac is reached
        for(Transaction t : pool) {
            if(max-- <= 0) {
                break;
            }
            l.add(t);
        }
        // remove selected transactions from pool and put them into chain map
        for(Transaction t : l) {
            inPool.remove(t.getTxId());
            inChain.put(t.getTxId(), t);
        }
        // add transactions to block builder
        return builder.addAllTransaction(l);
    }

    /**
     * Update pool and chain map to reflect the blockchain's state after switching to a different branch
     * @param inChain transactions that are now part of the main chain
     * @param inPool transactions that are now part of the txPool
     */
    synchronized void switchForks(HashMap<Integer, Transaction> inChain, HashMap<Integer, Transaction> inPool) {
        LOGGER.finest("Updating pool after switch");
        for(Transaction t : inChain.values()) {
            this.inChain.put(t.getTxId(), t);
            this.inPool.remove(t.getTxId());
        }
        for(Transaction t : inPool.values()) {
            this.inChain.remove(t.getTxId());
            this.inPool.put(t.getTxId(), t);
        }
    }

    /**
     * @param t the transaction to be added to the pool
     * @return false iff the pool or the chain already contains the transaction
     */
    public synchronized boolean addTransaction(Transaction t) {
        if(inChain.containsKey(t.getTxId()) || inPool.containsKey(t.getTxId()) || inPool.size() >= maxSize) {
            return false;
        }
        totalTx.registerPackets(1);
        inPool.put(t.getTxId(), t);
        return true;
    }

    public ThroughputMeasure getRegisteredTxPerSecond() {
        return totalTx;
    }

    public String toString() {
        return "TransactionPool {" +
                "\n\tchain:" + inChain.keySet() +
                "\n\tpool: " + inPool.keySet() +
                "\n}";
    }

}
