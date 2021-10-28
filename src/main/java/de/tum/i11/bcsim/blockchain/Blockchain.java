package de.tum.i11.bcsim.blockchain;

import com.google.protobuf.Timestamp;
import de.tum.i11.bcsim.proto.Messages;
import de.tum.i11.bcsim.proto.Messages.Block;
import de.tum.i11.bcsim.util.Pair;
import de.tum.i11.bcsim.util.ThroughputMeasure;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Logger;

public abstract class Blockchain {
    private static final Logger LOGGER = Logger.getLogger(Blockchain.class.getName());

    public static class BlockEntry {
        public final Block block;
        final int height; // height of this block (genesis has height 1)
        boolean confirmed;
        final Collection<BlockEntry> children;

        BlockEntry(Block b, int height) {
            this.block = b;
            this.height = height;
            this.children = new LinkedList<>();
            this.confirmed = false;
        }

        public Collection<BlockEntry> getChildren() {
            return children;
        }

        public int getHeight() {
            return height;
        }

        void addChild(BlockEntry be) {
//            assert be.block.getParentId() == block.getBlockId() && be.height == height+1;
            children.add(be);
        }

        public String toString() {
            return "Block #"+block.getBlockId()+" {\n\theight: "+height
                    +"\n\tcreator: "+block.getCreator()
                    +"\n\tchildren: "+children.stream().map(b -> b.block.getBlockId()+", ").reduce("", String::concat)
                    +"\n\ttransactions: "+block.getTransactionList().stream().map(t -> t.getTxId()+", ").reduce("", String::concat)
                    +"\n}";
        }

        @Override
        public int hashCode() {
            int result = block.hashCode();
            result = 31 * result + height;
            result = 31 * result + (confirmed ? 1 : 0);
            result = 31 * result + children.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BlockEntry that = (BlockEntry) o;

            if (height != that.height || children.size() != that.children.size()) return false;
            return equalBlocks(block, that.block);
        }

        private static boolean equalBlocks(Block a, Block b) {
            if (a.getCreator() != b.getCreator() || a.getBlockId() != b.getBlockId() || a.getParentId() != b.getParentId())
                return false;
            if (a.getTs().getSeconds() != b.getTs().getSeconds() || a.getTransactionCount() != b.getTransactionCount())
                return false;
            LinkedList<Messages.Transaction> other = new LinkedList<>(b.getTransactionList());
            for(Messages.Transaction t : a.getTransactionList()) {
                if(!other.removeIf(ot -> t.getTxId() == ot.getTxId()))
                    return false;
            }
            return true;
        }
    }

    protected final Random rnd;
    protected final HashMap<Integer, List<BlockEntry>> heightMap; // map to look up all blocks of a specific height
    protected final HashMap<Integer, BlockEntry> idMap; // map to look up blocks by their ID
    protected final HashMap<Integer, List<Block>> orphans; // mapping Ids of missing blocks to their orphans
    protected final TransactionPool txPool;

    protected int blockSize; // maximum block size
    protected int confirmations; // number of blocks needed to confirm a transaction (1st confirmation == tx mined into a block)
    protected final HashMap<Integer, Consumer<Messages.Transaction>> txListeners; // consumers to be executed once a tx was confirmed
    protected final boolean rndForkResolution; // in case of multiple tips, break ties randomly (true) or select earliest block (false)

    private final ThroughputMeasure confirmedBlocks = new ThroughputMeasure();
    private final ThroughputMeasure confirmedTx = new ThroughputMeasure();
    private final ThroughputMeasure confirmedBytes = new ThroughputMeasure();
    private final ThroughputMeasure totalBlocks = new ThroughputMeasure();

    public Blockchain(int blockSize, int poolSize, int confirmations, int capacity, boolean rndForkResolution) {
        double loadFactor = 0.75;
        int cap = (int) Math.ceil(capacity / loadFactor) + 1;
        this.heightMap = new HashMap<>(cap);
        this.idMap = new HashMap<>(cap);
        this.orphans = new HashMap<>(cap);
        this.rnd = new Random();
        this.txPool = new TransactionPool(poolSize);
        this.blockSize = blockSize;
        this.confirmations = confirmations;
        this.rndForkResolution = rndForkResolution;
        this.txListeners = new HashMap<>((int) Math.ceil(blockSize*confirmations / loadFactor) + 1);

        BlockEntry genesis = new BlockEntry(Block.newBuilder().setBlockId(0).setParentId(0).setCreator(0).setTs(Timestamp.newBuilder()).build(), 1);
        genesis.confirmed = true;

        idMap.put(0, genesis);
        LinkedList<BlockEntry> l = new LinkedList<>();
        l.add(genesis);
        heightMap.put(1, l);
    }

    public int getBlockSize() {
        return blockSize;
    }

    public ThroughputMeasure getConfirmedTxThroughput() {
        return confirmedTx;
    }

    public ThroughputMeasure getConfirmedBlockThroughput() {
        return confirmedBlocks;
    }

    public ThroughputMeasure getConfirmedByteThroughput() {
        return confirmedBytes;
    }

    public ThroughputMeasure getTotalBlockThroughput() {
        return totalBlocks;
    }

    public ThroughputMeasure getTotalTxThroughput() {
        return txPool.getRegisteredTxPerSecond();
    }

    public synchronized int getTotalBlockNum() {
        return idMap.size()+orphans.size();
    }

    public HashMap<Integer, BlockEntry> getBlocks() {
        return idMap;
    }

    public HashMap<Integer, List<BlockEntry>> getHeightMap() {
        return heightMap;
    }

    public synchronized BlockEntry getBlock(int id) {
        return idMap.get(id);
    }

    public HashMap<Integer, List<Block>> getOrphans() {
        return orphans;
    }

    public TransactionPool getTxPool() {
        return txPool;
    }

    public int getNumberOfUnconfirmedTx() {
        return txListeners.size();
    }

    public int getStaleBlockNum(int skip) {
        return (int) (heightMap.values().stream().skip(skip).mapToInt(List::size).sum()-heightMap.values().stream().skip(skip).count());
    }


    /**
     * Counts the length and occurrences of all stale forks in this blockchain
     * @param skip the height of the blockchain at which to start counting
     * @return A mapping of fork lengths to the number of their occurrences in the blockchain
     */
    public synchronized HashMap<Integer, Integer> getForkLengths(int skip) {
        HashMap<Integer, Integer> lengths = new HashMap<>();
        // skip to the first block to be included in the measurement
        BlockEntry next = heightMap.values().stream().skip(skip).findFirst().orElse(List.of(getBlock(0)))
                .stream().max(Comparator.comparingInt(this::longestChainLengthFrom)).orElse(getBlock(0));
        while(!next.children.isEmpty()) {
            if(next.children.size() == 1) {
                // no fork starting at this block
                next = next.children.iterator().next();
            } else {
                // there is a fork starting at this block
                LinkedList<Pair<BlockEntry, Integer>> forks = new LinkedList<>();
                // add all forks starting at this block and their corresponding lengths
                for(BlockEntry e : next.children) {
                    forks.add(new Pair<>(e, longestChainLengthFrom(e)));
                }
                // select the next child of longest fork (the main chain) as the next node
                BlockEntry theNext = forks.stream().max(Comparator.comparingInt(p -> p._2)).get()._1;
                // add discovered forks to mapping
                forks.forEach(p -> {
                    if(p._1.block.getBlockId() != theNext.block.getBlockId()) {
                        lengths.merge(p._2, 1, Integer::sum);
                    }
                });
                next = theNext;
            }
        }
        return lengths;
    }

    /**
     * Count the length of the longest chain starting from the given block
     * @param start the {@link BlockEntry} to start counting at
     * @return the length of the longest chain
     */
    public synchronized int longestChainLengthFrom(BlockEntry start) {
        HashMap<Integer, Integer> dist = new HashMap<>();
        dist.put(start.block.getBlockId(), 1);
        LinkedList<BlockEntry> blocks = new LinkedList<>(start.children);
        // Breadth-first search
        while(!blocks.isEmpty()) {
            BlockEntry next = blocks.removeFirst();
            dist.put(next.block.getBlockId(), dist.get(next.block.getParentId())+1);
            blocks.addAll(next.children);
        }
        return dist.values().stream().max(Comparator.comparingInt(e -> e)).orElse(1);
    }

    /**
     * Calculates sizes of all subtrees of the trees starting at the given roots
     * @param blocks the roots of trees to calculate subtree sizes of
     * @return a map (blockID, treeSize) mapping a block to the size of the tree rooted at that block
     */
    public synchronized Map<Integer, Integer> getGreedyHeaviestObservedSubtreeSizesFrom(Collection<BlockEntry> blocks) {
        HashMap<Integer, Integer> map = new HashMap<>();
        for(BlockEntry e : blocks) {
            map.putAll(getGreedyHeaviestObservedSubtreeSizesFrom(e));
        }
        return map;
    }

    /**
     * Calculates sizes of all subtrees of the trees starting at the given root
     * @param b the root of the tree to calculate subtree sizes of
     * @return a map (blockID, treeSize) mapping a block to the size of the tree rooted at that block
     */
    private synchronized Map<Integer, Integer> getGreedyHeaviestObservedSubtreeSizesFrom(BlockEntry b) {
        HashMap<Integer, Integer> map = new HashMap<>();
        HashMap<Integer, Integer> childrenLeft = new HashMap<>();
        map.put(b.block.getBlockId(), 1);
        childrenLeft.put(b.block.getBlockId(), b.children.size());
        LinkedList<BlockEntry> toVisit = new LinkedList<>(b.children);

        // Visit the entire tree
        while (!toVisit.isEmpty()) {
            BlockEntry next = toVisit.removeFirst();
            toVisit.addAll(next.children);
            map.put(next.block.getBlockId(), 1);
            childrenLeft.put(next.block.getBlockId(), next.children.size());

            // for each leaf, prune the leaf, backtrack and increment tree sizes
            while(childrenLeft.get(next.block.getBlockId()) == 0 && next.block.getBlockId() != b.block.getBlockId()) {
                BlockEntry parent = getBlock(next.block.getParentId());
                map.merge(parent.block.getBlockId(), map.get(next.block.getBlockId()), Integer::sum);
                childrenLeft.merge(parent.block.getBlockId(), -1, Integer::sum);
                next = parent;
            }
        }
        return map;
    }

    /**
     * @return the number of blocks that were confirmed but ultimately ended up being stale
     */
    public synchronized int getConfirmedStaleBlockNum(int skip) {
        HashSet<Integer> toSkip = new HashSet<>();
        for(int i = 0; i < skip; i++) {
            if(heightMap.containsKey(i))
                heightMap.get(i).forEach(e -> toSkip.add(e.block.getBlockId()));
        }
        int count = 0;
        BlockEntry lastBlock = heightMap.get(heightMap.size()).get(0);
        // starting at the last block on the current main branch, traverse the main branch backwards and
        // count all confirmed blocks in branches that are not the main branch
        while(lastBlock.block.getBlockId() != 0) {
            BlockEntry parent = idMap.get(lastBlock.block.getParentId());
            if(!toSkip.contains(parent.block.getBlockId())) {
                for (BlockEntry child : parent.children) {
                    if (!child.equals(lastBlock) && !toSkip.contains(child.block.getBlockId())) {
                        count += countConfirmedBlocksInBranch(child);
                    }
                }
            }
            lastBlock = parent;
        }
        return count;
    }

    /**
     * Count the number of confirmed blocks in the branch starting with the given BlockEntry
     * @param start the root of the branch
     * @return the number of confirmed blocks in the branch
     */
    private synchronized int countConfirmedBlocksInBranch(BlockEntry start) {
        if(!start.confirmed) {
            return 0;
        }
        int count = 1;
        // recursively count confirmed blocks in all sub branches
        for(BlockEntry child : start.children) {
            count += countConfirmedBlocksInBranch(child);
        }
        return count;
    }

    /**
     * Set a consumer to be executed once the transaction with the given ID is confirmed.
     * @param txId the transaction Id
     * @param action the consumer to be executed
     */
    private synchronized void setTxListener(int txId, Consumer<Messages.Transaction> action) {
        txListeners.put(txId, action);
    }

    /**
     * Execute all TxListeners triggered by the inclusion of the given block in the blockchain
     * @param e the blockchain entry triggering the listeners
     */
    private synchronized void executeListeners(BlockEntry e) {
        BlockEntry target = e;
        long time = System.currentTimeMillis();
        // traverse backwards through the blockchain until the block confirmed by the new entry is reached
        // if the required number of confirmations is <= 1, the given entry confirms itself
        for(int i = 1; i < confirmations; i++) {
            if(target == null || target.block.getBlockId() == 0) {
                return;
            }
            target = idMap.get(target.block.getParentId());
        }
        if(target.confirmed) {
            return;
        }
        target.confirmed = true;
        confirmedBlocks.registerPackets(1, time);
        confirmedTx.registerPackets(target.block.getTransactionCount(), time);
        int byteSize = target.block.getTransactionList().stream().mapToInt(t -> t.getData().size()).sum();
        confirmedBytes.registerPackets(byteSize, time);
        // execute all listeners of transactions in the newly confirmed block
        for(Messages.Transaction t : target.block.getTransactionList()) {
            var consumer = txListeners.get(t.getTxId());
            if(consumer != null) {
                consumer.accept(t);
                txListeners.remove(t.getTxId());
            }
        }
    }

    /**
     * @param blockId the block ID
     * @return true iff this blockchain contains a block with the given id (main or stale branches)
     */
    public synchronized boolean containsBlock(int blockId) {
        return getBlock(blockId) != null;
    }

    /**
     * Add a finalized block to this blockchain (parent block ID is set)
     * @param b the block
     * @return false iff this block already exists in this blockchain
     */
    public synchronized boolean addBlock(Block b) {
        if(getBlock(b.getBlockId()) != null) {
            return false;
        }
        BlockEntry parent = idMap.get(b.getParentId());
        if(parent == null) {
            // Parent is unknown, add this block as an orphan
            LOGGER.finest("Adding block to orphans");
            orphans.compute(b.getParentId(), (key, list) -> {
                if(list == null) {
                    LinkedList<Block> l = new LinkedList<>();
                    l.add(b);
                    return l;
                } else {
                    list.add(b);
                    return list;
                }
            });
            return true;
        }

        LOGGER.finest("Adding block to parent "+parent.block.getBlockId());

        var tips = getTips();
        boolean extendsTip = tips.stream().mapToInt(e -> e.block.getBlockId()).anyMatch(id -> id == parent.block.getBlockId());
        boolean multipleTips = tips.size() > 1;

        // if the new block resolves a fork, release transactions of the stale branch back into pool
        if(extendsTip && multipleTips) {
            LOGGER.finest("Block resolves fork");
            onForkChoice(parent, tips);
        }

        // if the new block extends the main chain, remove its transactions from the pool
        if(extendsTip) {
            LOGGER.finest("Block extends main chain, updating txPool");
            txPool.insertBlock(b);
        }

        LOGGER.finest("Creating block index");

        totalBlocks.registerPackets(1);
        // add new block to data structures
        BlockEntry newBlock = new BlockEntry(b, parent.height+1);
        parent.addChild(newBlock);
        idMap.put(b.getBlockId(), newBlock);
        heightMap.compute(newBlock.height, (key, list) -> {
           if(list == null) {
               LinkedList<BlockEntry> l = new LinkedList<>();
               l.add(newBlock);
               return l;
           } else {
               list.add(newBlock);
               return list;
           }
        });

        // execute transaction listeners triggered by the inclusion of this block
        executeListeners(newBlock);
        // add all oprhans resolved by the new block to the chain as well
        if(orphans.containsKey(b.getBlockId())) {
            for(Block o : orphans.get(b.getBlockId())) {
                addBlock(o);
            }
            orphans.remove(b.getBlockId());
        }
        return true;
    }

    /**
     * Find the least common ancestor of the given blocks. Used to switch between forks of the blockchain
     * @param blocks the blocks to find an ancestor of
     * @return
     */
    public BlockEntry findLeastCommonAncestor(Collection<BlockEntry> blocks) {
        BlockEntry[] es = blocks.toArray(new BlockEntry[0]);
        OptionalInt minOpt = blocks.stream().mapToInt(e -> e.height).min();
        if(minOpt.isEmpty()) return getBlock(0);
        for(int i = 0; i < es.length; i++) {
            while(es[i].height > minOpt.getAsInt()) {
                es[i] = getBlock(es[i].block.getParentId());
            }
        }

        while(!equalIds(es)) {
            for(int i = 0; i < es.length; i++) {
                es[i] = getBlock(es[i].block.getParentId());
            }
        }

        return es[0];
    }

    private boolean equalIds(BlockEntry[] es) {
        return Arrays.stream(es).mapToInt(e -> e.block.getBlockId()).distinct().count() == 1;
    }

    /**
     * Select the branch with the given leaf node as the new main branch. This means some transactions will no longer be
     * part of the main chain and are released back into the txPool. Other transactions that were previously not on the
     * main chain might be after this operation. After resolving the fork, the txPool reflects the new state of transactions
     * @param entry the leaf node of the new main branch
     * @param forks leafs of the forks chosen from
     */
    private synchronized void onForkChoice(BlockEntry entry, List<BlockEntry> forks) {
        // Prepare maps to store transactions of the main branch and all (now) stale branches
        HashMap<Integer, Messages.Transaction> inChain = new HashMap<>();
        HashMap<Integer, Messages.Transaction> inForks = new HashMap<>();

        BlockEntry lca = findLeastCommonAncestor(forks);
        BlockEntry[] bs = forks.toArray(new BlockEntry[0]);

        // Traverse backwards through forks until LCA is reached, keep track of TX in main and fork chains
        for(int i = 0; i < bs.length; i++) {
            boolean isMain = bs[i].block.getBlockId() == entry.block.getBlockId();
            while(bs[i].block.getBlockId() != lca.block.getBlockId()) {
                addTransactions(bs[i].block, isMain? inChain:inForks);
                bs[i] = getBlock(bs[i].block.getParentId());
            }
        }

        // some transactions might have been in both branches (stale and main). Remove them from the stale map
        for(Integer id : inChain.keySet()) {
            inForks.remove(id);
        }

        // instruct the txPool to update its view according to the maps
        txPool.switchForks(inChain, inForks);
    }

    /**
     * Calculate all leafs of the blockchain eligible to serve as parents for a new block
     * @return a list of tips
     */
    public abstract List<BlockEntry> getTips();

    private synchronized BlockEntry breakTie(List<BlockEntry> blocks) {
        BlockEntry selection;
        if(rndForkResolution) {
            // select random block as parent
            int rid = rnd.nextInt(blocks.size());
            LOGGER.finest("Chose index "+rid);
            selection = blocks.get(rid);
        } else {
            // select block that was created first as parent
            selection = blocks.stream().min((o1, o2) -> {
                Timestamp t1 = o1.block.getTs();
                Timestamp t2 = o2.block.getTs();
                if(t1.getSeconds() != t2.getSeconds()) {
                    return Long.compare(t1.getSeconds(), t2.getSeconds());
                }
                return Integer.compare(t1.getNanos(), t2.getNanos());
            }).orElse(blocks.get(0));
        }
        return selection;
    }

    /**
     * Prepare a newly mined block to be added to the blockchain by setting its parent id and filling it with transactions
     * @param b the block builder
     * @return the block builder
     */
    public synchronized Block.Builder prepareBlock(Block.Builder b) {
        List<BlockEntry> parents = getTips();

        if(parents.size() == 1) {
            txPool.fillBlock(b, blockSize);
            return b.setParentId(parents.get(0).block.getBlockId());
        }
        // multiple blocks possible as parent
        BlockEntry parent = breakTie(parents);

        b.setParentId(parent.block.getBlockId());

        onForkChoice(parent, parents);

        txPool.fillBlock(b, blockSize);
        return b;

    }

    /**
     * Add a newly mined block to the blockchain by setting its parent id and transactions, then appending it to the chain.
     * @param b the block builder
     */
    public synchronized void addNewBlock(Block.Builder b) {
        addBlock(prepareBlock(b).build());
    }

    /**
     * Add all transactions of the given block to the given map
     * @param b the block
     * @param map the map
     */
    private synchronized void addTransactions(Block b, HashMap<Integer, Messages.Transaction> map) {
        for(Messages.Transaction t : b.getTransactionList()) {
            map.put(t.getTxId(), t);
        }
    }

    /**
     * Add a transaction to this blockchain's txPool
     * @param t the transaction
     * @return false iff the given transaction already exists in the pool
     */
    public synchronized boolean addTransaction(Messages.Transaction t) {
        return txPool.addTransaction(t);
    }

    /**
     * Add a transaction to this blockchain's txPool and also register a consumer to be executed once the given transaction was confirmed
     * @param t the transaction to be added
     * @param action the consumer to be executed once the transaction was confirmed
     * @return fals iff the given transaction already exists in the pool
     */
    public synchronized boolean addTransaction(Messages.Transaction t, Consumer<Messages.Transaction> action) {
        if(txPool.addTransaction(t)) {
            setTxListener(t.getTxId(), action);
            return true;
        }
        return false;
    }

    /**
     * @return Return the id of the latest block in the chain (height wise) if there are multiple block with the same height, pick a random one
     */
    public synchronized int getLatestId() {
        return breakTie(getTips()).block.getBlockId();
    }

    public synchronized String toString() {
        return toGraphviz();
    }

    public synchronized String deepToString() {
        StringBuilder b = new StringBuilder("Blockchain size: "+idMap.size()+", height: "+heightMap.size()+"\n");
        heightMap.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry::getKey)).forEach(e -> {
            for(BlockEntry bl : e.getValue()) {
                b.append(bl).append("\n");
            }
        });
        return b.toString();
    }

    public String toGraphviz() {
        StringBuilder b = new StringBuilder("digraph G {0;");
        for(BlockEntry e : idMap.values()) {
            if(e.block.getBlockId() != 0) {
                b.append(e.block.getParentId()).append("->").append(e.block.getBlockId()).append(";");
            }
        }
        b.append("}");
        return b.toString();
    }

    public void renderGraphiz(String fileName) throws IOException {
        Graphviz.fromString(toGraphviz()).height(1000).width(2000).render(Format.SVG).toFile(new File(fileName));
    }
}
