package de.tum.i11.bcsim.blockchain;

import java.util.*;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

public class GHOSTBlockchain extends Blockchain{

    private final int ghostDepth;

    public GHOSTBlockchain(int blockSize, int poolSize, int confirmations, int capacity, boolean rndForkResolution, int ghostDepth) {
        super(blockSize, poolSize, confirmations, capacity, rndForkResolution);
        this.ghostDepth = ghostDepth;
    }

    @Override
    public synchronized List<BlockEntry> getTips() {
        // calculate subtree sizes of trees, only considering latest blocks until |maxBlockchainLength-ghostDepth)
        var map = getGreedyHeaviestObservedSubtreeSizesFrom(heightMap.get(Math.max(1,heightMap.size()-ghostDepth)));
        var toUnfold = new LinkedList<BlockEntry>();
        var found = new LinkedList<BlockEntry>();

        // Traverse the trees until a leaf is reached, at each choice select the node with the heavier subtree
        retainMax(map.entrySet().stream(), map.entrySet().stream(), Map.Entry::getValue).forEach(e -> toUnfold.add(getBlock(e.getKey())));
        while (!toUnfold.isEmpty()) {
            BlockEntry next = toUnfold.removeFirst();
            if(next.children.isEmpty()) {
                found.add(next);
            } else {
                retainMax(next.children.stream(), next.children.stream(), e -> map.get(e.block.getBlockId())).forEach(toUnfold::add);
            }
        }

        return found;
    }

    private static <T> Stream<T> retainMax(Stream<T> stream, Stream<T> maxStream, ToIntFunction<T> toInt) {
        int max = maxStream.mapToInt(toInt).max().getAsInt();
        return stream.filter(e -> toInt.applyAsInt(e) == max);
    }
}
