package de.tum.i11.bcsim.blockchain;

import java.util.*;

public class LCRBlockchain extends Blockchain{

    public LCRBlockchain(int blockSize, int poolSize, int confirmations, int capacity, boolean rndForkResolution) {
        super(blockSize, poolSize, confirmations, capacity, rndForkResolution);
    }

    @Override
    public synchronized List<BlockEntry> getTips() {
        // tips according to longest chain rule are leafs at the current maximum height
        return heightMap.get(heightMap.size());
    }
}
