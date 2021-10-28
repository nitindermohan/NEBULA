package de.tum.i11.bcsim;

import com.google.protobuf.Timestamp;
import de.tum.i11.bcsim.blockchain.Blockchain;
import de.tum.i11.bcsim.blockchain.GHOSTBlockchain;
import de.tum.i11.bcsim.blockchain.LCRBlockchain;
import de.tum.i11.bcsim.proto.Messages;
import de.tum.i11.bcsim.proto.Messages.*;
import de.tum.i11.bcsim.config.ConfigYAML;
import de.tum.i11.bcsim.util.LogSetup;
import de.tum.i11.bcsim.util.Util;
import org.junit.jupiter.api.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class TestBlockchain {
    private static final Logger LOGGER = Logger.getLogger(TestBlockchain.class.getName());


    @BeforeAll
    static void initAll() {
        LogSetup.setupLogging(null, Level.INFO);
    }

    @BeforeEach
    void init(TestInfo testInfo) {
    }

    @RepeatedTest(5)
    void testBc() {
        bcTest(new LCRBlockchain(10, 100, 6, 20, Util.getRndInt(0,2)<1));
        bcTest(new GHOSTBlockchain(10, 100, 6, 20, Util.getRndInt(0,2)<1, 1));
        bcTest(new GHOSTBlockchain(10, 100, 6, 20, Util.getRndInt(0,2)<1, 3));
        bcTest(new GHOSTBlockchain(10, 100, 6, 20, Util.getRndInt(0,2)<1, 8));
    }


    void bcTest(Blockchain bc) {
        assertThat(bc.getLatestId(), is(0));
        assertThat(bc.getTotalBlockNum(), is(1));
        assertThat(bc.getTxPool().size(), is(0));
        assertDoesNotThrow(() -> bc.getTxPool().toString());
        assertDoesNotThrow(bc::toString);

        for(int i = 1; i <= 22; i ++) {
            bc.getTxPool().addTransaction(tx(i));
        }
        assertDisjoint(bc.getTxPool().getChainMap(), bc.getTxPool().getPoolMap());
        assertTrue(bc.getTxPool().contains(tx(15)));
        assertFalse(bc.getTxPool().addTransaction(tx(15)));

        bc.addNewBlock(blockBuilder(1));
        assertDisjoint(bc.getTxPool().getChainMap(), bc.getTxPool().getPoolMap());

        bc.addBlock(block(2, 0, 21, 30));
        assertFalse(bc.addBlock(block(2, 0, 21, 30)));
        assertDisjoint(bc.getTxPool().getChainMap(), bc.getTxPool().getPoolMap());
        assertFalse(bc.getTxPool().contains(tx(25)));

        bc.addNewBlock(blockBuilder(3));
        assertDisjoint(bc.getTxPool().getChainMap(), bc.getTxPool().getPoolMap());
        assertTrue(bc.getTxPool().contains(tx(25)));

        bc.addNewBlock(blockBuilder(4));
        assertDisjoint(bc.getTxPool().getChainMap(), bc.getTxPool().getPoolMap());
        assertTrue(bc.getTxPool().getPoolMap().isEmpty());
        assertThat(bc.getTxPool().getChainMap().size(), is(30));
        for(int i = 1; i <= 30; i++) {
            assertTrue(bc.getTxPool().getChainMap().containsKey(i));
        }

        assertTrue(bc.getTxPool().contains(tx(25)));
        assertFalse(bc.getTxPool().addTransaction(tx(25)));
        assertDoesNotThrow(bc::toString);
        assertThat(bc.getTotalBlockNum(), is(5));

        assertDoesNotThrow(bc::deepToString);
    }

    @Test
    void testOrphans() {
        orphanTest(new LCRBlockchain(10, 100, 6, 20, false));
        orphanTest(new GHOSTBlockchain(10, 100, 6, 20, false, 1));
        orphanTest(new GHOSTBlockchain(10, 100, 6, 20, false, 3));
        orphanTest(new GHOSTBlockchain(10, 100, 6, 20, false, 8));
    }

    void orphanTest(Blockchain bc) {
        assertTrue(bc.addBlock(block(2, 1, 11, 20)));
        assertTrue(bc.addBlock(block(3, 1, 11, 20)));
        assertThat(bc.getTxPool().size(), is(0));
        assertThat(bc.getTotalBlockNum(), is(2));
        assertThat(bc.getOrphans().size(), is(1));
        assertTrue(bc.getOrphans().containsKey(1));

        assertDisjoint(bc.getTxPool().getChainMap(), bc.getTxPool().getPoolMap());

        assertTrue(bc.addBlock(block(1, 0, 1, 10)));
        assertTrue(bc.getOrphans().isEmpty());
        assertNotNull(bc.getBlock(2));
        assertNotNull(bc.getBlock(3));

        assertDisjoint(bc.getTxPool().getChainMap(), bc.getTxPool().getPoolMap());

        for(int i = 21; i <= 30; i ++) {
            bc.getTxPool().addTransaction(tx(i));
        }

        bc.addNewBlock(blockBuilder(4));
        assertDisjoint(bc.getTxPool().getChainMap(), bc.getTxPool().getPoolMap());
        assertTrue(bc.getTxPool().getPoolMap().isEmpty());
        assertThat(bc.getTxPool().getChainMap().size(), is(30));
        for(int i = 1; i <= 30; i++) {
            assertTrue(bc.getTxPool().getChainMap().containsKey(i));
        }
    }

    @Test
    void testMaxPoolSize() {
        LCRBlockchain bc = new LCRBlockchain(10, 4, 6, 20, false);
        assertTrue(bc.addTransaction(tx(1)));
        assertTrue(bc.addTransaction(tx(2)));
        assertTrue(bc.addTransaction(tx(3)));
        assertTrue(bc.addTransaction(tx(4)));
        assertFalse(bc.addTransaction(tx(5)));
        assertTrue(bc.getTxPool().contains(tx(4)));
        assertFalse(bc.getTxPool().contains(tx(5)));
    }

    @Test
    void testTxConfirmationListener() {
        txConfirmationListenerTest(new LCRBlockchain(10, 500, 4, 20, false));
        txConfirmationListenerTest(new GHOSTBlockchain(10, 500, 4, 20, false, 3));
    }

    void txConfirmationListenerTest(Blockchain bc) {
        boolean[] test = {false};
        bc.addTransaction(tx(1), t -> test[0] = true);
        for(int i = 2; i <= 50; i++) {
            bc.addTransaction(tx(i));
        }
        bc.addNewBlock(blockBuilder(1));
        bc.addNewBlock(blockBuilder(2));
        bc.addNewBlock(blockBuilder(3));
        bc.addBlock(block(4, 2, 31, 40));

        assertFalse(test[0]);

        bc.addNewBlock(blockBuilder(5));

        assertTrue(test[0]);
    }

    @Test
    void testTxConfirmationListenerSingleConfirmation() {
        txConfirmationListenerSingleConfirmationTest(new LCRBlockchain(10, 500, 1, 20, false));
        txConfirmationListenerSingleConfirmationTest(new GHOSTBlockchain(10, 500, 1, 20, false, 1));
        txConfirmationListenerSingleConfirmationTest(new GHOSTBlockchain(10, 500, 1, 20, false, 3));
        txConfirmationListenerSingleConfirmationTest(new GHOSTBlockchain(10, 500, 1, 20, false, 8));
    }

    void txConfirmationListenerSingleConfirmationTest(Blockchain bc) {
        boolean[] test = {false};
        bc.addTransaction(tx(1), t -> test[0] = true);
        for(int i = 2; i <= 59; i++) {
            bc.addTransaction(tx(i));
        }

        assertFalse(test[0]);

        bc.addNewBlock(blockBuilder(5));

        assertTrue(test[0]);
    }

    @Test
    void testFeeSelectionUniform() {
        List<ConfigYAML.TxFee> fees = List.of(new ConfigYAML.TxFee(0, 1), new ConfigYAML.TxFee(1, 1), new ConfigYAML.TxFee(2, 1));
        int[] selected = new int[fees.size()];
        double times = 1000;
        for(int i = 0; i < times; i++) {
            selected[Util.getFee(fees)]++;
        }
        assertTrue(diff(selected[0]/times, 1.0/3) < 0.1);
        assertTrue(diff(selected[1]/times, 1.0/3) < 0.1);
        assertTrue(diff(selected[2]/times, 1.0/3) < 0.1);
    }

    @Test
    void testFeeSelectionCustom1() {
        List<ConfigYAML.TxFee> fees = List.of(new ConfigYAML.TxFee(0, 1), new ConfigYAML.TxFee(1, 2), new ConfigYAML.TxFee(2, 1));
        int[] selected = new int[fees.size()];
        double times = 1000;
        for(int i = 0; i < times; i++) {
            selected[Util.getFee(fees)]++;
        }
        assertTrue(diff(selected[0]/times, 1.0/4) < 0.1);
        assertTrue(diff(selected[1]/times, 1.0/2) < 0.1);
        assertTrue(diff(selected[2]/times, 1.0/4) < 0.1);
    }

    @Test
    void testFeeSelectionCustom2() {
        List<ConfigYAML.TxFee> fees = List.of(new ConfigYAML.TxFee(0, 1), new ConfigYAML.TxFee(1, 10), new ConfigYAML.TxFee(2, 5));
        int[] selected = new int[fees.size()];
        double times = 1000;
        for(int i = 0; i < times; i++) {
            selected[Util.getFee(fees)]++;
        }
        assertTrue(diff(selected[0]/times, 1.0/16) < 0.1);
        assertTrue(diff(selected[1]/times, 5.0/8) < 0.1);
        assertTrue(diff(selected[2]/times, 5.0/16) < 0.1);
    }

    @Test
    void testFeeSelectionNoShares() {
        List<ConfigYAML.TxFee> fees = List.of(new ConfigYAML.TxFee(0, 0), new ConfigYAML.TxFee(1, 0), new ConfigYAML.TxFee(2, 0));
        int[] selected = new int[fees.size()];
        double times = 1000;
        for(int i = 0; i < times; i++) {
            selected[Util.getFee(fees)]++;
        }
        assertTrue(diff(selected[0]/times, 1.0/3) < 0.1);
        assertTrue(diff(selected[1]/times, 1.0/3) < 0.1);
        assertTrue(diff(selected[2]/times, 1.0/3) < 0.1);
    }

    @Test
    void testFeeSelectionAllSharesOnOne() {
        List<ConfigYAML.TxFee> fees = List.of(new ConfigYAML.TxFee(0, 0), new ConfigYAML.TxFee(1, 1), new ConfigYAML.TxFee(2, 0));
        int[] selected = new int[fees.size()];
        double times = 1000;
        for(int i = 0; i < times; i++) {
            selected[Util.getFee(fees)]++;
        }
        assertEquals(1000, selected[1]);
    }

    @Test
    void testTxInclusionAccordingToFee() {
        txInclusionAccordingToFeeTest(new LCRBlockchain(100, 1000, 1, 100, true));
        txInclusionAccordingToFeeTest(new GHOSTBlockchain(100, 1000, 1, 100, true, 1));
        txInclusionAccordingToFeeTest(new GHOSTBlockchain(100, 1000, 1, 100, true, 3));
        txInclusionAccordingToFeeTest(new GHOSTBlockchain(100, 1000, 1, 100, true, 8));

    }

    void txInclusionAccordingToFeeTest(Blockchain bc) {
        LinkedList<Transaction> ts = new LinkedList<>();
        for(int i = 0; i < 200; i++) {
            ts.add(tx(i, Util.getRndInt(0, 101)));
        }
        Collections.shuffle(ts);
        for(Transaction t : ts) {
            bc.addTransaction(t);
        }
        bc.addNewBlock(blockBuilder(1));
        Block b = bc.getBlock(1).block;

        ts.sort((t1, t2) -> t2.getTxFee() - t1.getTxFee());
        int fee100 = ts.get(99).getTxFee();

        assertTrue(b.getTransactionList().stream().allMatch(t -> t.getTxFee() >= fee100));
        assertTrue(bc.getTxPool().getPoolMap().values().stream().allMatch(t -> t.getTxFee() <= fee100));
    }

    @Test
    void testLongestChainLengthFrom() {
        longestChainLengthFromTest(c -> new LCRBlockchain(100, 1000, c, 100, true));
        longestChainLengthFromTest(c -> new GHOSTBlockchain(100, 1000, c, 100, true, 1));
        longestChainLengthFromTest(c -> new GHOSTBlockchain(100, 1000, c, 100, true, 3));
        longestChainLengthFromTest(c -> new GHOSTBlockchain(100, 1000, c, 100, true, 8));
    }

    Block[] bs = new Block[]{
            block(1,0,0,0),
            block(2,1,0,0),
            block(3,2,0,0),
            block(4,3,0,0),
            block(5,3,0,0),
            block(6,5,0,0),
            block(7,6,0,0),
            block(8,5,0,0),
            block(9,8,0,0),
            block(10,9,0,0),
            block(11,4,0,0),
            block(12,11,0,0),
            block(13,12,0,0),
            block(14,13,0,0),
            block(15,14,0,0),
            block(16,15,0,0),
            block(17,9,0,0),
            block(18,14,0,0),
            block(19,18,0,0),
            block(20,0,0,0),
            block(21,20,0,0)
    };

    @Test
    void testLCA() {
        Blockchain bc = new LCRBlockchain(100, 1000, 1, 100, true);
        Blockchain bc2 = new GHOSTBlockchain(100, 1000, 1, 100, true, 7);
        Arrays.stream(bs).forEach(bc::addBlock);
        Arrays.stream(bs).forEach(b -> {
            if(b.getBlockId() != 16 && b.getBlockId() != 19) {
                bc2.addBlock(b);
            }
        });
        assertEquals(bc2.findLeastCommonAncestor(bc2.getTips()).block.getBlockId(), 3);

        bc2.addBlock(bs[15]);
        bc2.addBlock(bs[18]);
        assertEquals(bc2.findLeastCommonAncestor(bc2.getTips()).block.getBlockId(), 14);
        assertEquals(bc.findLeastCommonAncestor(bc.getTips()).block.getBlockId(), 14);
        assertEquals(bc.findLeastCommonAncestor(bc.getHeightMap().get(4)).block.getBlockId(), 3);
        assertEquals(bc.findLeastCommonAncestor(bc.getHeightMap().get(8)).block.getBlockId(), 3);
        assertEquals(bc.findLeastCommonAncestor(bc.getHeightMap().get(3)).block.getBlockId(), 0);
        assertEquals(bc.findLeastCommonAncestor(bc.getHeightMap().get(1)).block.getBlockId(), 0);
    }

    @Test
    void testGHOST() {
        Blockchain bc = new GHOSTBlockchain(100, 1000, 1, 100, true, 7);

        for(int i = 1; i < bs.length; i++) {
            if(i != 16 && i != 19) {
                bc.addBlock(bs[i-1]);
            }
        }
        List<Integer> tips = bc.getTips().stream().map(e -> e.block.getBlockId()).collect(Collectors.toList());
        assertTrue(tips.contains(10)&&tips.contains(17)&&tips.contains(15)&&tips.contains(18)&&tips.size()==4);

        bc.addBlock(bs[15]);
        bc.addBlock(bs[18]);

        tips = bc.getTips().stream().map(e -> e.block.getBlockId()).collect(Collectors.toList());
        assertTrue(tips.contains(16)&&tips.contains(19)&&tips.size()==2);

        bc.addNewBlock(blockBuilder(22));
        int pid = bc.getBlock(22).block.getParentId();
        assertTrue(pid == 19 || pid == 16);

        bc = new GHOSTBlockchain(100, 1000, 1, 100, true, 4);

        for(int i = 1; i < bs.length; i++) {
            if(i != 16 && i != 19) {
                bc.addBlock(bs[i-1]);
            }
        }
        tips = bc.getTips().stream().map(e -> e.block.getBlockId()).collect(Collectors.toList());
        assertTrue(tips.contains(15)&&tips.contains(18)&&tips.size()==2);
        bc.addNewBlock(blockBuilder(22));
        pid = bc.getBlock(22).block.getParentId();
        assertTrue(pid == 15 || pid == 18);
    }

    void longestChainLengthFromTest(Function<Integer, Blockchain> bcSupplier) {
        Blockchain bc = bcSupplier.apply(1);
        Blockchain bc2 = bcSupplier.apply(2);
        Blockchain bc3 = bcSupplier.apply(3);

        Arrays.stream(bs).forEach(bc::addBlock);
        Arrays.stream(bs).forEach(bc2::addBlock);
        Arrays.stream(bs).forEach(bc3::addBlock);

        var forkLengths = bc.getForkLengths(0);
        assertThat(forkLengths.remove(2), is(2));
        assertThat(forkLengths.remove(4), is(1));
        assertTrue(forkLengths.isEmpty());

        assertThat(bc.longestChainLengthFrom(bc.getBlock(0)), is(11));
        assertThat(bc.longestChainLengthFrom(bc.getBlock(3)), is(8));
        assertThat(bc.longestChainLengthFrom(bc.getBlock(5)), is(4));
        assertThat(bc.longestChainLengthFrom(bc.getBlock(6)), is(2));
        assertThat(bc.longestChainLengthFrom(bc.getBlock(7)), is(1));
        assertThat(bc.longestChainLengthFrom(bc.getBlock(8)), is(3));

        assertEquals(bc.getConfirmedStaleBlockNum(0), 11);
        assertEquals(bc2.getConfirmedStaleBlockNum(0), 6);
        assertEquals(bc3.getConfirmedStaleBlockNum(0), 2);

        assertDoesNotThrow(() -> bc2.renderGraphiz("test.svg"));

        try {
            Files.deleteIfExists(Path.of("test.svg"));
        } catch (IOException ignored) {}

        var ttx = tx(12345);
        assertTrue(bc.addTransaction(ttx, tx -> {}));
        assertFalse(bc.addTransaction(ttx, tx -> {}));

       bc = bcSupplier.apply(1);

       for(int i = 1; i < 10000; i++) {
           bc.addBlock(block(i, i-1,0,0));
       }
        assertThat(bc.longestChainLengthFrom(bc.getBlock(0)), is(10000));
        assertThat(bc.getConfirmedStaleBlockNum(0), is(0));
    }

    private double diff(double a, double b) {
        return Math.abs(a-b);
    }

    private static Transaction tx(int id, int fee) {
        return Transaction.newBuilder().setTxId(id).setTs(Timestamp.newBuilder().build()).setTxFee(fee).build();
    }

    private static Transaction tx(int id) {
        return Transaction.newBuilder().setTxId(id).setTs(Timestamp.newBuilder().build()).setTxFee(0).build();
    }

    private static List<Transaction> getTx(int from, int to) {
        LinkedList<Transaction> ts = new LinkedList<>();
        for(int i = from; i <= to; i++) {
            ts.add(tx(i));
        }
        return ts;
    }

    private static Block block(int id, int parent, int txFrom, int txTo) {
        return blockBuilder(id).setParentId(parent).addAllTransaction(getTx(txFrom, txTo)).build();
    }

    private static Block.Builder blockBuilder(int id) {
        return Block.newBuilder().setBlockId(id).setCreator(0).setTs(Timestamp.newBuilder().build());
    }

    private static void assertDisjoint(Map<Integer, Transaction> a, Map<Integer, Transaction> b) {
        HashSet<Integer> keys = new HashSet<>();
        keys.addAll(a.keySet());
        keys.addAll(b.keySet());
        if(!keys.isEmpty())
            assertThat(keys.size(), is(a.size()+b.size()));
    }

    /**
     * Basic sanity checks that that this blockchain is indeed a valid blockchain
     * @return true iff this blockchain is a valid blockchain
     */
    public static boolean isValidBlockchain(Blockchain bc) {
        // find ALL leaf nodes
        for(Blockchain.BlockEntry e : bc.getBlocks().values()) {
            if (e.getChildren().isEmpty()) {
                // assert that the path from leaf e to the genesis is a valid blockchain
                assertTrue(isValidFromLeafNode(e, bc));
            }
        }
        // latest block should not be empty and have no children
        List<Blockchain.BlockEntry> latest = bc.getHeightMap().get(bc.getHeightMap().size());
        assertFalse(latest == null || latest.isEmpty());

        for(Blockchain.BlockEntry e : latest) {
            assertTrue(e.getChildren().isEmpty());
        }

        // heightmap should have entries for all heights 1 to heightMap.size()
        for(int i = 1; i <= bc.getHeightMap().size(); i++) {
            List<Blockchain.BlockEntry> hl = bc.getHeightMap().get(i);
            assertFalse(hl == null || hl.isEmpty());
            // confirm that the height of block entries was set correctly
            for(Blockchain.BlockEntry e : hl) {
                assertFalse(e.getHeight() != i);
            }
        }
        return true;
    }

    /**
     * Starting from leaf node e and traversing backwards, checks that each transaction only exists once in the blockchain
     * @param e the leaf entry
     * @return true iff no transaction occurs more than once in the path from e to the genesis block
     */
    public static boolean isValidFromLeafNode(Blockchain.BlockEntry e, Blockchain bc) {
        HashSet<Integer> transactions = new HashSet<>();
        Blockchain.BlockEntry next = e;
        while(next.block.getBlockId() != 0) {
            for(Messages.Transaction t : next.block.getTransactionList()) {
                assertTrue(transactions.add(t.getTxId()));
            }
            next = bc.getBlock(next.block.getParentId());
        }
        return true;
    }

    public static boolean equals(Blockchain a, Blockchain b) {
        Blockchain that = (Blockchain) b;

        if(a.getBlockSize() != that.getBlockSize() || a.getHeightMap().size() != that.getHeightMap().size()
                || a.getBlocks().size() != that.getBlocks().size()
                || a.getOrphans().size() != that.getOrphans().size()) {
            return false;
        }

        for(Map.Entry<Integer, List<Blockchain.BlockEntry>> e : a.getHeightMap().entrySet()) {
            if(!that.getHeightMap().containsKey(e.getKey())) {
                return false;
            }
            var thatList = new LinkedList<>(that.getHeightMap().get(e.getKey()));
            if(e.getValue().size() != thatList.size()) {
                return false;
            }
            for(Blockchain.BlockEntry be : e.getValue()) {
                if(!thatList.removeIf(be::equals))
                    return false;
            }
        }

        for(Map.Entry<Integer, Blockchain.BlockEntry> e : a.getBlocks().entrySet()) {
            if(!e.getValue().equals(that.getBlocks().get(e.getKey()))) {
                return false;
            }
        }

        for(Map.Entry<Integer, List<Block>> e : a.getOrphans().entrySet()) {
            var thatList = that.getOrphans().get(e.getKey());
            if(thatList == null || e.getValue().size() != thatList.size()) {
                return false;
            }
        }

        return true;
    }

    @AfterEach
    void tearDown() {

    }

    @AfterAll
    static void tearDownAll() {
    }
}