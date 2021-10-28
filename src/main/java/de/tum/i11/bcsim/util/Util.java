package de.tum.i11.bcsim.util;

import com.google.protobuf.Timestamp;
import de.tum.i11.bcsim.config.ConfigYAML;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Util {
    public static byte[] rndBytes(int length) {
        byte[] bytes = new byte[length];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    public static int getRndInt(int fromIncl, int toExl) {
        return ThreadLocalRandom.current().nextInt(fromIncl, toExl);
    }

    public static int rndInt(int max, Integer ex) {
//        assert 0 <= max && (0 != ex || max != ex);
        Integer ret;
        do {
            ret = ThreadLocalRandom.current().nextInt(0, max);
        }while(ret.equals(ex));
        return  ret;
    }

    public static int rndInt(int max) {
        return rndInt(max, null);
    }

    public static Collection<Integer> getUniqueRndInts(int fromIncl, int toExl, int n) {
        if(n > toExl-fromIncl)
            throw new IllegalArgumentException("Cannot create "+n+" unique elements from given interval.");
        HashSet<Integer> s = new HashSet<>(n);
        while(s.size() < n) {
            s.add(getRndInt(fromIncl, toExl));
        }
        return s;
    }

    public static int getFee(List<ConfigYAML.TxFee> fees) {
        int totalShare = fees.stream().mapToInt(f -> f.share).sum();
        if(totalShare <= 0) {
            return fees.get(getRndInt(0, fees.size())).fee;
        }
        int selection = getRndInt(0, totalShare);
        int count = 0;
        for (ConfigYAML.TxFee fee : fees) {
            count += fee.share;
            if(selection < count) {
                return fee.fee;
            }
        }
        return fees.get(fees.size()-1).fee;
    }

    public static Timestamp getTimestamp() {
        long millis = System.currentTimeMillis();

        return Timestamp.newBuilder().setSeconds(millis / 1000)
                .setNanos((int) ((millis % 1000) * 1000000)).build();
    }

    public static int getIdInRound(List<Integer> ids, long roundLength) {
        return getIdInRound(ids, roundLength, -1);
    }

    static long seed = 237942183808L;
    public static int getIdInRound(List<Integer> ids, long roundLength, int last) {
        // long lastRoundStart = (System.currentTimeMillis()/roundLength)*roundLength;
        Random r = new Random(seed);
        int chosen;
        do {
            chosen = ids.get(r.nextInt(ids.size()));
        } while(chosen == last);
        return chosen;
    }

    public static long getMillisecondsToNextRoundStartAt(long roundDuration, long startTime) {
        return roundDuration - startTime%roundDuration;
    }

    public static double nextGaussian(double mean){
        return Math.max(0, nextGaussian(mean, 0.2*mean));
    }

    public static double nextGaussian(double mean, double stdDeviation){
        return ThreadLocalRandom.current().nextGaussian()*stdDeviation+mean;
    }

    public static double nextExponential(double mean) {
        double lambda = 1/mean;
        return Math.log(1-ThreadLocalRandom.current().nextDouble())/-lambda;
    }

    public static double nextUniform() {
        return ThreadLocalRandom.current().nextDouble();
    }

    public static int getID(int creator, int count) {
//        return ThreadLocalRandom.current().nextInt();
        return creator*1000000+count;
    }
}
