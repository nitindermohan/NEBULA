package de.tum.i11.bcsim.main;

import de.tum.i11.bcsim.coordinator.BlockchainCoordinator;
import de.tum.i11.bcsim.coordinator.DPoSCoordinator;
import de.tum.i11.bcsim.coordinator.ProofBasedCoordinator;
import de.tum.i11.bcsim.config.Config;
import de.tum.i11.bcsim.util.LogSetup;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class Main {

    static {
        System.setProperty("java.util.logging.manager", LogSetup.NoShutdownLogManager.class.getName());
    }

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException {
        if(args != null && args.length >= 1 && args[0].contains("-h")) {
            System.out.println("java -jar -Xms<initial mem size m/g> -Xmx<max mem size m/g> " +
                    "bcsim.jar <this coordinator's IP> <this coordinator's port> <path/to/config/folder>");
            return;
        }

        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd-HHmmssSSS");
        Logger.getLogger("io.netty").setLevel(Level.OFF);
        Logger.getLogger("org.hibernate").setLevel(Level.OFF);

        BlockchainCoordinator[] co = new BlockchainCoordinator[1];
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down ...");
            if(co[0] != null) {
                co[0].stop(null, "Main", "Ctrl+C", true);
                co[0].closedFuture().join();
                LOGGER.info("Done");
            }
        }));
        assert args != null;
        InetSocketAddress addr = new InetSocketAddress(args[0], Integer.parseInt(args[1]));

        ArrayList<Config> configs = new ArrayList<>();

        if(args.length >= 3) {
            File configDir = new File(args[2]);
            if(configDir.isDirectory()) {
                Arrays.stream(Objects.requireNonNull(configDir.listFiles())).filter(f -> f.getName().contains(".yaml"))
                        .sorted(Comparator.comparing(File::getName)).forEach(f -> {
                    try {
                        configs.add(new Config(f.getAbsolutePath()));
                    } catch (IOException e) {
                        System.err.println(e);
                        System.exit(1);
                    }
                });
            } else {
                configs.add(new Config(configDir.getAbsolutePath()));
            }
        } else {
            configs.add(new Config("../src/config/config.yaml"));
        }

        LogSetup.setupLogging(Path.of("bcsim.log"), configs.get(0).getLogLevel());

        for(int con = 0; con < configs.size(); con++) {
            Config c = configs.get(con);
            for(int run = 1; run <= c.getRuns(); run++) {
                c.prefix = df.format(new Date())+"_"+c.getFileName().split("\\.")[0]+"_"+run;

                LOGGER.info("Starting run "+run+" on "+addr+" with config: " + c);

                switch(c.getBlockchainType()) {
                    case "proofBased": {
                        co[0] = new ProofBasedCoordinator(addr, c);
                        break;
                    }
                    case "dPoSBFT": {
                        co[0] = new DPoSCoordinator(addr, c);
                        break;
                    }
                    default: System.exit(1);
                }

                if(c.getManualStart()) {
                    Scanner sc = new Scanner(System.in);
                    if (sc.nextLine().startsWith("start")) {
                        co[0].startAsOrchestrator();
                    }
                    sc.close();
                } else if(addr.equals(c.getCoordinatorAddresses().get(0).address)) {
                    try {
                        Thread.sleep(c.getNetworkDelay());
                    } catch (InterruptedException ignored) {}
                    co[0].startAsOrchestrator();
                }

                if(run == c.getRuns() && con == configs.size()-1) {
                    co[0].closedFuture().thenRun(() -> System.exit(0));
                } else if((Boolean) co[0].closedFuture().join()){
                    System.exit(1);
                }
                System.gc();
            }
        }
    }
}
