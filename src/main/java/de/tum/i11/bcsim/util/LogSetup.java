package de.tum.i11.bcsim.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.logging.*;

public abstract class LogSetup {

    public static class NoShutdownLogManager extends LogManager {
        static NoShutdownLogManager _this;

        public NoShutdownLogManager() {
            _this = this;
        }

        @Override
        public void reset() {
        }

        private void reset0() {
            super.reset();
        }

        public static void resetFinally() {
            _this.reset0();
        }
    }

    public static void setupLogging(Path logfile, Level loglevel) {

        final InputStream cfgStream = LogSetup.class.getResourceAsStream("/logging.properties");
        try {
            LogManager.getLogManager().readConfiguration(cfgStream);
            cfgStream.close();
        } catch (final IOException e) {
            Logger.getAnonymousLogger().severe("Could not load default /logging.properties file");
            Logger.getAnonymousLogger().severe(e.getMessage());
        }


        Logger logger = LogManager.getLogManager().getLogger("");
        String fmt = LogManager.getLogManager().getProperty("java.util.logging.SimpleFormatter.format");
        System.setProperty("java.util.logging.SimpleFormatter.format",
                (fmt != null) ? fmt : "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s [%3$s] %5$s %6$s%n");

        FileHandler fileHandler;
        try {
            String filename;
            if (logfile == null || logfile.getFileName() == null) {
                filename = "onion.log";
            } else {
                filename = logfile.getFileName().toString();
            }
            fileHandler = new FileHandler(filename, true);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        fileHandler.setFormatter(new SimpleFormatter());
        logger.addHandler(fileHandler);


        if (loglevel != null) {
            logger.setLevel(loglevel);
        } else {
            // usually the current logger uses the value set in logging.properties, so apply it to all handlers
            // determine fallback if no log level is set
            for (Logger tmp = logger; loglevel == null & tmp != null; tmp = tmp.getParent()) {
              loglevel = tmp.getLevel();
            }
            if (loglevel == null) {
              loglevel = Level.parse("FINEST");
                System.out.println("No default log level set, using FINEST!");
            }
        }

        for (Handler h : logger.getHandlers()) {
            h.setLevel(loglevel);
        }
    }
}
