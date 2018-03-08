package org.rdfostrich.hobbit.mocha.versioning;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/**
 * An adapter for invoking queries on a Comunica engine using an OSTRICH store.
 * @author Ruben Taelman (ruben.taelman@ugent.be)
 */
public class OstrichSystemAdapter extends VersionedSystemAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OstrichSystemAdapter.class);

    private static final String BASE_DIR = "/ostrich/";
    private static final String DATA_FOLDER = BASE_DIR + "data/";
    private static final String OSTRICH_STORE_FOLDER = BASE_DIR + "hobbit.ostrich/";
    private static final String OSTRICH_INGESTER = BASE_DIR + "ingester/ingest.js";
    private static final String HOST = "localhost";
    private static final int PORT = 3000;
    private static final String ENDPOINT = "http://" + HOST + ":" + PORT + "/sparql";
    private static final String COMUNICA_HTTP_BIN = BASE_DIR + "comunica/packages/actor-init-sparql/bin/http.js";
    private static final String COMUNICA_CONFIG_OSTRICH = "comunica/packages/actor-init-sparql/config/config-ostrich.json";
    private static final String COMUNICA_CONTEXT = "{ \"sources\": [{ \"type\": \"ostrichFile\", \"value\" : \"" + OSTRICH_STORE_FOLDER + "\" }]}";

    public OstrichSystemAdapter() {
        super(DATA_FOLDER, ENDPOINT);
    }

    @Override
    protected Pair<Integer, Long> loadVersion(int version, String baseDir) throws IOException, InterruptedException {
        return loadVersion(OSTRICH_INGESTER, OSTRICH_STORE_FOLDER, version, baseDir);
    }

    @Override
    protected Process initializeQueryEndpoint() throws IOException, InterruptedException {
        return initializeQueryEndpoint(COMUNICA_HTTP_BIN, COMUNICA_CONTEXT, COMUNICA_CONFIG_OSTRICH);
    }

    public static Pair<Integer, Long> loadVersion(String ingesterBin, String storeFolder, int version, String baseDir)
            throws IOException, InterruptedException {
        String[] command = { "node", ingesterBin, storeFolder, Integer.toString(version), baseDir };
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        Pair<Integer, Long> results = null;
        while ((line = in.readLine()) != null) {
            if (line.contains(",")) {
                String[] split = line.split(",");
                results = Pair.of(Integer.parseInt(split[0]), Long.parseLong(split[1]));
            } else {
                LOGGER.info(line);
            }
        }
        p.waitFor();
        in.close();
        return results;
    }

    public static Process initializeQueryEndpoint(String httpBin, String context, String configFile)
            throws IOException, InterruptedException {
        String[] command = { "node", httpBin, context };
        ProcessBuilder pb = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.environment().put("COMUNICA_CONFIG", configFile);
        Process p = pb.start();
        p.waitFor(10, TimeUnit.MILLISECONDS);
        waitUntilPortIsOpen(100);
        return p;
    }

    protected static void waitUntilPortIsOpen(int attempts) {
        Socket s = null;
        int attempt = 0;
        while (attempt++ < attempts) {
            try {
                s = new Socket(HOST, PORT);
            } catch (IOException e1) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e2) {
                }
            } finally {
                if (s != null) {
                    try {
                        s.close();
                        return;
                    } catch (Exception e) {
                    }
                }
            }
        }
        throw new RuntimeException("Query endpoint was not started in time");
    }
}
