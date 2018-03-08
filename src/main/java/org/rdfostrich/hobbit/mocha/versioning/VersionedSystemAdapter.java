package org.rdfostrich.hobbit.mocha.versioning;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.query.*;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An adapter for invoking queries on a versioned SPARQL endpoint.
 * @author Ruben Taelman (ruben.taelman@ugent.be)
 */
public abstract class VersionedSystemAdapter extends AbstractSystemAdapter {

    /**
     * The signal sent by the benchmarked system to indicate that it
     * has finished with a phase of bulk loading.
     */
    public static final byte BULK_LOADING_DATA_FINISHED = (byte) 150;
    /**
     * The signal sent by the benchmark controller to indicate that all
     * data has successfully sent by the data generators
     */
    public static final byte BULK_LOAD_DATA_GEN_FINISHED = (byte) 151;

    private static final Logger LOGGER = LoggerFactory.getLogger(VersionedSystemAdapter.class);

    private final AtomicInteger dataReceived = new AtomicInteger(0);
    private final AtomicInteger dataSent = new AtomicInteger(0);
    private final Semaphore dataReceive = new Semaphore(0);

    private final String tempDataFolder;
    private final String endpoint;

    private boolean dataLoadingFinished = false;
    private int currentVersion = 0;
    private Process endpointProcess = null;

    public VersionedSystemAdapter(String tempDataFolder, String endpoint) {
        this.tempDataFolder = tempDataFolder;
        this.endpoint = endpoint;
    }

    @Override
    public void init() throws Exception {
        LOGGER.info("Initializing Versioned System Adapter...");
        try {
            super.init();
        } catch (Throwable e) {
            LOGGER.error("Error while initializing: " + Lists.newArrayList(e.getStackTrace()));
            throw e;
        }

        // Initialize file storage
        new File(tempDataFolder).mkdir();

        LOGGER.info("Versioned System Adapter initialized successfully.");
    }

    @Override
    public void receiveGeneratedData(byte[] data) {
        // Copy the dataset locally, so it can be ingested during the task
        ByteBuffer dataBuffer = ByteBuffer.wrap(data);
        String fileName = RabbitMQUtils.readString(dataBuffer);

        byte[] dataContentBytes = new byte[dataBuffer.remaining()];
        dataBuffer.get(dataContentBytes, 0, dataBuffer.remaining());

        if (dataContentBytes.length != 0) {
            try {
                LOGGER.info("Received data for " + fileName);
                if (fileName.contains("/")) {
                    fileName = fileName.replaceAll("[^/]*[/]", "");
                }
                FileOutputStream fos = new FileOutputStream(tempDataFolder + File.separator + fileName);
                IOUtils.write(dataContentBytes, fos);
                fos.close();
            } catch (FileNotFoundException e) {
                LOGGER.error("Exception while creating/opening files to write received data.", e);
            } catch (IOException e) {
                LOGGER.error("Exception while writing data file", e);
            }
        } else {
            LOGGER.error("The received data was empty");
        }

        if (dataReceived.incrementAndGet() == dataSent.get()) {
            dataReceive.release();
        }
    }

    @Override
    public void receiveGeneratedTask(String taskId, byte[] data) {
        if(dataLoadingFinished) {
            LOGGER.info("Task " + taskId + " received from task generator");

            if (this.endpointProcess == null) {
                // Start query endpoint
                LOGGER.info("Query endpoint was not running yet, starting...");
                try {
                    this.endpointProcess = initializeQueryEndpoint();
                } catch (IOException | InterruptedException e) {
                    LOGGER.error("Could not start query endpoint.", e);
                }
            }

            // read the query
            ByteBuffer buffer = ByteBuffer.wrap(data);
            String queryText = RabbitMQUtils.readString(buffer);

            Query query = QueryFactory.create(queryText);
            QueryExecution qexec = QueryExecutionFactory.sparqlService(endpoint, query);
            ResultSet rs = null;

            try {
                rs = qexec.execSelect();
            } catch (Exception e) {
                LOGGER.error("Task " + taskId + " failed to execute.", e);
            }

            ByteArrayOutputStream queryResponseBos = new ByteArrayOutputStream();
            ResultSetFormatter.outputAsJSON(queryResponseBos, rs);
            byte[] results = queryResponseBos.toByteArray();
            LOGGER.info("Task " + taskId + " executed successfully.");
            LOGGER.info("Results: " + rs.getRowNumber());
            qexec.close();

            try {
                sendResultToEvalStorage(taskId, results);
                LOGGER.info("Results sent to evaluation storage.");
            } catch (IOException e) {
                LOGGER.error("Exception while sending storage space cost to evaluation storage.", e);
            }
        } else {
            LOGGER.error("Received a task before ingesting was finished!");
        }
    }

    @Override
    public void receiveCommand(byte command, byte[] data) {
        if (BULK_LOAD_DATA_GEN_FINISHED == command) {
            LOGGER.info("Received signal from Data Generator that data generation finished.");
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int numberOfMessages = buffer.getInt();
            boolean lastLoadingPhase = buffer.get() != 0;
            LOGGER.info("Received signal that all data of version " + currentVersion + " were successfully sent from all data generators (#" + numberOfMessages + ")");

            // if all data have been received before BULK_LOAD_DATA_GEN_FINISHED command received
            // release before acquire, so it can immediately proceed to bulk loading
            if(dataReceived.get() == dataSent.addAndGet(numberOfMessages)) {
                dataReceive.release();
            }

            LOGGER.info("Wait for receiving all data of version " + currentVersion + ".");
            try {
                dataReceive.acquire();
            } catch (InterruptedException e) {
                LOGGER.error("Exception while waiting for all data of version " + currentVersion + " to be received.", e);
            }

            LOGGER.info("All data of version " + currentVersion + " received. Proceed to the loading of such version.");
            try {
                loadVersion(currentVersion, tempDataFolder);
                LOGGER.info("Successfully loaded version " + currentVersion + ".");
            } catch (IOException | InterruptedException e) {
                LOGGER.error("Error while loading version " + currentVersion + ".", e);
            }

            LOGGER.info("Send signal to Benchmark Controller that all data of version " + currentVersion + " successfully loaded.");
            try {
                sendToCmdQueue(BULK_LOADING_DATA_FINISHED);
            } catch (IOException e) {
                LOGGER.error("Exception while sending signal that all data of version " + currentVersion + " successfully loaded.", e);
            }

            // Cleanup temp files
            File theDir = new File(tempDataFolder);
            for (File f : theDir.listFiles()) {
                f.delete();
            }

            currentVersion++;
            dataLoadingFinished = lastLoadingPhase;
        }
        super.receiveCommand(command, data);
    }

    protected abstract Pair<Integer, Long> loadVersion(int version, String baseDir) throws IOException, InterruptedException;

    protected abstract Process initializeQueryEndpoint() throws IOException, InterruptedException;

    @Override
    public void close() throws IOException {
        LOGGER.info("Closing Versioned System Adapter...");
        endpointProcess.destroy();
        super.close();
        LOGGER.info("Versioned System Adapter closed successfully.");

    }
}
