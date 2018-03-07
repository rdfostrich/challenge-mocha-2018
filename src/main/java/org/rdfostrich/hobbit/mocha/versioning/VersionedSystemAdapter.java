package org.rdfostrich.hobbit.mocha.versioning;

import org.apache.commons.io.FileUtils;
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

    public static final int TASK_INGEST = 1;
    public static final int TASK_STORAGE_SPACE = 2;
    public static final int TASK_QUERY = 3;

    private static final Logger LOGGER = LoggerFactory.getLogger(VersionedSystemAdapter.class);

    private final AtomicInteger dataReceived = new AtomicInteger(0);
    private final AtomicInteger dataSent = new AtomicInteger(0);
    private final Semaphore dataReceive = new Semaphore(0);

    private final String tempDataFolder;
    private final String storageFolder;
    private final String endpoint;

    private boolean dataReceivingFinished = false;
    private boolean dataLoadingFinished = false;
    private int currentVersion = 0;
    private Process endpointProcess = null;

    public VersionedSystemAdapter(String tempDataFolder, String storageFolder, String endpoint) {
        this.tempDataFolder = tempDataFolder;
        this.storageFolder = storageFolder;
        this.endpoint = endpoint;
    }

    @Override
    public void init() throws Exception {
        LOGGER.info("Initializing Versioned System Adapter...");
        super.init();

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
        if (dataReceivingFinished) {
            LOGGER.info("Task " + taskId + " received from task generator");

            ByteBuffer taskBuffer = ByteBuffer.wrap(data);
            // read the query type
            String taskType = RabbitMQUtils.readString(taskBuffer);
            // read the query
            String queryText = RabbitMQUtils.readString(taskBuffer);

            int task = Integer.parseInt(taskType);
            byte[][] resultsArray;
            try {
                switch (task) {
                    case TASK_INGEST:
                        resultsArray = this.taskIngest(taskType, queryText);
                        break;
                    case TASK_STORAGE_SPACE:
                        resultsArray = this.taskStorageSpace(taskType);
                        endpointProcess = this.initializeQueryEndpoint();
                        break;
                    case TASK_QUERY:
                        resultsArray = this.taskQuery(taskType, queryText);
                        break;
                    default:
                        throw new RuntimeException("Received invalid task " + taskId);
                }

                try {
                    sendResultToEvalStorage(taskId, RabbitMQUtils.writeByteArrays(resultsArray));
                    LOGGER.info("Results sent to evaluation storage.");
                } catch (IOException e) {
                    LOGGER.error("Exception while sending storage space cost to evaluation storage.", e);
                }
            } catch (Exception e) {
                LOGGER.error("Task " + taskId + " failed to execute.", e);
            }
            LOGGER.info("Task " + taskId + " executed successfully.");
        } else {
            LOGGER.error("Received a task before ingesting was finished!");
        }
    }

    protected byte[][] taskIngest(String taskType, String queryText) {
        LOGGER.info("Task: Ingest");

        int version = Integer.parseInt(queryText.substring(8, queryText.indexOf(",")));
        currentVersion++;
        LOGGER.info("Ingesting version " + version);

        LOGGER.info("Loading version " + version + " (" + queryText + ")...");
        byte[][] resultsArray = new byte[3][];
        try {
            Pair<Integer, Long> results = loadVersion(version, queryText);
            LOGGER.info("Version " + version + " loaded successfully.");

            resultsArray[0] = RabbitMQUtils.writeString(taskType);
            resultsArray[1] = RabbitMQUtils.writeString(Integer.toString(results.getLeft()));
            resultsArray[2] = RabbitMQUtils.writeString(Long.toString(results.getRight()));

        } catch (IOException | InterruptedException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
        }
        return resultsArray;
    }

    protected byte[][] taskStorageSpace(String taskType) {
        LOGGER.info("Task: Storage Space");

        byte[][] resultsArray = new byte[2][];
        long finalDatabasesSize = FileUtils.sizeOfDirectory(new File(storageFolder));
        LOGGER.info("Total datasets size: "+ finalDatabasesSize / 1000f + " KB.");

        resultsArray = new byte[2][];
        resultsArray[0] = RabbitMQUtils.writeString(taskType);
        resultsArray[1] = RabbitMQUtils.writeString(Long.toString(finalDatabasesSize));
        return resultsArray;
    }

    protected byte[][] taskQuery(String taskType, String queryText) {
        LOGGER.info("Task: Query");

        if (!dataLoadingFinished) {
            LOGGER.error("Tried querying before data was ingested.");
        }
        String queryType = queryText.substring(21, 22);
        LOGGER.info("queryType: " + queryType);
        Query query = QueryFactory.create(queryText);
        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(endpoint, query);
        ResultSet results = queryExecution.execSelect();

        ByteArrayOutputStream queryResponseBos = new ByteArrayOutputStream();
        ResultSetFormatter.outputAsJSON(queryResponseBos, results);

        int resultSize = results.getRowNumber();
        queryExecution.close();

        byte[][] resultsArray = new byte[4][];
        resultsArray[0] = RabbitMQUtils.writeString(taskType);
        resultsArray[1] = RabbitMQUtils.writeString(queryType);
        resultsArray[2] = RabbitMQUtils.writeString(Integer.toString(resultSize));
        resultsArray[3] = queryResponseBos.toByteArray();
        LOGGER.info("results: " + resultSize);
        return resultsArray;
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
            if (dataReceived.incrementAndGet() == dataSent.get()) {
                dataReceive.release();
            }

            LOGGER.info("Wait for receiving all data of version " + currentVersion + ".");
            try {
                dataReceive.acquire();
            } catch (InterruptedException e) {
                LOGGER.error("Exception while waiting for all data of version " + currentVersion + " to be received.", e);
            }

            LOGGER.info("Send signal to Benchmark Controller that all data of version " + currentVersion + " successfully loaded.");
            try {
                sendToCmdQueue(BULK_LOADING_DATA_FINISHED);
            } catch (IOException e) {
                LOGGER.error("Exception while sending signal that all data of version " + currentVersion + " successfully loaded.", e);
            }

            currentVersion++;
            dataReceivingFinished = lastLoadingPhase;
        } else if(BULK_LOADING_DATA_FINISHED == command) {
            LOGGER.info("Received signal that all generated data loaded successfully.");
            dataLoadingFinished = true;
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
