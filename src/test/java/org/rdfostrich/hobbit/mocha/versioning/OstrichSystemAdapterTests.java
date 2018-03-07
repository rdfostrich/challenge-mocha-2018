package org.rdfostrich.hobbit.mocha.versioning;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.query.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link OstrichSystemAdapter}.
 *
 * @author Ruben Taelman (ruben.taelman@ugent.be)
 */
public class OstrichSystemAdapterTests {

    private static final String BASE_DIR = System.getProperty("user.dir") + "/";
    private static final String OSTRICH_STORE_FOLDER = BASE_DIR + "test.ostrich";
    private static final String HOST = "localhost";
    private static final int PORT = 3000;
    private static final String ENDPOINT = "http://" + HOST + ":" + PORT + "/sparql";
    private static final String COMUNICA_HTTP_BIN = BASE_DIR + "comunica/packages/actor-init-sparql/bin/http.js";
    private static final String COMUNICA_CONFIG_OSTRICH = "comunica/packages/actor-init-sparql/config/config-ostrich.json";
    private static final String COMUNICA_CONTEXT = "{ \"sources\": [{ \"type\": \"ostrichFile\", \"value\" : \"" + OSTRICH_STORE_FOLDER + "\" }]}";

    @Before
    public void beforeEach() throws IOException {
        if (new File(OSTRICH_STORE_FOLDER).isDirectory()) {
            FileUtils.deleteDirectory(new File(OSTRICH_STORE_FOLDER));
        }
    }

    @After
    public void afterEach() throws IOException {
        FileUtils.deleteDirectory(new File(OSTRICH_STORE_FOLDER));
    }

    @Test
    public void testLoadSingleVersion() throws IOException, InterruptedException {
        Pair<Integer, Long> results = OstrichSystemAdapter.loadVersion("ingester/ingest.js", OSTRICH_STORE_FOLDER,
                0, System.getProperty("user.dir") + "/src/test/resources/v0/");
        assertThat(results.getLeft(), is(6));
        assertThat(results.getRight(), not(is(0)));
    }

    @Test
    public void testTwoVersions() throws IOException, InterruptedException {
        Pair<Integer, Long> results0 = OstrichSystemAdapter.loadVersion("ingester/ingest.js", OSTRICH_STORE_FOLDER,
                0, System.getProperty("user.dir") + "/src/test/resources/v0/");
        assertThat(results0.getLeft(), is(6));
        assertThat(results0.getRight(), not(is(0)));

        Pair<Integer, Long> results1 = OstrichSystemAdapter.loadVersion("ingester/ingest.js", OSTRICH_STORE_FOLDER,
                1, System.getProperty("user.dir") + "/src/test/resources/v1/");
        assertThat(results1.getLeft(), is(7));
        assertThat(results1.getRight(), not(is(0)));
    }

    @Test
    public void testInitializeQueryEndpoint() throws IOException, InterruptedException {
        Process p = OstrichSystemAdapter.initializeQueryEndpoint(COMUNICA_HTTP_BIN, COMUNICA_CONTEXT, COMUNICA_CONFIG_OSTRICH);
        p.destroy();
    }

    @Test
    public void testQuery() throws IOException, InterruptedException {
        OstrichSystemAdapter.loadVersion("ingester/ingest.js", OSTRICH_STORE_FOLDER,
                0, System.getProperty("user.dir") + "/src/test/resources/v0/");
        OstrichSystemAdapter.loadVersion("ingester/ingest.js", OSTRICH_STORE_FOLDER,
                1, System.getProperty("user.dir") + "/src/test/resources/v1/");

        Process p = OstrichSystemAdapter.initializeQueryEndpoint(COMUNICA_HTTP_BIN, COMUNICA_CONTEXT, COMUNICA_CONFIG_OSTRICH);

        Query query = QueryFactory.create("SELECT * WHERE { GRAPH <http://graph.version.0> { ?s ?p ?o } }");
        QueryExecution queryExecution = QueryExecutionFactory.sparqlService(ENDPOINT, query);
        ResultSet results = queryExecution.execSelect();

        assertThat(results.getResultVars().size(), is(3));
        String solutionString = "";
        while (results.hasNext()) {
            QuerySolution solution = results.next();
            Iterator<String> it = solution.varNames();
            while (it.hasNext()) {
                String varName = it.next();
                solutionString += varName + "=" + solution.get(varName) + ";";
            }
        }
        assertThat(solutionString, equalTo("p=a;o=a;s=a;p=a;o=b;s=a;p=a;o=c;s=a;p=a;o=a;s=b;p=a;o=b;s=b;p=a;o=c;s=b;"));
        assertThat(results.getRowNumber(), is(6));

        queryExecution.close();
        p.destroy();
    }

}
