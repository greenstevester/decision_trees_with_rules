package com.maxdemarzi;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;


// see // https://neo4j.com/docs/java-reference/current/java-embedded/
// see https://github.com/neo4j/neo4j-documentation/blob/4.4/embedded-examples/src/main/java/org/neo4j/examples/EmbeddedNeo4j.java
// see https://neo4j.com/docs/java-reference/current/java-embedded/bolt/
// see https://stackoverflow.com/questions/38665465/deploy-a-procedure-to-neo4j-when-using-the-embedded-driver
// see https://stackoverflow.com/questions/43965481/how-to-configure-neo4j-embedded-to-run-apoc-procedures

public class EmbeddedNeo4jTest {

    private static final Logger log = LogManager.getLogger(EmbeddedNeo4jTest.class);

    public static final String MATCH_N_DETACH_DELETE_N = "MATCH (n) DETACH DELETE n";
    public static final String END_NODE = "4]->(4)}";
    public static final String END_NODE_3 = "3]->(3)}";
    private DatabaseManagementService managementService;
    private GraphDatabaseService graphDb;
    private static final Path databaseDirectory = Path.of( "target/neo4j-test-db" );

    private static final String MODEL_STATEMENT =
        "CREATE (tree:Tree { id: 'bar entrance' })" +
                "CREATE (over21_rule:Rule { parameter_names: 'age', parameter_types: 'int',  expression: 'age >= 21' })" +
                "CREATE (gender_rule:Rule { parameter_names: 'age,gender', parameter_types: 'int,String', expression:'(age >= 18) && gender.equals(\"female\")' })" +
//                "CREATE (dress_code_rule:Rule { parameter_names: 'dress_code', parameter_types: 'String', expression:'dress_code.equals(\"black\")' })" +
                "CREATE (answer_yes:Answer { id: 'yes'})" +
                "CREATE (answer_no:Answer { id: 'no'})" +
                "CREATE (tree)-[:HAS]->(over21_rule)" +
                "CREATE (over21_rule)-[:IS_TRUE]->(answer_yes)" +
                "CREATE (over21_rule)-[:IS_FALSE]->(gender_rule)" +
                "CREATE (gender_rule)-[:IS_TRUE]->(answer_yes)" +
                "CREATE (gender_rule)-[:IS_FALSE]->(answer_no)" ;
//                "CREATE (dress_code_rule)-[:IS_TRUE]->(answer_yes)" +
//                "CREATE (dress_code_rule)-[:IS_FALSE]->(answer_no)";

    private static final String QUERY1 =
                "CALL com.maxdemarzi.traverse.decision_tree('bar entrance', {gender:'male', age:'20'}) yield path return path";

    private static final String QUERY2 =
                "CALL com.maxdemarzi.traverse.decision_tree('bar entrance', {gender:'female', age:'18'}) yield path return path";

    private static final String QUERY3 =
            "CALL com.maxdemarzi.traverse.decision_tree('bar entrance', {gender:'female', age:'17'}) yield path return path";

//    private static final String QUERY4 =
//            "CALL com.maxdemarzi.traverse.decision_tree('bar entrance', {dress_code:'white'}) yield path return path";

    @BeforeEach
    public void setup() throws IOException, KernelException {
        log.info("startup - creating DB connection...");
        FileUtils.deleteDirectory(databaseDirectory.toFile());
        managementService = new DatabaseManagementServiceBuilder( databaseDirectory ).build();
        graphDb = managementService.database( DEFAULT_DATABASE_NAME );
        log.info("startup - creating DB connection...DONE");

        log.info("startup - registering procedure...");
        GraphDatabaseAPI graphDatabaseAPI = (GraphDatabaseAPI) graphDb;
        GlobalProcedures gp = graphDatabaseAPI
                .getDependencyResolver()
                .resolveDependency(org.neo4j.kernel.api.procedure.GlobalProcedures.class);
        gp.registerProcedure(DecisionTreeTraverser.class);
        log.info("startup - registering procedure...DONE");

        log.info("startup - cleaning db and adding decision model...");
        graphDb.executeTransactionally(MATCH_N_DETACH_DELETE_N);
        graphDb.executeTransactionally(MODEL_STATEMENT);
        log.info("startup - cleaning db and adding decision model...DONE");
    }

    @AfterEach
    public void tearDown() {
        log.info("Shutting down database..." );
        managementService.shutdown();
        log.info("Shutting down database...DONE" );
    }

    @Test
    void testMaleUnder21() {
        log.info("testMaleUnder21..." );
        try ( Transaction tx = graphDb.beginTx() )
        {
            Result result = tx.execute(QUERY1);
            Optional<Map<String, Object>> x = result.stream().findFirst();
            if (x.isPresent()) {
                String joined = x.stream()
                                        .map(Object::toString)
                                        .collect(Collectors.joining(" "));
                log.info("value: {}", joined );
                assert(joined.endsWith("[IS_FALSE," + END_NODE));
            }
        }
        log.info("testMaleUnder21...DONE" );
    }

    @Test
    void testFemaleOver18() {
        log.info("testFemaleOver18..." );
        try ( Transaction tx = graphDb.beginTx() )
        {
            Result result = tx.execute(QUERY2);
            Optional<Map<String, Object>> x = result.stream().findFirst();
            if (x.isPresent()) {
                String joined = x.stream()
                                        .map(Object::toString)
                                        .collect(Collectors.joining(" "));
                log.info("value: {}", joined );
                assert(joined.endsWith("[IS_TRUE," + END_NODE_3));
            }
        }
        log.info("testFemaleOver18...DONE" );
    }

    @Test
    void testFemaleUnder18() {
        log.info("testFemaleUnder18..." );
        try ( Transaction tx = graphDb.beginTx() )
        {
            Result result = tx.execute(QUERY3);
            Optional<Map<String, Object>> x = result.stream().findFirst();
            if (x.isPresent()) {
                String joined = x.stream()
                                        .map(Object::toString)
                                        .collect(Collectors.joining(" "));
                log.info("value: {}", joined );
                assert(joined.endsWith("[IS_FALSE," + END_NODE));
            }
        }
        log.info("testFemaleUnder18...DONE" );
    }

//    @Test
//    void testDressCode() {
//        log.info("testDressCode..." );
//        try ( Transaction tx = graphDb.beginTx() )
//        {
//            Result result = tx.execute(QUERY4);
//            Optional<Map<String, Object>> x = result.stream().findFirst();
//            if (x.isPresent()) {
//                String joined = x.stream()
//                                        .map(Object::toString)
//                                        .collect(Collectors.joining(" "));
//                log.info("value: {}", joined );
//                assert(joined.endsWith("[IS_FALSE," + END_NODE));
//            }
//        }
//        log.info("testDressCode...DONE" );
//    }

}
