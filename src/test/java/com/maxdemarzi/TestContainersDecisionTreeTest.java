package com.maxdemarzi;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

// see https://www.testcontainers.org/modules/databases/neo4j/

@Slf4j
@Testcontainers
public class TestContainersDecisionTreeTest {

    private static final List<String> PACKAGES_TO_SCAN = List.of(
            "com.maxdemarzi"
    );

    private Reflections createReflections(String pkg) {
        return new Reflections(
                pkg,
                new MethodAnnotationsScanner()
        );
    }

    private static final String MODEL_STATEMENT =
            "CREATE (tree:Tree { id: 'bar entrance' })" +
                    "CREATE (over21_rule:Rule { parameter_names: 'age', parameter_types:'int', expression:'age >= 21' })" +
                    "CREATE (gender_rule:Rule { parameter_names: 'age,gender', parameter_types:'int,String', expression:'(age >= 18) && gender.equals(\"female\")' })" +
                    "CREATE (answer_yes:Answer { id: 'yes'})" +
                    "CREATE (answer_no:Answer { id: 'no'})" +
                    "CREATE (tree)-[:HAS]->(over21_rule)" +
                    "CREATE (over21_rule)-[:IS_TRUE]->(answer_yes)" +
                    "CREATE (over21_rule)-[:IS_FALSE]->(gender_rule)" +
                    "CREATE (gender_rule)-[:IS_TRUE]->(answer_yes)" +
                    "CREATE (gender_rule)-[:IS_FALSE]->(answer_no)";

    @Container
    private static Neo4jContainer<?> neo4jContainer =
            new Neo4jContainer<>(DockerImageName.parse("neo4j:4.4"))
                    .withNeo4jConfig("dbms.security.procedures.unrestricted", "com.maxdemarzi.*")
                    .withAdminPassword(null); // Disable password

    @BeforeEach
    public void setup() {
        log.info("startup - creating DB connection");

        String boltUrl = neo4jContainer.getBoltUrl();
        try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.none()); Session session = driver.session()) {
            session.writeTransaction(tx -> {
               tx.run("MATCH (n) DETACH DELETE n");
               tx.run(MODEL_STATEMENT);
               return null;
            });
        }
    }

    @Test
    void testSomethingUsingBolt() {
        // Retrieve the Bolt URL from the container
        String boltUrl = neo4jContainer.getBoltUrl();
        try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.none()); Session session = driver.session()) {
            long one = session.run("CALL com.maxdemarzi.traverse.decision_tree('bar entrance', {gender:'male', age:'20'}) yield path return path", Collections.emptyMap()).next().get(0).asLong();
            assertEquals(one, 1L);
        }
    }
}
