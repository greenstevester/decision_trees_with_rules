package com.maxdemarzi.schema;

import com.maxdemarzi.results.StringResult;
import java.io.IOException;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

public class Schema {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction txn;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @Procedure(name = "com.maxdemarzi.schema.generate", mode = Mode.SCHEMA)
    @Description("CALL com.maxdemarzi.schema.generate() - generate schema")
    public Stream<StringResult> generate() throws IOException {
        org.neo4j.graphdb.schema.Schema schema = txn.schema();
        if (!schema.getIndexes(Labels.Tree).iterator().hasNext()) {
            schema.constraintFor(Labels.Tree).assertPropertyIsUnique("id").create();
        }
        return Stream.of(new StringResult("Schema Generated"));
    }
}
