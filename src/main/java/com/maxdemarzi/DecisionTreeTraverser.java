package com.maxdemarzi;

import com.maxdemarzi.results.PathResult;
import com.maxdemarzi.schema.Labels;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.compress.utils.Lists;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;


public class DecisionTreeTraverser {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @Context
    public Transaction txn;

    // evaluator basically says, when do we stop traversing?
    // when do we accept this path or not accept this path?
    // it says if you get to an answer, then stop - because the path is good, include it - you found the answer.
    // if not, keep going until you find an answer.
    private static final DecisionTreeEvaluator decisionTreeEvaluator = new DecisionTreeEvaluator();   // when do we stop traversing.

    @Procedure(name = "com.maxdemarzi.traverse.decision_tree", mode = Mode.READ)
    @Description("CALL com.maxdemarzi.traverse.decision_tree(tree, facts) - traverse decision tree")
    public Stream<PathResult> traverseDecisionTree(@Name("tree") String id, @Name("facts") Map<String, String> facts) {
        // Which Decision Tree are we interested in?
        String mapAsString = facts.keySet().stream()
                                     .map(key -> key + "=" + facts.get(key))
                                     .collect(Collectors.joining(", ", "{", "}"));
        log.info("id:{} facts:{}", id, mapAsString);
        Node tree = txn.findNode(Labels.Tree, "id", id);
        if (tree != null) {
            log.info("tree:{}", tree.getDegree());
            // Find the paths by traversing this graph and the facts given
            return decisionPath(tree, facts);
        }
        return null;
    }


    private Stream<PathResult> decisionPath(Node tree, Map<String, String> facts) {
        TraversalDescription myTraversal = txn
            .traversalDescription()
            .depthFirst()
            .expand(new DecisionTreeExpander(facts))
            .evaluator(decisionTreeEvaluator);  // created once

        List<PathResult> l = Lists.newArrayList();
        myTraversal
            .traverse(tree)
            .forEach(n -> {
                l.add(new PathResult(n));
            });
        return l.stream();
    }

    @Procedure(name = "com.maxdemarzi.traverse.decision_tree_two", mode = Mode.READ)
    @Description("CALL com.maxdemarzi.traverse.decision_tree_two(tree, facts) - traverse decision tree")
    public Stream<PathResult> traverseDecisionTreeTwo(@Name("tree") String id, @Name("facts") Map<String, String> facts)
        throws IOException {
        // Which Decision Tree are we interested in?
        Node tree = txn.findNode(Labels.Tree, "id", id);
        if (tree != null) {
            // Find the paths by traversing this graph and the facts given
            return decisionPathTwo(tree, facts);
        }
        return null;
    }

    private Stream<PathResult> decisionPathTwo(Node tree, Map<String, String> facts) {
        TraversalDescription myTraversal = txn
            .traversalDescription()
            .depthFirst()
            .expand(new DecisionTreeExpanderTwo(facts, log))
            .evaluator(decisionTreeEvaluator);

        List<PathResult> l = Lists.newArrayList();
        myTraversal
            .traverse(tree)
            .forEach(n -> {
                l.add(new PathResult(n));
            });
        return l.stream();
    }
}
