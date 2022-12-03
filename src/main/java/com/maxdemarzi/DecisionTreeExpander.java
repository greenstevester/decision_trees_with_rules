package com.maxdemarzi;

import com.maxdemarzi.schema.Labels;
import com.maxdemarzi.schema.RelationshipTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.janino.ExpressionEvaluator;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;

// see https://janino-compiler.github.io/janino/
public class DecisionTreeExpander implements PathExpander {

    private static final Logger log = LogManager.getLogger(DecisionTreeExpander.class);

    private Map<String, String> facts;

    public DecisionTreeExpander(Map<String, String> facts) {
        this.facts = facts;
    }

    @Override
    public Iterable<Relationship> expand(Path path, BranchState branchState) {

        // If we get to an Answer stop traversing, we found a valid path.
        if (path.endNode().hasLabel(Labels.Answer)) {
            return Collections.emptyList();
        }

        // If we have Rules to evaluate, go do that.
        if (path.endNode().hasRelationship(Direction.OUTGOING, RelationshipTypes.HAS)) {
            return path.endNode().getRelationships(Direction.OUTGOING, RelationshipTypes.HAS);
        }

        // when we get to a rule node, check to see if its an end node and if so,
        // perform a boolean operation via isTrue() method to evaluate the expression
        if (path.endNode().hasLabel(Labels.Rule)) {
            try {
                if (isTrue(path.endNode())) {
                    log.info("path endnode:{} IS TRUE", path.endNode());
                    return path.endNode().getRelationships(Direction.OUTGOING, RelationshipTypes.IS_TRUE);
                } else {
                    log.info("path endnode:{} IS FALSE", path.endNode());
                    return path.endNode().getRelationships(Direction.OUTGOING, RelationshipTypes.IS_FALSE);
                }
            } catch (Exception e) {
                // Could not continue this way!
                return Collections.emptyList();
            }
        }

        // Otherwise, not sure what to do really.
        return Collections.emptyList();
    }


    // see https://janino-compiler.github.io/janino/#getting_started
    private boolean isTrue(Node rule) {

        // Get the properties of the rule stored in the node
        Map<String, Object> ruleProperties = rule.getAllProperties();
        String[] parameterNames = Magic.explode((String) ruleProperties.get("parameter_names"));
        Class<?>[] parameterTypes = Magic.stringToTypes((String) ruleProperties.get("parameter_types"));

        // Fill the arguments array with their corresponding values
        Object[] arguments = new Object[parameterNames.length];
        for (int j = 0; j < parameterNames.length; ++j) {
            try {
                arguments[j] = Magic.createObject(parameterTypes[j], facts.get(parameterNames[j]));
            } catch (Exception e) {
                log.error("error occurred while resolving expression from parameters: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }

        ExpressionEvaluator ee = new ExpressionEvaluator();
        ee.setParameters(parameterNames, parameterTypes);
        ee.setExpressionType(boolean.class);
        String expression = (String) ruleProperties.get("expression");

        // And now we "cook" (scan, parse, compile and load) the expression.
        try {
            ee.cook(expression);
        } catch (Exception e) {
            log.error("error occurred while cooking the expression: {}", e.getMessage());
            throw new RuntimeException(e);
        }
        boolean returnValue = false;
        try {
            returnValue = (boolean) ee.evaluate(arguments);
        } catch (InvocationTargetException e) {
            log.error("error occurred while evaluating the result: {}", e.getMessage());
            throw new RuntimeException(e);
        }
        return returnValue;
    }

    @Override
    public PathExpander reverse() {
        return null;
    }
}
