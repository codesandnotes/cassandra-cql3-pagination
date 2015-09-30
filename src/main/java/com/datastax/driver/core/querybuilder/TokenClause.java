package com.datastax.driver.core.querybuilder;

import java.util.List;

public class TokenClause extends Clause {

    private final String name;
    private final String operation;
    private final Object value;

    public TokenClause(String name, String operation, Object value) {
        super();
        this.name = name;
        this.operation = operation;
        this.value = value;
    }

    @Override
    String name() {
        return name;
    }

    @Override
    Object firstValue() {
        return value;
    }

    @Override
    void appendTo(StringBuilder sb, List<Object> variables) {
        Utils.appendName(QueryBuilder.token(name), sb).append(operation);
        appendValue(value, sb, variables);
    }

    @Override
    boolean containsBindMarker() {
        return Utils.containsBindMarker(value);
    }

    static StringBuilder appendValue(Object value, StringBuilder sb, List<Object> variables) {
        if (variables == null || !Utils.isSerializable(value))
            return Utils.appendValue(value, sb);
        sb.append("token(?)");
        variables.add(value);
        return sb;
    }
}
