package com.datastax.driver.core.querybuilder;

import java.util.List;
import java.util.UUID;

public class UUIDTokenClause extends Clause {

    private final String name;
    private final String operation;
    private final UUID value;

    public UUIDTokenClause(String name, String operation, UUID value) {
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
