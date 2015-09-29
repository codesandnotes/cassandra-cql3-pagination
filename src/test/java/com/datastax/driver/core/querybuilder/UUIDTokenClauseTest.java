package com.datastax.driver.core.querybuilder;

import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class UUIDTokenClauseTest {

    @Test
    public void generateAQueryStringThatIncludesTokenFunctionsInWhereClause() {

        Select query = QueryBuilder.select().from("keyspace", "table").where(new UUIDTokenClause("id", ">", UUID.randomUUID())).limit(5);

        Assert.assertEquals("select * from keyspace.table where token(id)>token(?) limit 5;", query.getQueryString().toLowerCase());
    }
}
