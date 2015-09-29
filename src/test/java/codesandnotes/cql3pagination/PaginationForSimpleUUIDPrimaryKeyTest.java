package codesandnotes.cql3pagination;

import codesandnotes.Integration;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.UUIDTokenClause;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

public class PaginationForSimpleUUIDPrimaryKeyTest extends Integration {

    public static final String TABLE = "simple_uuid_pk";

    @Before
    public void _init() {
        embeddedCassandra.loadScripts("db/simple_uuid_pk.dml.cql");
    }

    @After
    public void _cleanup() {
        embeddedCassandra.clearTables();
    }

    @Test
    public void retrieve_5_ResultsOfPage_1_InAListOf_10() {
        List<Row> page1 = embeddedCassandra.session().execute(
                QueryBuilder.select().from(CASSANDRA_KEYSPACE, TABLE).limit(5)
        ).all();
        print(page1);

        Assert.assertEquals(5, page1.size());
    }

    @Test
    public void retrieve_5_ResultsOfPage_2_InAListOf_10_UsingCQL() {
        List<Row> page1 = embeddedCassandra.session().execute(
                QueryBuilder.select().from(CASSANDRA_KEYSPACE, TABLE).limit(5)
        ).all();
        print(page1);

        UUID lastUUIDInPage1 = page1.get(4).getUUID("id");

        List<Row> page2 = embeddedCassandra.session().execute(
                "select * from cql3_pagination.simple_uuid_pk where token(id) > token(" + lastUUIDInPage1 + ")"
        ).all();
        print(page2);

        Assert.assertEquals(5, page2.size());
        Assert.assertNotEquals(page2.get(0).getUUID("id"), page1.get(0).getUUID("id"));
    }

    @Test
    public void retrieve_5_ResultsOfPage_2_InAListOf_10_UsingQueryBuilder() {
        List<Row> page1 = embeddedCassandra.session().execute(
                QueryBuilder.select().from(CASSANDRA_KEYSPACE, TABLE).limit(5)
        ).all();
        print(page1);

        UUID lastUUIDInPage1 = page1.get(4).getUUID("id");

        Select query = QueryBuilder.select().from(CASSANDRA_KEYSPACE, TABLE)
                .where(new UUIDTokenClause("id", ">", lastUUIDInPage1)).limit(5);
        List<Row> page2 = embeddedCassandra.session().execute(
                query
        ).all();
        print(page2);

        Assert.assertEquals(5, page2.size());
        Assert.assertNotEquals(page2.get(0).getUUID("id"), page1.get(0).getUUID("id"));
    }

    private void print(List<Row> rows) {
        for (Row row : rows) {
            System.out.println(row.getUUID("id") + " | " + row.getString("value"));
        }
    }
}
