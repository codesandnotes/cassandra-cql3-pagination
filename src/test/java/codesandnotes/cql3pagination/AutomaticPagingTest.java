package codesandnotes.cql3pagination;

import codesandnotes.Integration;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class AutomaticPagingTest extends Integration {

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
    public void pageQueriedResults() {
        Statement query = new SimpleStatement("select * from cql3_pagination.simple_uuid_pk");
        query.setFetchSize(5);
        ResultSet resultSet = embeddedCassandra.session().execute(query);

        List<Row> page1 = new ArrayList<>();
        Iterator<Row> iterator1 = resultSet.iterator();
        while (resultSet.getAvailableWithoutFetching() > 0) {
            page1.add(iterator1.next());
        }
        print("Page 1: ", page1);

        Assert.assertEquals(5, page1.size());

        List<Row> page2 = new ArrayList<>();
        Iterator<Row> iterator2 = resultSet.iterator();
        resultSet.fetchMoreResults();
        iterator2.hasNext();
        while (resultSet.getAvailableWithoutFetching() > 0) {
            page2.add(iterator2.next());
        }
        print("Page 2: ", page2);

        Assert.assertEquals(5, page2.size());

        Assert.assertNotEquals(page1.get(0).getUUID("id"), page2.get(0).getUUID("id"));
    }

    private void print(String header, List<Row> rows) {
        System.out.println(header);
        for (Row row : rows) {
            System.out.println(row.getUUID("id") + " | " + row.getString("value"));
        }
    }
}
