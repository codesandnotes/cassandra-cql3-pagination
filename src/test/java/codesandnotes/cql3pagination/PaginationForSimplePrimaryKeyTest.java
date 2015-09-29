package codesandnotes.cql3pagination;

import codesandnotes.Integration;
import org.junit.After;
import org.junit.Test;

public class PaginationForSimplePrimaryKeyTest extends Integration {

    @After
    public void _cleanup() {
        embeddedCassandra.clearTables();
    }

    @Test
    public void retrieveNextFollowing_10_ResultsFromPage_2() {

    }
}
