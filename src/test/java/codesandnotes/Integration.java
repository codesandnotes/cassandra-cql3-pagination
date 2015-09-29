package codesandnotes;

import codesandnotes.cassandra.EmbeddedCassandra;
import org.junit.Ignore;

@Ignore
public class Integration {

    protected static final String CASSANDRA_KEYSPACE = "cql3_pagination";
    protected static final String CASSANDRA_NODES = "127.0.0.1";
    protected static final int CASSANDRA_PORT = 9142;

    protected static EmbeddedCassandra embeddedCassandra;

    public Integration() {
        if (embeddedCassandra == null) {
            synchronized (this) {
                if (embeddedCassandra == null) {
                    embeddedCassandra = EmbeddedCassandra.getInstance(CASSANDRA_KEYSPACE, CASSANDRA_NODES, CASSANDRA_PORT);
                    embeddedCassandra.start("db/database.ddl.cql");
                }
            }
        }
    }
}
