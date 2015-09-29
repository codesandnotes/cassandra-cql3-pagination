package codesandnotes.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A utility class used to manage a cassandra-unit's embedded server.<br/>
 * Very useful in case of acceptance or integration tests!
 */
public class EmbeddedCassandra {

	private static EmbeddedCassandra embeddedCassandra;

	private final String keyspace;
	private final String nodes;
	private final int port;

	private Cluster cluster;
	private Session session;

	private EmbeddedCassandra(String keyspace, String nodes, int port) {
		this.keyspace = keyspace;
		this.nodes = nodes;
		this.port = port;
	}

	/**
	 * Clears all the tables in the keyspace, without actually dropping the tables.
	 */
	public void clearTables() {
		Collection<TableMetadata> tables = cluster.getMetadata().getKeyspace(keyspace).getTables();
		tables.forEach(table -> session.execute(QueryBuilder.truncate(table)));
	}

	public Cluster cluster() {
		return cluster;
	}

	/**
	 * Either creates or returns an existing {@link EmbeddedCassandra} instance. If the instance already exists, that
	 * instance's parameters (keyspace, nodes, port) are used: no new instance will be generated!
	 */
	public static EmbeddedCassandra getInstance(String keyspace, String nodes, int port) {
		if (embeddedCassandra == null) {
			ReentrantLock lock = new ReentrantLock();
			lock.lock();
			try {
				if (embeddedCassandra == null) {
					embeddedCassandra = new EmbeddedCassandra(keyspace, nodes, port);
				}
			} finally {
				lock.unlock();
			}
		}
		return embeddedCassandra;
	}

	public String keyspace() {
		return keyspace;
	}

	/**
	 * Loads additional CQL data sets, specified as a list of files in the classpath.
	 */
	public void loadScripts(String... cqlScriptPaths) {
		Arrays.stream(cqlScriptPaths).forEach(cqlScriptPath -> {
			CQLDataLoader dataLoader = new CQLDataLoader(session);
			ClassPathCQLDataSet dataSet = new ClassPathCQLDataSet(cqlScriptPath);
			dataLoader.load(dataSet);
		});
	}

	public Session session() {
		return session;
	}

	/**
	 * Starts this instance of an embedded cassandra.
	 */
	public void start() {
		try {
			EmbeddedCassandraServerHelper.startEmbeddedCassandra();

			cluster = new Cluster.Builder().addContactPoints(nodes).withPort(port).build();
			session = cluster.connect();

		} catch (Exception e) {
			throw new RuntimeException("Unexpected error while trying to start() EmbeddedCassandra.");
		}
	}

	/**
	 * Starts this instance of an embedded cassandra and executes the CQL commands in the specified CSL file.
	 *
	 * @param cqlScriptPath The path (from the classpath) to the CQL script to execute at startup.
	 */
	public void start(String cqlScriptPath) {
		this.start();

		CQLDataLoader dataLoader = new CQLDataLoader(session);
		ClassPathCQLDataSet dataSet = new ClassPathCQLDataSet(cqlScriptPath, true, true, keyspace);
		dataLoader.load(dataSet);
	}
}
