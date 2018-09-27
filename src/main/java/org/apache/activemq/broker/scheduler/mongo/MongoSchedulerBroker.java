package org.apache.activemq.broker.scheduler.mongo;

import java.util.Map;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.scheduler.SchedulerBroker;

public class MongoSchedulerBroker implements BrokerPlugin {

	private MongoJobSchedulerStore jobSchedulerStore;

	private String host;
	private int port;
	private String database;
	private Map<String, String> props;

	public MongoSchedulerBroker() {
		this(null, 0, null);
	}

	public MongoSchedulerBroker(String host, int port, String database) {
		this(host, port, database, null);
	}

	public MongoSchedulerBroker(String host, int port, String database, Map<String, String> props) {
		super();

		if (host == null)
			host = "127.0.0.1";
		if (port == 0)
			port = 27017;
		if (database == null)
			database = "activemq-broker";

		this.host = host;
		this.port = port;
		this.database = database;
		this.props = props;

	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public Map<String, String> getProps() {
		return props;
	}

	public void setProps(Map<String, String> props) {
		this.props = props;
	}

	@Override
	public Broker installPlugin(Broker broker) throws Exception {
		return new SchedulerBroker(broker.getBrokerService(), broker, jobSchedulerStore);
	}

	public void afterPropertiesSet() {
		jobSchedulerStore = new MongoJobSchedulerStore(host, port, database, props);
	}

	public void start() throws Exception {
		jobSchedulerStore.getJobScheduler("JMS");
		jobSchedulerStore.start();
	}

	public void stop() throws Exception {
		jobSchedulerStore.stop();
	}
}
