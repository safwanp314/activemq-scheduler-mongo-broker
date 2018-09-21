package org.apache.activemq.broker.scheduler.mongo;

import java.util.Map;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.scheduler.SchedulerBroker;

public class MongoSchedulerBroker implements BrokerPlugin {

	private final MongoJobSchedulerStore jobSchedulerStore;

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

		jobSchedulerStore = new MongoJobSchedulerStore(host, port, database, props);
	}

	@Override
	public Broker installPlugin(Broker broker) throws Exception {
		return new SchedulerBroker(broker.getBrokerService(), broker, jobSchedulerStore);
	}

	public void start() throws Exception {
		jobSchedulerStore.getJobScheduler("JMS");
		jobSchedulerStore.start();
	}

	public void stop() throws Exception {
		jobSchedulerStore.stop();
	}
}
