package org.apache.activemq.broker.scheduler.mongo;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.broker.scheduler.JobScheduler;
import org.apache.activemq.broker.scheduler.JobSchedulerStore;
import org.apache.activemq.broker.scheduler.memory.InMemoryJobSchedulerStore;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoJobSchedulerStore extends ServiceSupport implements JobSchedulerStore {

	private static final Logger LOG = LoggerFactory.getLogger(InMemoryJobSchedulerStore.class);

	private final ReentrantLock lock = new ReentrantLock();
	private final String host;
	private final int port;
	private final String database;
	private final Map<String, String> props;

	private final Map<String, MongoJobScheduler> schedulers = new HashMap<String, MongoJobScheduler>();

	public MongoJobSchedulerStore(String host, int port, String database, Map<String, String> props) {
		super();
		this.host = host;
		this.port = port;
		this.database = database;
		this.props = props;
	}

	@Override
	protected void doStop(ServiceStopper stopper) throws Exception {
		for (MongoJobScheduler scheduler : schedulers.values()) {
			try {
				scheduler.stop();
			} catch (Exception e) {
				LOG.error("Failed to stop scheduler: {}", scheduler.getName(), e);
			}
		}
	}

	@Override
	protected void doStart() throws Exception {
		for (MongoJobScheduler scheduler : schedulers.values()) {
			try {
				scheduler.start();
			} catch (Exception e) {
				LOG.error("Failed to start scheduler: {}", scheduler.getName(), e);
			}
		}
	}

	@Override
	public JobScheduler getJobScheduler(String name) throws Exception {
		this.lock.lock();
		try {
			MongoJobScheduler result = this.schedulers.get(name);
			if (result == null) {
				LOG.debug("Creating new in-memory scheduler: {}", name);
				result = new MongoJobScheduler(host, port, database, props, name);
				this.schedulers.put(name, result);
				if (isStarted()) {
					result.start();
				}
			}
			return result;
		} finally {
			this.lock.unlock();
		}
	}

	@Override
	public boolean removeJobScheduler(String name) throws Exception {
		boolean result = false;

		this.lock.lock();
		try {
			MongoJobScheduler scheduler = this.schedulers.remove(name);
			result = scheduler != null;
			if (result) {
				LOG.debug("Removing in-memory Job Scheduler: {}", name);
				scheduler.stop();
				this.schedulers.remove(name);
			}
		} finally {
			this.lock.unlock();
		}
		return result;
	}

	// ---------- Methods that don't really apply to this implementation ------//

	@Override
	public long size() {
		return 0;
	}

	@Override
	public File getDirectory() {
		return null;
	}

	@Override
	public void setDirectory(File directory) {
	}
}
