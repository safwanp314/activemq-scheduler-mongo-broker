package org.apache.activemq.broker.scheduler.mongo;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.MessageFormatException;

import org.apache.activemq.broker.scheduler.CronParser;
import org.apache.activemq.broker.scheduler.Job;
import org.apache.activemq.broker.scheduler.JobListener;
import org.apache.activemq.broker.scheduler.JobScheduler;
import org.apache.activemq.broker.scheduler.JobSupport;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IdGenerator;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;

public class MongoJobScheduler implements JobScheduler {

	private static final Logger LOG = LoggerFactory.getLogger(MongoJobScheduler.class);
	private static final IdGenerator ID_GENERATOR = new IdGenerator();
	private static final Long SCHEDULER_INTERVAL = 60000L;

	private final String name;
	private final AtomicBoolean started = new AtomicBoolean(false);
	private final AtomicBoolean dispatchEnabled = new AtomicBoolean(false);
	private MongoCollection<Document> mongoCollection;
	private final List<JobListener> jobListeners = new CopyOnWriteArrayList<>();
	private final JobSchedularThread schedularThread = new JobSchedularThread();

	private final String host;
	private final int port;
	private final String database;
	private final Map<String, String> props;

	public MongoJobScheduler(String host, int port, String database, Map<String, String> props, String name) {
		super();
		this.host = host;
		this.port = port;
		this.database = database;
		this.name = name;
		this.props = props;
	}

	private MongoClient mongo(String host, Integer port, Map<String, String> props) throws Exception {

		MongoClientOptions.Builder clientOptions = new MongoClientOptions.Builder();

		if (props != null && props.size() > 0) {
			Iterator<Entry<String, String>> it = props.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, String> entry = it.next();
				String key = entry.getKey();
				String vale = entry.getValue();
				try {
					Method method = null;
					Method[] methods = clientOptions.getClass().getDeclaredMethods();
					for (Method m : methods) {
						if (m.getName().equals(key)) {
							method = m;
							break;
						}
					}
					if (method != null) {

						Class<?> clazz = method.getParameterTypes()[0];
						if (clazz.equals(Integer.class)) {
							method.invoke(Integer.parseInt(vale));
						} else if (clazz.equals(String.class)) {
							method.invoke(vale);
						} else if (clazz.equals(Long.class)) {
							method.invoke(Long.parseLong(vale));
						} else if (clazz.equals(Double.class)) {
							method.invoke(Double.parseDouble(vale));
						} else if (clazz.equals(Float.class)) {
							method.invoke(Float.parseFloat(vale));
						} else if (clazz.equals(Boolean.class)) {
							method.invoke(Boolean.parseBoolean(vale));
						} else {
							method.invoke(clazz.cast(vale));
						}
					} else {
						LOG.warn("property not found in mongo config {}", key);
					}
				} catch (Exception e) {
					LOG.error("error in setting prop {}", key, e);
				}
			}
		}
		return new MongoClient(new ServerAddress(host, port), clientOptions.build());
	}

	private MongoCollection<Document> mongoCollection() {
		try {
			if (mongoCollection == null) {
				mongoCollection = mongo(host, port, props).getDatabase(database).getCollection(name);
			}
			return mongoCollection;
		} catch (Exception e) {
			LOG.error("error in install plugin", e);
			return null;
		}
	}

	@Override
	public String getName() throws Exception {
		return this.name;
	}

	public void start() throws Exception {
		if (started.compareAndSet(false, true)) {
			startDispatching();
			schedularThread.start();
			LOG.trace("JobScheduler[{}] started", name);
		}
	}

	@SuppressWarnings("deprecation")
	public void stop() throws Exception {
		if (started.compareAndSet(true, false)) {
			stopDispatching();
			schedularThread.stop();
			LOG.trace("JobScheduler[{}] stopped", name);
		}
	}

	public boolean isStarted() {
		return started.get();
	}

	public boolean isDispatchEnabled() {
		return dispatchEnabled.get();
	}

	@Override
	public void startDispatching() throws Exception {
		dispatchEnabled.set(true);
	}

	@Override
	public void stopDispatching() throws Exception {
		dispatchEnabled.set(false);
	}

	@Override
	public void addListener(JobListener listener) throws Exception {
		this.jobListeners.add(listener);
	}

	@Override
	public void removeListener(JobListener listener) throws Exception {
		this.jobListeners.remove(listener);
	}

	@Override
	public void schedule(String jobId, ByteSequence payload, long delay) throws Exception {
		doSchedule(jobId, payload, "", 0, delay, 0);

	}

	@Override
	public void schedule(String jobId, ByteSequence payload, String cronEntry) throws Exception {
		doSchedule(jobId, payload, cronEntry, 0, 0, 0);
	}

	@Override
	public void schedule(String jobId, ByteSequence payload, String cronEntry, long delay, long period, int repeat)
			throws Exception {
		doSchedule(jobId, payload, cronEntry, delay, period, repeat);
	}

	@Override
	public void remove(long time) throws Exception {
		throw new Exception("NOT SUPPORTED METHOD");
	}

	@Override
	public void remove(String jobId) throws Exception {
		doRemoveJob(jobId);
	}

	@Override
	public void removeAllJobs() throws Exception {
		throw new Exception("NOT SUPPORTED METHOD");
	}

	@Override
	public void removeAllJobs(long start, long finish) throws Exception {
		throw new Exception("NOT SUPPORTED METHOD");
	}

	@Override
	public long getNextScheduleTime() throws Exception {
		throw new Exception("NOT SUPPORTED METHOD");
	}

	@Override
	public List<Job> getNextScheduleJobs() throws Exception {
		throw new Exception("NOT SUPPORTED METHOD");
	}

	@Override
	public List<Job> getAllJobs() throws Exception {
		throw new Exception("NOT SUPPORTED METHOD");
	}

	@Override
	public List<Job> getAllJobs(long start, long finish) throws Exception {
		throw new Exception("NOT SUPPORTED METHOD");
	}

	private void doSchedule(final String jobId, final ByteSequence payload, final String cronEntry, long delay,
			long period, int repeat) throws IOException {

		long startTime = System.currentTimeMillis();
		long executionTime = 0;
		// round startTime - so we can schedule more jobs at the same time
		startTime = ((startTime + 500) / 500) * 500;

		if (cronEntry != null && cronEntry.length() > 0) {
			try {
				executionTime = CronParser.getNextScheduledTime(cronEntry, startTime);
			} catch (MessageFormatException e) {
				throw new IOException(e.getMessage());
			}
		}

		if (executionTime == 0) {
			// start time not set by CRON - so it it to the current time
			executionTime = startTime;
		}

		if (delay > 0) {
			executionTime += delay;
		} else {
			executionTime += period;
		}

		MongoJob newJob = new MongoJob(jobId);
		newJob.setStart(startTime);
		newJob.setCronEntry(cronEntry);
		newJob.setDelay(delay);
		newJob.setPeriod(period);
		newJob.setRepeat(repeat);
		newJob.setNextTime(executionTime);
		newJob.setPayload(payload.getData());
		Document document = newJob.toDocument();
		// BasicDBObject query = new BasicDBObject("jobId", jobId);
		// if (mongoCollection.findOneAndUpdate(query, document) == null) {
		mongoCollection().insertOne(document);
		// }
	}

	private void doReschedule(MongoJob job, long nextExecutionTime) {

		job.setNextTime(nextExecutionTime);
		job.incrementExecutionCount();
		if (!job.isCron()) {
			job.decrementRepeatCount();
		}

		LOG.trace("JobScheduler rescheduling job[{}] to fire at: {}", job.getJobId(),
				JobSupport.getDateTime(nextExecutionTime));

		Document document = job.toDocument();
		BasicDBObject query = new BasicDBObject("jobId", job.getJobId());
		if (mongoCollection().findOneAndUpdate(query, document) == null) {
			mongoCollection().insertOne(document);
		}

	}

	private void doRemoveJob(String jobId) throws IOException {
		BasicDBObject query = new BasicDBObject("jobId", jobId);
		mongoCollection().deleteOne(query);
	}

	private void doRemoveJob(Document document) throws IOException {
		mongoCollection().deleteOne(document);
	}

	private boolean canDispatch() {
		return isStarted() && isDispatchEnabled();
	}

	private long calculateNextExecutionTime(MongoJob job, long currentTime, int repeat) throws MessageFormatException {
		long result = currentTime;
		String cron = job.getCronEntry();
		if (cron != null && cron.length() > 0) {
			result = CronParser.getNextScheduledTime(cron, result);
		} else if (job.getRepeat() != 0) {
			result += job.getPeriod();
		}
		return result;
	}

	private void dispatch(MongoJob job) throws IllegalStateException, IOException {
		if (canDispatch()) {
			LOG.debug("Firing: {}", job);
			for (JobListener l : jobListeners) {
				l.scheduledJob(job.getJobId(), new ByteSequence(job.getPayload()));
			}
		}
	}

	private class JobSchedularThread extends Thread {

		@Override
		public void run() {

			while (true) {
				try {

					if (!started.get()) {
						break;
					}

					long curr = System.currentTimeMillis(), prev, diff;

					if (dispatchEnabled.get()) {

						LOG.debug("finding sceduled messages in database at {}", curr);
						MongoCursor<Document> it = mongoCollection().find(Filters.lte("nextTime", new Date(curr)))
								.batchSize(5000).iterator();
						LOG.debug("finding sceduled messages cursor {}", it.hasNext());
						while (it.hasNext()) {
							Document document = it.next();
							LOG.debug("finding sceduled messages document {}", document);

							try {
								long currentTime = System.currentTimeMillis(), nextExecutionTime;
								MongoJob job = new MongoJob(document);

								int repeat = job.getRepeat();
								nextExecutionTime = calculateNextExecutionTime(job, currentTime, repeat);
								if (!job.isCron()) {
									dispatch(job);
									if (repeat != 0) {
										// Reschedule for the next time, the scheduler will take care of
										// updating the repeat counter on the update.
										doReschedule(job, nextExecutionTime);
									} else {
										doRemoveJob(document);
									}
								} else {
									if (repeat == 0) {
										// This is a non-repeating Cron entry so we can fire and forget it.
										dispatch(job);
										doRemoveJob(document);
									}

									if (nextExecutionTime > currentTime) {
										// Reschedule the cron job as a new event, if the cron entry signals
										// a repeat then it will be stored separately and fired as a normal
										// event with decrementing repeat.
										doReschedule(job, nextExecutionTime);

										if (repeat != 0) {
											// we have a separate schedule to run at this time
											// so the cron job is used to set of a separate schedule
											// hence we won't fire the original cron job to the
											// listeners but we do need to start a separate schedule
											String jobId = ID_GENERATOR.generateId();
											ByteSequence payload = new ByteSequence(job.getPayload());
											schedule(jobId, payload, "", job.getDelay(), job.getPeriod(),
													job.getRepeat());
										}
									}
								}
							} catch (Exception e) {
								LOG.error("error in mongo scheduler", e);
							}
						}
					}
					prev = curr;
					curr = System.currentTimeMillis();
					diff = SCHEDULER_INTERVAL - (curr - prev);
					if (diff > 0) {
						Thread.sleep(diff);
					}
				} catch (Exception e) {
					LOG.error("thread interrupted", e);
				}
			}
		}
	}
}
