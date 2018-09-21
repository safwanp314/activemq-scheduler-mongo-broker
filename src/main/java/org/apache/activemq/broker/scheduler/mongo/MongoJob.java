package org.apache.activemq.broker.scheduler.mongo;

import java.util.Date;

import org.apache.activemq.broker.scheduler.Job;
import org.apache.activemq.broker.scheduler.JobSupport;
import org.bson.Document;
import org.bson.types.Binary;

public class MongoJob implements Job {

	private final String jobId;

	private int repeat;
	private long start;
	private long nextTime;
	private long delay;
	private long period;
	private String cronEntry;
	private int executionCount;

	private byte[] payload;

	public MongoJob(String jobId) {
		this.jobId = jobId;
	}

	public MongoJob(Document document) {
		this.jobId = document.getString("jobId");
		this.repeat = document.getInteger("repeat");
		Date start = document.getDate("start");
		if (start != null)
			this.start = start.getTime();
		Date nextTime = document.getDate("nextTime");
		if (nextTime != null)
			this.nextTime = nextTime.getTime();
		this.delay = document.getLong("delay");
		this.period = document.getLong("period");
		this.cronEntry = document.getString("cronEntry");
		Binary binary = document.get("payload", Binary.class);
		if (binary != null)
			this.payload = binary.getData();
	}

	@Override
	public String getJobId() {
		return jobId;
	}

	@Override
	public int getRepeat() {
		return repeat;
	}

	public void setRepeat(int repeat) {
		this.repeat = repeat;
	}

	@Override
	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getNextTime() {
		return nextTime;
	}

	public void setNextTime(long nextTime) {
		this.nextTime = nextTime;
	}

	@Override
	public long getDelay() {
		return delay;
	}

	public void setDelay(long delay) {
		this.delay = delay;
	}

	@Override
	public long getPeriod() {
		return period;
	}

	public void setPeriod(long period) {
		this.period = period;
	}

	@Override
	public String getCronEntry() {
		return cronEntry;
	}

	public void setCronEntry(String cronEntry) {
		this.cronEntry = cronEntry;
	}

	@Override
	public byte[] getPayload() {
		return payload;
	}

	public void setPayload(byte[] payload) {
		this.payload = payload;
	}

	@Override
	public String getStartTime() {
		return JobSupport.getDateTime(getStart());
	}

	@Override
	public String getNextExecutionTime() {
		return JobSupport.getDateTime(getNextTime());
	}

	@Override
	public int getExecutionCount() {
		return executionCount;
	}

	public void incrementExecutionCount() {
		this.executionCount++;
	}

	public void decrementRepeatCount() {
		if (this.repeat > 0) {
			this.repeat--;
		}
	}

	/**
	 * @return true if this Job represents a Cron entry.
	 */
	public boolean isCron() {
		return getCronEntry() != null && getCronEntry().length() > 0;
	}

	@Override
	public int hashCode() {
		return jobId.hashCode();
	}

	@Override
	public String toString() {
		return "Job: " + getJobId();
	}

	public Document toDocument() {
		Document document = new Document().append("jobId", jobId).append("repeat", repeat)
				.append("start", new Date(start)).append("nextTime", new Date(nextTime)).append("delay", delay)
				.append("period", period).append("cronEntry", cronEntry).append("executionCount", executionCount)
				.append("payload", new Binary(payload));
		return document;
	}
}
