/*
 * Copyright (C) 2016 Keith M. Hughes
 * Forked from code (c) Michael S. Klishin, Alex Petrov, 2011-2015.
 * Forked from code from MuleSoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
/*
 * $Id: MongoDBJobStore.java 253170 2014-01-06 02:28:03Z waded $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package io.smartspaces.scheduling.quartz.orientdb.internal;

import java.util.List;

import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardJobDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardTriggerDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.TriggerConverter;

public class TriggerAndJobPersister {

	private static final Logger log = LoggerFactory.getLogger(TriggerAndJobPersister.class);

	private final StandardTriggerDao triggerDao;
	private final StandardJobDao jobDao;
	private TriggerConverter triggerConverter;

	public TriggerAndJobPersister(StandardTriggerDao triggerDao, StandardJobDao jobDao,
			TriggerConverter triggerConverter) {
		this.triggerDao = triggerDao;
		this.jobDao = jobDao;
		this.triggerConverter = triggerConverter;
	}

	public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
		final ODocument doc = jobDao.getJob(jobKey);
		return triggerDao.getTriggersForJob(doc);
	}

	public boolean removeJob(JobKey jobKey) {
		ODocument jobDoc = jobDao.getJob(jobKey);
		if (jobDoc != null) {
			jobDao.remove(jobDoc);
			triggerDao.removeByJobId(jobDoc.getIdentity());
			return true;
		} else {
			return false;
		}
	}

	public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
		for (JobKey key : jobKeys) {
			removeJob(key);
		}
		return false;
	}

	public boolean removeTrigger(TriggerKey triggerKey) {
		ODocument trigger = triggerDao.findTrigger(triggerKey);
		if (trigger != null) {
			removeOrphanedJob(trigger);
			triggerDao.remove(trigger);
			return true;
		}
		return false;
	}

	public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
		// FIXME return boolean allFound = true when all removed
		for (TriggerKey key : triggerKeys) {
			removeTrigger(key);
		}
		return false;
	}

	public boolean removeTriggerWithoutNextFireTime(OperableTrigger trigger) {
		if (trigger.getNextFireTime() == null) {
			log.info("Removing trigger {} as it has no next fire time.", trigger.getKey());
			removeTrigger(trigger.getKey());
			return true;
		}
		return false;
	}

	public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
		OperableTrigger oldTrigger = triggerDao.getTrigger(triggerKey);
		if (oldTrigger == null) {
			return false;
		}

		if (!oldTrigger.getJobKey().equals(newTrigger.getJobKey())) {
			throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
		}

		removeOldTrigger(triggerKey);
		copyOldJobDataMap(newTrigger, oldTrigger);
		storeNewTrigger(newTrigger, oldTrigger);

		return true;
	}

	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger) throws JobPersistenceException {
		ORID jobId = jobDao.storeJob(newJob, false);

		storeTrigger(newTrigger, jobId, false);
	}

	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws JobPersistenceException {
		JobKey jobKey = newTrigger.getJobKey();
		if (jobKey == null) {
			throw new JobPersistenceException("Trigger must be associated with a job. Please specify a JobKey.");
		}

		ODocument jobDoc = jobDao.getJob(jobKey);
		if (jobDoc != null) {
			storeTrigger(newTrigger, jobDoc.getIdentity(), replaceExisting);
		} else {
			throw new JobPersistenceException("Could not find job with key " + jobKey);
		}
	}

	private void copyOldJobDataMap(OperableTrigger newTrigger, OperableTrigger trigger) {
		// Copy across the job data map from the old trigger to the new one.
		newTrigger.getJobDataMap().putAll(trigger.getJobDataMap());
	}

	private boolean isNotDurable(ODocument job) {
		return !job.containsField(Constants.JOB_DURABILITY)
				|| job.field(Constants.JOB_DURABILITY).toString().equals("false");
	}

	private boolean isOrphan(ODocument job) {
		return (job != null) && isNotDurable(job) && triggerDao.hasLastTrigger(job);
	}

	private void removeOldTrigger(TriggerKey triggerKey) {
		// Can't call remove trigger as if the job is not durable, it will
		// remove the job too
		triggerDao.remove(triggerKey);
	}

	// If the removal of the Trigger results in an 'orphaned' Job that is not
	// 'durable',
	// then the job should be removed also.
	private void removeOrphanedJob(ODocument trigger) {
		if (trigger.containsField(Constants.TRIGGER_JOB_ID)) {
			// There is only 1 job per trigger so no need to look further.
			ODocument job = trigger.field(Constants.TRIGGER_JOB_ID);
			if (isOrphan(job)) {
				jobDao.remove(job);
			}
		} else {
			log.debug("The trigger had no associated jobs");
		}
	}

	private void storeNewTrigger(OperableTrigger newTrigger, OperableTrigger oldTrigger)
			throws JobPersistenceException {
		try {
			storeTrigger(newTrigger, false);
		} catch (JobPersistenceException jpe) {
			storeTrigger(oldTrigger, false);
			throw jpe;
		}
	}

	private void storeTrigger(OperableTrigger newTrigger, ORID jobId, boolean replaceExisting)
			throws JobPersistenceException {
		ODocument trigger = triggerConverter.toDocument(newTrigger, jobId);
		if (replaceExisting) {
			triggerDao.replace(newTrigger.getKey(), trigger);
		} else {
			triggerDao.insert(trigger, newTrigger);
		}
	}
}
