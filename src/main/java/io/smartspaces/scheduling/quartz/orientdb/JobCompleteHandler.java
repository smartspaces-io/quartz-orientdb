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

package io.smartspaces.scheduling.quartz.orientdb;

import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smartspaces.scheduling.quartz.orientdb.dao.StandardJobDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.LocksDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.TriggerDao;

//TODO Think of some better name for doing work after a job has completed :-)
public class JobCompleteHandler {

    private static final Logger log = LoggerFactory.getLogger(JobCompleteHandler.class);

    private final TriggerAndJobPersister persister;
    private final SchedulerSignaler signaler;
    private final StandardJobDao jobDao;
    private final LocksDao locksDao;
    private TriggerDao triggerDao;

    public JobCompleteHandler(TriggerAndJobPersister persister, SchedulerSignaler signaler,
                              StandardJobDao jobDao, LocksDao locksDao, TriggerDao triggerDao) {
        this.persister = persister;
        this.signaler = signaler;
        this.jobDao = jobDao;
        this.locksDao = locksDao;
        this.triggerDao = triggerDao;
    }

    public void jobComplete(OperableTrigger trigger, JobDetail job,
                            CompletedExecutionInstruction executionInstruction)
            throws JobPersistenceException {
        log.debug("Trigger completed {}", trigger.getKey());

        if (job.isPersistJobDataAfterExecution()) {
            if (job.getJobDataMap().isDirty()) {
                log.debug("Job data map dirty, will store {}", job.getKey());
                jobDao.storeJobInMongo(job, true);
            }
        }

        if (job.isConcurrentExectionDisallowed()) {
            locksDao.unlockJob(job);
        }

        process(trigger, executionInstruction);

        locksDao.unlockTrigger(trigger);
    }

    private boolean isTriggerDeletionRequested(CompletedExecutionInstruction triggerInstCode) {
        return triggerInstCode == CompletedExecutionInstruction.DELETE_TRIGGER;
    }

    private void process(OperableTrigger trigger, CompletedExecutionInstruction executionInstruction)
            throws JobPersistenceException {
        // check for trigger deleted during execution...
        OperableTrigger dbTrigger = triggerDao.getTrigger(trigger.getKey());
        if (dbTrigger != null) {
            if (isTriggerDeletionRequested(executionInstruction)) {
                if (trigger.getNextFireTime() == null) {
                    // double check for possible reschedule within job
                    // execution, which would cancel the need to delete...
                    if (dbTrigger.getNextFireTime() == null) {
                        persister.removeTrigger(trigger.getKey());
                    }
                } else {
                    persister.removeTrigger(trigger.getKey());
                    signaler.signalSchedulingChange(0L);
                }
            } else if (executionInstruction == CompletedExecutionInstruction.SET_TRIGGER_COMPLETE) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            } else if (executionInstruction == CompletedExecutionInstruction.SET_TRIGGER_ERROR) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            } else if (executionInstruction == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            } else if (executionInstruction == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE) {
                // TODO: need to store state
                signaler.signalSchedulingChange(0L);
            }
        }
    }
}
