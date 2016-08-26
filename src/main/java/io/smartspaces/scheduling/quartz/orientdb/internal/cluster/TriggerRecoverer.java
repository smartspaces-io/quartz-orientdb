package io.smartspaces.scheduling.quartz.orientdb.internal.cluster;

import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smartspaces.scheduling.quartz.orientdb.internal.Constants;
import io.smartspaces.scheduling.quartz.orientdb.internal.LockManager;
import io.smartspaces.scheduling.quartz.orientdb.internal.TriggerAndJobPersister;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardJobDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardLockDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardTriggerDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.MisfireHandler;

public class TriggerRecoverer {

    private static final Logger log = LoggerFactory.getLogger(TriggerRecoverer.class);

    private final StandardLockDao locksDao;
    private final TriggerAndJobPersister persister;
    private final LockManager lockManager;
    private final StandardTriggerDao triggerDao;
    private final StandardJobDao jobDao;
    private final RecoveryTriggerFactory recoveryTriggerFactory;
    private final MisfireHandler misfireHandler;

    public TriggerRecoverer(StandardLockDao locksDao, TriggerAndJobPersister persister,
                            LockManager lockManager, StandardTriggerDao triggerDao,
                            StandardJobDao jobDao, RecoveryTriggerFactory recoveryTriggerFactory,
                            MisfireHandler misfireHandler) {
        this.locksDao = locksDao;
        this.persister = persister;
        this.lockManager = lockManager;
        this.triggerDao = triggerDao;
        this.jobDao = jobDao;
        this.recoveryTriggerFactory = recoveryTriggerFactory;
        this.misfireHandler = misfireHandler;
    }

    public void recover() throws JobPersistenceException {
        for (TriggerKey key : locksDao.findOwnTriggersLocks()) {
            OperableTrigger trigger = triggerDao.getTrigger(key);
            if (trigger == null) {
                continue;
            }

            // Make the trigger's lock fresh for other nodes,
            // so they don't recover it.
            if (locksDao.updateOwnLock(trigger.getKey())) {
                doRecovery(trigger);
                lockManager.releaseAcquiredTrigger(trigger);
            }
        }
    }

    /**
     * Do recovery procedure after failed run of given trigger.
     *
     * @param trigger    trigger to recover
     * @return recovery trigger or null if its job doesn't want that
     * @throws JobPersistenceException
     */
    public OperableTrigger doRecovery(OperableTrigger trigger) throws JobPersistenceException {
        OperableTrigger recoveryTrigger = null;
        if (jobDao.requestsRecovery(trigger.getJobKey())) {
            recoveryTrigger = recoverTrigger(trigger);
            if (!wasOneShotTrigger(trigger)) {
                updateMisfires(trigger);
            }
        } else if (wasOneShotTrigger(trigger)) {
            cleanUpFailedRun(trigger);
        } else {
            updateMisfires(trigger);
        }
        return recoveryTrigger;
    }

    private OperableTrigger recoverTrigger(OperableTrigger trigger)
            throws JobPersistenceException {
        log.info("Recovering trigger: {}", trigger.getKey());
        OperableTrigger recoveryTrigger = recoveryTriggerFactory.from(trigger);
        persister.storeTrigger(recoveryTrigger, Constants.STATE_WAITING, false);
        return recoveryTrigger;
    }

    private void updateMisfires(OperableTrigger trigger) throws JobPersistenceException {
        if (misfireHandler.applyMisfireOnRecovery(trigger)) {
            log.info("Misfire applied. Replacing trigger: {}", trigger.getKey());
            persister.storeTrigger(trigger, Constants.STATE_WAITING, true);
        } else {
            //TODO should complete trigger?
            log.warn("Recovery misfire not applied for trigger: {}",
                    trigger.getKey());
//            storeTrigger(conn, trig,
//                    null, true, STATE_COMPLETE, forceState, recovering);
//            schedSignaler.notifySchedulerListenersFinalized(trig);
        }
    }

    private void cleanUpFailedRun(OperableTrigger trigger) {
        persister.removeTrigger(trigger.getKey());
    }

    private boolean wasOneShotTrigger(OperableTrigger trigger) {
        return trigger.getNextFireTime() == null
                && trigger.getStartTime().equals(trigger.getFinalFireTime());
    }
}
