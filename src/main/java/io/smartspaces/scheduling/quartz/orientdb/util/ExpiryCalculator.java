package io.smartspaces.scheduling.quartz.orientdb.util;

import java.util.Date;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.Constants;
import io.smartspaces.scheduling.quartz.orientdb.cluster.Scheduler;
import io.smartspaces.scheduling.quartz.orientdb.dao.SchedulerDao;

public class ExpiryCalculator {

    private static final Logger log = LoggerFactory.getLogger(ExpiryCalculator.class);

    private final SchedulerDao schedulerDao;
    private final Clock clock;
    private final long jobTimeoutMillis;
    private final long triggerTimeoutMillis;

    public ExpiryCalculator(SchedulerDao schedulerDao, Clock clock,
                            long jobTimeoutMillis, long triggerTimeoutMillis) {
        this.schedulerDao = schedulerDao;
        this.clock = clock;
        this.jobTimeoutMillis = jobTimeoutMillis;
        this.triggerTimeoutMillis = triggerTimeoutMillis;
    }

    public boolean isJobLockExpired(ODocument lock) {
        return isLockExpired(lock, jobTimeoutMillis);
    }

    public boolean isTriggerLockExpired(ODocument lock) {
        String schedulerId = lock.field(Constants.LOCK_INSTANCE_ID);
        return isLockExpired(lock, triggerTimeoutMillis) && hasDefunctScheduler(schedulerId);
    }

    private boolean hasDefunctScheduler(String schedulerId) {
        Scheduler scheduler = schedulerDao.findInstance(schedulerId);
        if (scheduler == null) {
            log.debug("No such scheduler: {}", schedulerId);
            return false;
        }
        return scheduler.isDefunct(clock.millis()) && schedulerDao.isNotSelf(scheduler);
    }

    private boolean isLockExpired(ODocument lock, long timeoutMillis) {
        Date lockTime = lock.field(Constants.LOCK_TIME);
        long elapsedTime = clock.millis() - lockTime.getTime();
        return (elapsedTime > timeoutMillis);
    }
}