package io.smartspaces.scheduling.quartz.orientdb.internal.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardSchedulerDao;

/**
 * The responsibility of this class is to check-in inside Scheduler Cluster.
 */
public class CheckinTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(CheckinTask.class);

  /**
   * This implementation shuts down JVM to not allow to execute the same JOB by
   * two schedulers.
   *
   * If a scheduler cannot register itself due to an exception we stop JVM to
   * prevent concurrent execution of the same jobs together with other nodes
   * that might have found this scheduler as defunct and take over its triggers.
   */
  private static final Runnable DEFAULT_ERROR_HANDLER = new Runnable() {
    @Override
    public void run() {
      // TODO Is there a way to stop only Quartz?
      System.exit(1);
    }
  };

  private StandardSchedulerDao schedulerDao;
  private Runnable errorhandler = DEFAULT_ERROR_HANDLER;

  public CheckinTask(StandardSchedulerDao schedulerDao) {
    this.schedulerDao = schedulerDao;
  }

  // for tests only
  public void setErrorHandler(Runnable errorhandler) {
    this.errorhandler = errorhandler;
  }

  @Override
  public void run() {
    // TODO(keith): When this is real, it needs to run in a transaction.
    log.debug("Node {}:{} checks-in.", schedulerDao.schedulerName, schedulerDao.instanceId);
    try {
      schedulerDao.checkIn();
    } catch (Exception e) {
      log.error("Node " + schedulerDao.instanceId + " could not check-in: " + e.getMessage(), e);
      errorhandler.run();
    }
  }
}
