package io.smartspaces.scheduling.quartz.orientdb.internal.cluster;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

public class CheckinExecutor {

  private static final Logger log = LoggerFactory.getLogger(CheckinExecutor.class);

  // Arbitrary value:
  private static final int INITIAL_DELAY = 0;
  private final CheckinTask checkinTask;
  private final long checkinIntervalMillis;
  private final String instanceId;

  private ScheduledExecutorService executor;

  /**
   * The future for the checkin task.
   */
  private Future<?> checkinFuture;

  public CheckinExecutor(ScheduledExecutorService executor, CheckinTask checkinTask, long checkinIntervalMillis, String instanceId) {
    this.executor = executor;
    this.checkinTask = checkinTask;
    this.checkinIntervalMillis = checkinIntervalMillis;
    this.instanceId = instanceId;
  }

  /**
   * Start execution of CheckinTask.
   */
  public void start() {
    log.debug("Starting check-in task for scheduler instance: {}", instanceId);
    checkinFuture = executor.scheduleAtFixedRate(checkinTask, INITIAL_DELAY, checkinIntervalMillis, MILLISECONDS);
  }

  /**
   * Stop execution of CheckinTask.
   */
  public void shutdown() {
    log.debug("Stopping CheckinExecutor for scheduler instance: {}", instanceId);
    checkinFuture.cancel(true);
  }
}
