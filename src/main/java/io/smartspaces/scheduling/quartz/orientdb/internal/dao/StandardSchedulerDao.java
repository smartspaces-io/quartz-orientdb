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

package io.smartspaces.scheduling.quartz.orientdb.internal.dao;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import io.smartspaces.scheduling.quartz.orientdb.internal.Constants;
import io.smartspaces.scheduling.quartz.orientdb.internal.StandardOrientDbStoreAssembler;
import io.smartspaces.scheduling.quartz.orientdb.internal.cluster.Scheduler;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.Clock;

public class StandardSchedulerDao {

  private static final Logger log = LoggerFactory.getLogger(StandardSchedulerDao.class);

  private final StandardOrientDbStoreAssembler storeAssembler;

  public final String schedulerName;
  public final String instanceId;
  public final Clock clock;

  public StandardSchedulerDao(StandardOrientDbStoreAssembler storeAssembler, String schedulerName,
      String instanceId, Clock clock) {
    this.storeAssembler = storeAssembler;
    this.schedulerName = schedulerName;
    this.instanceId = instanceId;
    this.clock = clock;
  }

  /**
   * Checks-in in cluster to inform other nodes that its alive.
   */
  public void checkIn() {
    long lastCheckinTime = clock.millis();

    log.debug("Saving node data: name='{}', id='{}', checkin time={}, interval={}", schedulerName,
        instanceId, lastCheckinTime);

    List<ODocument> schedulers = createSchedulerFilter(schedulerName, instanceId);
    ODocument scheduler;
    if (!schedulers.isEmpty()) {
      scheduler = schedulers.get(0);
    } else {
      scheduler = new ODocument("Scheduler").field(Constants.SCHEDULER_NAME_FIELD, schedulerName)
          .field(Constants.SCHEDULER_INSTANCE_ID_FIELD, instanceId);
    }

    scheduler.field(Constants.SCHEDULER_LAST_CHECKIN_TIME_FIELD, lastCheckinTime);

    scheduler.save();

    log.debug("Node {}:{} ", schedulerName, instanceId);
  }

  /**
   * @return Scheduler or null when not found
   */
  public Scheduler findInstance(String instanceId) {
    log.debug("Finding scheduler instance: {}", instanceId);
    List<ODocument> docs = createSchedulerFilter(schedulerName, instanceId);

    Scheduler scheduler = null;
    if (!docs.isEmpty()) {
      scheduler = toScheduler(docs.get(0));
      log.debug("Returning scheduler instance '{}' with last checkin time: {}",
          scheduler.getInstanceId(), scheduler.getLastCheckinTime());
    } else {
      log.debug("Scheduler instance '{}' not found.", instanceId);
    }
    return scheduler;
  }

  public boolean isNotSelf(Scheduler scheduler) {
    return !instanceId.equals(scheduler.getInstanceId());
  }

  /**
   * Return all scheduler instances in ascending order by last check-in time.
   *
   * @return scheduler instances ordered by last check-in time
   */
  public List<Scheduler> getAllByCheckinTime() {
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select from Scheduler order by lastCheckinTime asc");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> result = database.command(query).execute(schedulerName, instanceId);

    List<Scheduler> schedulers = new LinkedList<Scheduler>();
    for (ODocument schedulerDoc : result) {
      schedulers.add(toScheduler(schedulerDoc));
    }

    return schedulers;
  }

  /**
   * Remove selected scheduler instance entry from database.
   *
   * The scheduler is selected based on its name, instanceId, and
   * lastCheckinTime. If the last check-in time is different, then it is not
   * removed, for it might have gotten back to live.
   *
   * @param instanceId
   *          instance id
   * @param lastCheckinTime
   *          last time scheduler has checked in
   *
   * @return when removed successfully
   */
  public boolean remove(String instanceId, long lastCheckinTime) {
    log.debug("Removing scheduler: {},{},{}", schedulerName, instanceId, lastCheckinTime);

    List<ODocument> schedulerCollection =
        createSchedulerFilter(schedulerName, instanceId, lastCheckinTime);
    for (ODocument schedulerDoc : schedulerCollection) {
      schedulerDoc.delete();
    }

    log.debug("Result of removing scheduler ({},{},{}):", schedulerName, instanceId,
        lastCheckinTime);
    return !schedulerCollection.isEmpty();
  }

  private List<ODocument> createSchedulerFilter(String schedulerName, String instanceId,
      long lastCheckinTime) {
    // TODO(keith): class and field names should come from external constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select from Scheduler where schedulerName=? and instanceId=? and lastCheckinTime=?");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> result =
        database.command(query).execute(schedulerName, instanceId, lastCheckinTime);

    return result;
  }

  private List<ODocument> createSchedulerFilter(String schedulerName, String instanceId) {
    // TODO(keith): class and field names should come from external constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select from Scheduler where schedulerName=? and instanceId=?");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> result = database.command(query).execute(schedulerName, instanceId);

    return result;
  }

  /**
   * Create a scheduler from the OrientDB document.
   * 
   * @param schedulerDoc
   *          the orientdb document
   * @return
   */
  private Scheduler toScheduler(ODocument schedulerDoc) {
    return new Scheduler((String) schedulerDoc.field(Constants.SCHEDULER_NAME_FIELD),
        (String) schedulerDoc.field(Constants.SCHEDULER_INSTANCE_ID_FIELD),
        (Long) schedulerDoc.field(Constants.SCHEDULER_LAST_CHECKIN_TIME_FIELD),
        (Long) schedulerDoc.field(Constants.SCHEDULER_CHECKIN_INTERVAL_FIELD));
  }
}
