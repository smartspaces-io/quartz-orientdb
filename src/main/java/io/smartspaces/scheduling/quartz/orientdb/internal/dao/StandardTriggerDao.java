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

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import io.smartspaces.scheduling.quartz.orientdb.internal.Constants;
import io.smartspaces.scheduling.quartz.orientdb.internal.StandardOrientDbStoreAssembler;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.TriggerConverter;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.Keys;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.QueryHelper;

public class StandardTriggerDao {

  private static final Logger log = LoggerFactory.getLogger(StandardTriggerDao.class);

  private final StandardOrientDbStoreAssembler storeAssembler;

  private QueryHelper queryHelper;

  private TriggerConverter triggerConverter;

  public StandardTriggerDao(StandardOrientDbStoreAssembler storeAssembler, QueryHelper queryHelper,
      TriggerConverter triggerConverter) {
    this.storeAssembler = storeAssembler;
    this.queryHelper = queryHelper;
    this.triggerConverter = triggerConverter;
  }

  public void removeAll() {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    for (ODocument trigger : database.browseClass("Trigger")) {
      trigger.delete();
    }
  }

  public boolean exists(TriggerKey triggerKey) {
    // TODO(keith): class and field names should come from external
    // constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select from Trigger where keyGroup=? and keyName=?");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> result =
        database.command(query).execute(triggerKey.getGroup(), triggerKey.getName());

    return !result.isEmpty();
  }

  public List<ODocument> findEligibleToRun(Date noLaterThanDate) {
    // TODO(keith): class and field names should come from external
    // constants
    // Also create query ahead of time when DAO starts.
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select from Trigger where (nextFireTime is null or nextFireTime <= ?) and state = 'waiting' order by nextFireTime asc");
    List<ODocument> result = database.command(query).execute(noLaterThanDate);

    if (log.isInfoEnabled()) {
      log.info("Found {} triggers which are eligible to be run.", result.size());
    }

    return result;
  }

  public int getCount() {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    return (int) database.countClass("Trigger");
  }

  public List<String> getGroupNames() {
    // TODO(keith): class and field names should come from external
    // constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select DISTINCT(keyGroup) from Trigger");

    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<String> result = database.command(query).execute();

    return result;
  }

  public String getState(TriggerKey triggerKey) {
    ODocument doc = findTrigger(triggerKey);
    return (String) doc.field(Constants.TRIGGER_STATE);
  }

  public OperableTrigger getTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    ODocument doc = findTrigger(triggerKey);
    if (doc != null) {
      return triggerConverter.toTrigger(triggerKey, doc);
    } else {
      return null;
    }
  }

  public List<OperableTrigger> getTriggersForJob(ODocument doc) throws JobPersistenceException {
    final List<OperableTrigger> triggers = new LinkedList<OperableTrigger>();
    if (doc != null) {
      for (ODocument item : findByJobId(doc.getIdentity())) {
        triggers.add(triggerConverter.toTrigger(item));
      }
    }
    return triggers;
  }

  public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) {
    Set<TriggerKey> keys = new HashSet<>();
    for (ODocument doc : findMatchingDocuments(matcher)) {
      keys.add(Keys.toTriggerKey(doc));
    }

    return keys;
  }

  public Set<String> getTriggerGroupsThatMatch(GroupMatcher<TriggerKey> matcher) {
    Set<String> keys = new HashSet<>();
    for (ODocument doc : findMatchingDocuments(matcher)) {
      keys.add((String) doc.field(Constants.KEY_GROUP));
    }

    return keys;
  }

  public boolean hasLastTrigger(ODocument job) {
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select from Trigger where jobId=? limit 2");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> referencedTriggers = database.command(query).execute(job.getIdentity());

    return referencedTriggers.size() == 1;
  }

  public void insert(ODocument trigger, Trigger offendingTrigger)
      throws ObjectAlreadyExistsException {
    try {
      trigger.save();
    } catch (Exception key) {
      throw new ObjectAlreadyExistsException(offendingTrigger);
    }
  }

  public void remove(TriggerKey triggerKey) {
    for (ODocument trigger : getTriggersByKey(triggerKey)) {
      trigger.delete();
    }
  }

  public void remove(ODocument trigger) {
    trigger.delete();
  }

  public void removeByJobId(ORID jobId) {
    for (ODocument trigger : findByJobId(jobId)) {
      trigger.delete();
    }
  }

  public void replace(TriggerKey triggerKey, ODocument triggerUpdate) {
    if (log.isInfoEnabled()) {
      log.info("Replacing trigger {} triggers wwith data {} at {}", triggerKey, triggerUpdate, new Date());
    }
    for (ODocument trigger : getTriggersByKey(triggerKey)) {
      trigger.merge(triggerUpdate, true, true).save();
    }
  }

  public void setState(TriggerKey triggerKey, String state) {
    for (ODocument trigger : getTriggersByKey(triggerKey)) {
      trigger.field(Constants.TRIGGER_STATE, state).save();
    }
  }

  public void setStateInAll(String state) {
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Trigger");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> triggers = database.command(query).execute();

    setStates(triggers, state);
  }

  public void setStateByJobId(ORID jobId, String state) {
    setStates(findByJobId(jobId), state);
  }

  public void setStateInGroups(Set<String> groups, String state) {
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select from Trigger where " + queryHelper.inGroups(groups));
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> triggers = database.command(query).execute();

    setStates(triggers, state);
  }

  public void setStateInMatching(GroupMatcher<TriggerKey> matcher, String state) {
    setStates(findMatchingDocuments(matcher), state);
  }

  public Set<String> groupsOfMatching(GroupMatcher<TriggerKey> matcher) {
    String groupMatcherClause = queryHelper.matchingKeysConditionFor(matcher);
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select DISTINCT(keyGroup) from Trigger where " + groupMatcherClause);

    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();

    List<String> groups = database.query(query);

    return new HashSet<String>(groups);

  }

  /**
   * Get all trigger group names by job ID.
   * 
   * @param jobId
   *          the job ID
   * 
   * @return the set of group names
   */
  public Set<String> getGroupsByJobId(ORID jobId) {
    // TODO(keith): class and field names should come from external
    // constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select DISTINCT(keyGroup) from Trigger where jobId=?");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<String> result = database.command(query).execute(jobId);

    return new HashSet<String>(result);
  }

  private List<ODocument> findByJobId(ORID jobId) {
    // TODO(keith): class and field names should come from external
    // constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select from Trigger where jobId=?");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> result = database.command(query).execute(jobId);

    return result;
  }

  /**
   * Find a trigger by its trigger key.
   * 
   * @param triggerKey
   *          the trigger key
   * 
   * @return the trigger for the key, or {@code null} if no such trigger
   */
  public ODocument findTrigger(TriggerKey triggerKey) {
    List<ODocument> triggers = getTriggersByKey(triggerKey);

    if (!triggers.isEmpty()) {
      return triggers.get(0);
    } else {
      return null;
    }
  }

  /**
   * Set the state for all supplied triggers.
   * 
   * @param triggers
   *          the triggers
   * @param state
   *          the new state
   */
  private void setStates(List<ODocument> triggers, String state) {
    for (ODocument trigger : triggers) {
      trigger.field(Constants.TRIGGER_STATE, state).save();
    }
  }

  private List<ODocument> getTriggersByKey(TriggerKey triggerKey) {
    // TODO(keith): class and field names should come from external
    // constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select from Trigger where keyGroup=? and keyName=?");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> result =
        database.command(query).execute(triggerKey.getGroup(), triggerKey.getName());

    return result;
  }

  private List<ODocument> findMatchingDocuments(GroupMatcher<TriggerKey> matcher) {
    String groupMatcherClause = queryHelper.matchingKeysConditionFor(matcher);
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select from Trigger where " + groupMatcherClause);

    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();

    List<ODocument> documents = database.query(query);

    return documents;
  }
}
