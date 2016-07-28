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

package io.smartspaces.scheduling.quartz.orientdb.dao;

import static com.mongodb.client.model.Sorts.ascending;
import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.KEY_GROUP;
import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.toFilter;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.Constants;
import io.smartspaces.scheduling.quartz.orientdb.trigger.TriggerConverter;
import io.smartspaces.scheduling.quartz.orientdb.util.Keys;
import io.smartspaces.scheduling.quartz.orientdb.util.QueryHelper;

public class TriggerDao {

  private static final Logger log = LoggerFactory.getLogger(TriggerDao.class);

  private MongoCollection<Document> triggerCollection;

  private QueryHelper queryHelper;

  private TriggerConverter triggerConverter;

  public TriggerDao(MongoCollection<Document> triggerCollection, QueryHelper queryHelper,
      TriggerConverter triggerConverter) {
    this.triggerCollection = triggerCollection;
    this.queryHelper = queryHelper;
    this.triggerConverter = triggerConverter;
  }

  public void createIndex() {
    triggerCollection.createIndex(Keys.KEY_AND_GROUP_FIELDS, new IndexOptions().unique(true));
  }

  public void dropIndex() {
    triggerCollection.dropIndex("keyName_1_keyGroup_1");
  }

  public void clear() {
    triggerCollection.deleteMany(new Document());
  }

  public MongoCollection<Document> getCollection() {
    return triggerCollection;
  }

  public boolean exists(Bson filter) {
    return triggerCollection.count(filter) > 0;
  }

  public FindIterable<Document> findEligibleToRun(Date noLaterThanDate) {
    Bson query = createNextTriggerQuery(noLaterThanDate);
    if (log.isInfoEnabled()) {
      log.info("Found {} triggers which are eligible to be run.", getCount(query));
    }
    return triggerCollection.find(query).sort(ascending(Constants.TRIGGER_NEXT_FIRE_TIME));
  }

  public Document findTrigger(Bson filter) {
    return triggerCollection.find(filter).first();
  }

  public int getCount() {
    return (int) triggerCollection.count();
  }

  public List<String> getGroupNames() {
    return triggerCollection.distinct(KEY_GROUP, String.class).into(new ArrayList<String>());
  }

  public String getState(TriggerKey triggerKey) {
    Document doc = findTrigger(triggerKey);
    return doc.getString(Constants.TRIGGER_STATE);
  }

  public OperableTrigger getTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    Document doc = findTrigger(Keys.toFilter(triggerKey));
    if (doc == null) {
      return null;
    }
    return triggerConverter.toTrigger(triggerKey, doc);
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
    Set<TriggerKey> keys = new HashSet<TriggerKey>();
    Bson query = queryHelper.matchingKeysConditionFor(matcher);
    for (Document doc : triggerCollection.find(query).projection(Keys.KEY_AND_GROUP_FIELDS)) {
      keys.add(Keys.toTriggerKey(doc));
    }
    return keys;
  }

  public boolean hasLastTrigger(Document job) {
    List<Document> referencedTriggers =
        triggerCollection.find(Filters.eq(Constants.TRIGGER_JOB_ID, job.get("_id"))).limit(2)
            .into(new ArrayList<Document>(2));
    return referencedTriggers.size() == 1;
  }

  public void insert(ODocument trigger, Trigger offendingTrigger)
      throws ObjectAlreadyExistsException {
    try {
      triggerCollection.insertOne(trigger);
    } catch (MongoWriteException key) {
      throw new ObjectAlreadyExistsException(offendingTrigger);
    }
  }

  public void remove(Bson filter) {
    triggerCollection.deleteMany(filter);
  }

  public void remove(TriggerKey triggerKey) {
    remove(toFilter(triggerKey));
  }

  public void removeByJobId(Object id) {
    triggerCollection.deleteMany(Filters.eq(Constants.TRIGGER_JOB_ID, id));
  }

  public void replace(TriggerKey triggerKey, ODocument trigger) {
    triggerCollection.replaceOne(toFilter(triggerKey), trigger);
  }

  public void setState(TriggerKey triggerKey, String state) {
    triggerCollection.updateOne(Keys.toFilter(triggerKey), createTriggerStateUpdateDocument(state));
  }

  public void setStateInAll(String state) {
    setStates(new Document(), state);
  }

  public void setStateByJobId(ObjectId jobId, String state) {
    setStates(new Document(Constants.TRIGGER_JOB_ID, jobId), state);
  }

  public void setStateInGroups(List<String> groups, String state) {
    setStates(queryHelper.inGroups(groups), state);
  }

  public void setStateInMatching(GroupMatcher<TriggerKey> matcher, String state) {
    setStates(matcher, state);
  }

  private Bson createNextTriggerQuery(Date noLaterThanDate) {
    return Filters.and(
        Filters.or(Filters.eq(Constants.TRIGGER_NEXT_FIRE_TIME, null),
            Filters.lte(Constants.TRIGGER_NEXT_FIRE_TIME, noLaterThanDate)),
        Filters.eq(Constants.TRIGGER_STATE, Constants.STATE_WAITING));
  }

  private Bson createTriggerStateUpdateDocument(String state) {
    return new Document("$set", new Document(Constants.TRIGGER_STATE, state));
  }

  private FindIterable<ODocument> findByJobId(ORID jobId) {
    return triggerCollection.find(Filters.eq(Constants.TRIGGER_JOB_ID, jobId));
  }

  private Document findTrigger(TriggerKey key) {
    return findTrigger(toFilter(key));
  }

  private long getCount(Bson query) {
    return triggerCollection.count(query);
  }

  private void setStates(Bson filter, String state) {
    triggerCollection.updateMany(filter, createTriggerStateUpdateDocument(state));
  }

  private void setStates(GroupMatcher<TriggerKey> matcher, String state) {
    triggerCollection.updateMany(queryHelper.matchingKeysConditionFor(matcher),
        createTriggerStateUpdateDocument(state), new UpdateOptions().upsert(false));
  }
}
