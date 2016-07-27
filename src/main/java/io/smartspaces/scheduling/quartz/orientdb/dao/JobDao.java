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

import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.KEY_GROUP;
import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.toFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.impl.matchers.GroupMatcher;

import com.mongodb.MongoWriteException;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import io.smartspaces.scheduling.quartz.orientdb.JobConverter;
import io.smartspaces.scheduling.quartz.orientdb.StandardOrientDbStoreAssembler;
import io.smartspaces.scheduling.quartz.orientdb.util.GroupHelper;
import io.smartspaces.scheduling.quartz.orientdb.util.Keys;
import io.smartspaces.scheduling.quartz.orientdb.util.QueryHelper;

public class JobDao {

  private final StandardOrientDbStoreAssembler storeAssembler;
  private final QueryHelper queryHelper;
  private final GroupHelper groupHelper;
  private final JobConverter jobConverter;

  public JobDao(StandardOrientDbStoreAssembler assembler, QueryHelper queryHelper,
      JobConverter jobConverter) {
    this.queryHelper = queryHelper;
    this.groupHelper = new GroupHelper(jobCollection, queryHelper);
    this.jobConverter = jobConverter;
  }

  public void clear() {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    for (ODocument job : database.browseClass("Job")) {
      job.delete();
    }
  }

  public void createIndex() {
    jobCollection.createIndex(Keys.KEY_AND_GROUP_FIELDS, new IndexOptions().unique(true));
  }

  public void dropIndex() {
    jobCollection.dropIndex("keyName_1_keyGroup_1");
  }

  public boolean exists(JobKey jobKey) {
    return !getJobsByKey(jobKey).isEmpty();
  }

  public ODocument getById(Object id) {
    return jobCollection.find(Filters.eq("_id", id)).first();
  }

  public ODocument getJob(JobKey jobKey) {
    List<ODocument> result = getJobsByKey(jobKey);
      
    if (result.isEmpty()) {
      return null;
    } else {
    return result.get(0);
    }
  }

  private List<ODocument> getJobsByKey(JobKey jobKey) {
    // TODO(keith): class and field names should come from external constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Job where keyGroup=? and keyName=?");
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> result = database.command(query).execute(jobKey.getGroup(), jobKey.getName());
    return result;
  }

  public int getCount() {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    return  (int)database.countClass("Job");
  }

  public List<String> getGroupNames() {
    return jobCollection.distinct(KEY_GROUP, String.class).into(new ArrayList<String>());
  }

  public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) {
    Set<JobKey> keys = new HashSet<JobKey>();
    Bson query = queryHelper.matchingKeysConditionFor(matcher);
    for (Document doc : jobCollection.find(query).projection(Keys.KEY_AND_GROUP_FIELDS)) {
      keys.add(Keys.toJobKey(doc));
    }
    return keys;
  }

  public Collection<ORID> idsOfMatching(GroupMatcher<JobKey> matcher) {
    List<ORID> list = new ArrayList<>();
    for (ODocument doc : findMatching(matcher)) {
      list.add(doc.getIdentity());
    }
    return list;
  }

  public void remove(Bson keyObject) {
    jobCollection.deleteMany(keyObject);
  }

  public boolean requestsRecovery(JobKey jobKey) {
    // TODO check if it's the same as getJobDataMap?
    ODocument jobDoc = getJob(jobKey);
    return jobDoc.getBoolean(JobConverter.JOB_REQUESTS_RECOVERY, false);
  }

  public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
    ODocument doc = getJob(jobKey);
    if (doc == null) {
      // Return null if job does not exist, per interface
      return null;
    }
    return jobConverter.toJobDetail(doc);
  }

  public ORID storeJobInMongo(JobDetail newJob, boolean replaceExisting)
      throws ObjectAlreadyExistsException {
    JobKey key = newJob.getKey();


    ODocument job = jobConverter.toDocument(newJob, key);
 
    ODocument object = getJob(key);

    ObjectId objectId = null;
    if (object != null && replaceExisting) {
      jobCollection.replaceOne(keyDbo, job);
    } else if (object == null) {
      try {
        jobCollection.insertOne(job);
        objectId = job.getObjectId("_id");
      } catch (MongoWriteException e) {
        // Fine, find it and get its id.
        object = getJob(keyDbo);
        objectId = object.getObjectId("_id");
      }
    } else {
      objectId = object.getObjectId("_id");
    }

    return objectId;
  }

  private Collection<Document> findMatching(GroupMatcher<JobKey> matcher) {
    return groupHelper.inGroupsThatMatch(matcher);
  }
}
