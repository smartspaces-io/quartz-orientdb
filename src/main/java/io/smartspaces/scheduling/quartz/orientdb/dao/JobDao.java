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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.result.DeleteResult;
import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.JobConverter;
import io.smartspaces.scheduling.quartz.orientdb.util.GroupHelper;
import io.smartspaces.scheduling.quartz.orientdb.util.Keys;
import io.smartspaces.scheduling.quartz.orientdb.util.QueryHelper;

public class JobDao {

	private final MongoCollection<Document> jobCollection;
	private final QueryHelper queryHelper;
	private final GroupHelper groupHelper;
	private final JobConverter jobConverter;

	public JobDao(MongoCollection<Document> jobCollection, QueryHelper queryHelper, JobConverter jobConverter) {
		this.jobCollection = jobCollection;
		this.queryHelper = queryHelper;
		this.groupHelper = new GroupHelper(jobCollection, queryHelper);
		this.jobConverter = jobConverter;
	}

	public MongoCollection<Document> getCollection() {
		return jobCollection;
	}

	public DeleteResult clear() {
		return jobCollection.deleteMany(new Document());
	}

	public void createIndex() {
		jobCollection.createIndex(Keys.KEY_AND_GROUP_FIELDS, new IndexOptions().unique(true));
	}

	public void dropIndex() {
		jobCollection.dropIndex("keyName_1_keyGroup_1");
	}

	public boolean exists(JobKey jobKey) {
		return jobCollection.count(Keys.toFilter(jobKey)) > 0;
	}

	public Document getById(Object id) {
		return jobCollection.find(Filters.eq("_id", id)).first();
	}

	public ODocument getJob(Bson keyObject) {
		return jobCollection.find(keyObject).first();
	}

	public ODocument getJob(JobKey key) {
		return getJob(toFilter(key));
	}

	public int getCount() {
		return (int) jobCollection.count();
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

	public Collection<ObjectId> idsOfMatching(GroupMatcher<JobKey> matcher) {
		List<ObjectId> list = new ArrayList<ObjectId>();
		for (Document doc : findMatching(matcher)) {
			list.add(doc.getObjectId("_id"));
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

	public ObjectId storeJobInMongo(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException {
		JobKey key = newJob.getKey();

		Bson keyDbo = toFilter(key);
		ODocument job = jobConverter.toDocument(newJob, key);

		ODocument object = getJob(keyDbo);

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
