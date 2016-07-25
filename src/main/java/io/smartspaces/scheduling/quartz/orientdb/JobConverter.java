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

import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.KEY_GROUP;
import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.KEY_NAME;

import java.io.IOException;

import org.bson.Document;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.spi.ClassLoadHelper;

import com.orientechnologies.orient.core.record.impl.ODocument;

import io.smartspaces.scheduling.quartz.orientdb.util.SerialUtils;

/**
 * A converter between Quartz job descriptions and and OrientDB records.
 */
public class JobConverter {

  public static final String JOB_DURABILITY = "durability";

  private static final String JOB_CLASS = "jobClass";

  private static final String JOB_DESCRIPTION = "jobDescription";

  public static final String JOB_REQUESTS_RECOVERY = "requestsRecovery";

  private ClassLoadHelper loadHelper;

  public JobConverter(ClassLoadHelper loadHelper) {
    this.loadHelper = loadHelper;
  }

  public ODocument toDocument(JobDetail newJob, JobKey key) {
    ODocument job = new ODocument("Job");

    job.field(KEY_NAME, key.getName());
    job.field(KEY_GROUP, key.getGroup());
    job.field(JOB_DESCRIPTION, newJob.getDescription());
    job.field(JOB_CLASS, newJob.getJobClass().getName());
    job.field(JOB_DURABILITY, newJob.isDurable());
    job.field(JOB_REQUESTS_RECOVERY, newJob.requestsRecovery());
    job.fromMap(newJob.getJobDataMap());

    return job;
  }

  public JobDetail toJobDetail(ODocument doc) throws JobPersistenceException {
    String jobClassName = doc.field(JOB_CLASS);
    try {
      // Make it possible for subclasses to use custom class loaders.
      // When Quartz jobs are implemented as Clojure records, the only way to
      // use
      // them without switching to gen-class is by using a
      // clojure.lang.DynamicClassLoader instance.
      @SuppressWarnings("unchecked")
      Class<Job> jobClass = (Class<Job>) loadHelper.getClassLoader().loadClass(jobClassName);

      JobBuilder builder = createJobBuilder(doc, jobClass);
      withDurability(doc, builder);
      withRequestsRecovery(doc, builder);
      JobDataMap jobData = createJobDataMap(doc);
      return builder.usingJobData(jobData).build();
    } catch (ClassNotFoundException e) {
      throw new JobPersistenceException("Could not load job class " + jobClassName, e);
    } catch (IOException e) {
      throw new JobPersistenceException("Could not load job class " + jobClassName, e);
    }
  }

  private JobDataMap createJobDataMap(ODocument doc) throws IOException {
    JobDataMap jobData = new JobDataMap();

    String jobDataString = doc.field(Constants.JOB_DATA);
    if (jobDataString != null) {
      jobData.putAll(SerialUtils.deserialize(jobData, jobDataString));
    } else {
      for (String key : doc.fieldNames()) {
        if (!key.equals(KEY_NAME) && !key.equals(KEY_GROUP) && !key.equals(JOB_CLASS)
            && !key.equals(JOB_DESCRIPTION) && !key.equals(JOB_DURABILITY)
            && !key.equals(JOB_REQUESTS_RECOVERY) && !key.equals("_id")) {
          jobData.put(key, doc.field(key));
        }
      }
    }

    jobData.clearDirtyFlag();
    return jobData;
  }

  private void withDurability(ODocument doc, JobBuilder builder) throws JobPersistenceException {
    Object jobDurability = doc.field(JOB_DURABILITY);
    if (jobDurability != null) {
      if (jobDurability instanceof Boolean) {
        builder.storeDurably((Boolean) jobDurability);
      } else if (jobDurability instanceof String) {
        builder.storeDurably(Boolean.valueOf((String) jobDurability));
      } else {
        throw new JobPersistenceException("Illegal value for " + JOB_DURABILITY + ", class "
            + jobDurability.getClass() + " not supported");
      }
    }
  }

  private void withRequestsRecovery(ODocument doc, JobBuilder builder) {
    boolean requestRecovery = false;
    Boolean requestRecoveryField = doc.field(JOB_REQUESTS_RECOVERY);
    if (requestRecoveryField != null) {
      requestRecovery = requestRecoveryField;
    }
    builder.requestRecovery(requestRecovery);
  }

  private JobBuilder createJobBuilder(ODocument doc, Class<Job> jobClass) {
    String keyName = doc.field(KEY_NAME);
    String keyGroup = doc.field(KEY_GROUP);
    String jobDescription = doc.field(JOB_DESCRIPTION);

    return JobBuilder.newJob(jobClass).withIdentity(keyName, keyGroup)
        .withDescription(jobDescription);
  }
}
