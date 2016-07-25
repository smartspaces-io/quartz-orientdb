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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.bson.types.ObjectId;
import org.quartz.JobKey;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;

import io.smartspaces.scheduling.quartz.orientdb.dao.JobDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.PausedJobGroupsDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.PausedTriggerGroupsDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.TriggerDao;
import io.smartspaces.scheduling.quartz.orientdb.util.GroupHelper;
import io.smartspaces.scheduling.quartz.orientdb.util.QueryHelper;
import io.smartspaces.scheduling.quartz.orientdb.util.TriggerGroupHelper;

public class TriggerStateManager {

  private final TriggerDao triggerDao;
  private final JobDao jobDao;
  private PausedJobGroupsDao pausedJobGroupsDao;
  private final PausedTriggerGroupsDao pausedTriggerGroupsDao;
  private final QueryHelper queryHelper;

  public TriggerStateManager(TriggerDao triggerDao, JobDao jobDao,
      PausedJobGroupsDao pausedJobGroupsDao, PausedTriggerGroupsDao pausedTriggerGroupsDao,
      QueryHelper queryHelper) {
    this.triggerDao = triggerDao;
    this.jobDao = jobDao;
    this.pausedJobGroupsDao = pausedJobGroupsDao;
    this.pausedTriggerGroupsDao = pausedTriggerGroupsDao;
    this.queryHelper = queryHelper;
  }

  public Set<String> getPausedTriggerGroups() {
    return pausedTriggerGroupsDao.getPausedGroups();
  }

  public TriggerState getState(TriggerKey triggerKey) {
    return getTriggerState(triggerDao.getState(triggerKey));
  }

  public void pause(TriggerKey triggerKey) {
    triggerDao.setState(triggerKey, Constants.STATE_PAUSED);
  }

  public Collection<String> pause(GroupMatcher<TriggerKey> matcher) {
    triggerDao.setStateInMatching(matcher, Constants.STATE_PAUSED);

    final GroupHelper groupHelper = new GroupHelper(triggerDao.getCollection(), queryHelper);
    final Set<String> set = groupHelper.groupsThatMatch(matcher);
    pausedTriggerGroupsDao.pauseGroups(set);

    return set;
  }

  public void pauseAll() {
    final GroupHelper groupHelper = new GroupHelper(triggerDao.getCollection(), queryHelper);
    triggerDao.setStateInAll(Constants.STATE_PAUSED);
    pausedTriggerGroupsDao.pauseGroups(groupHelper.allGroups());
  }

  public void pauseJob(JobKey jobKey) {
    final ObjectId jobId = jobDao.getJob(jobKey).getObjectId("_id");
    final TriggerGroupHelper groupHelper =
        new TriggerGroupHelper(triggerDao.getCollection(), queryHelper);
    List<String> groups = groupHelper.groupsForJobId(jobId);
    triggerDao.setStateByJobId(jobId, Constants.STATE_PAUSED);
    pausedTriggerGroupsDao.pauseGroups(groups);
  }

  public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) {
    final TriggerGroupHelper groupHelper =
        new TriggerGroupHelper(triggerDao.getCollection(), queryHelper);
    List<String> groups = groupHelper.groupsForJobIds(jobDao.idsOfMatching(groupMatcher));
    triggerDao.setStateInGroups(groups, Constants.STATE_PAUSED);
    pausedJobGroupsDao.pauseGroups(groups);
    return groups;
  }

  public void resume(TriggerKey triggerKey) {
    // TODO: port blocking behavior and misfired triggers handling from
    // StdJDBCDelegate in Quartz
    triggerDao.setState(triggerKey, Constants.STATE_WAITING);
  }

  public Collection<String> resume(GroupMatcher<TriggerKey> matcher) {
    triggerDao.setStateInMatching(matcher, Constants.STATE_WAITING);

    final GroupHelper groupHelper = new GroupHelper(triggerDao.getCollection(), queryHelper);
    final Set<String> set = groupHelper.groupsThatMatch(matcher);
    pausedTriggerGroupsDao.unpauseGroups(set);
    return set;
  }

  public void resume(JobKey jobKey) {
    final ObjectId jobId = jobDao.getJob(jobKey).getObjectId("_id");
    // TODO: port blocking behavior and misfired triggers handling from
    // StdJDBCDelegate in Quartz
    triggerDao.setStateByJobId(jobId, Constants.STATE_WAITING);
  }

  public void resumeAll() {
    final GroupHelper groupHelper = new GroupHelper(triggerDao.getCollection(), queryHelper);
    triggerDao.setStateInAll(Constants.STATE_WAITING);
    pausedTriggerGroupsDao.unpauseGroups(groupHelper.allGroups());
  }

  public Collection<String> resumeJobs(GroupMatcher<JobKey> groupMatcher) {
    final TriggerGroupHelper groupHelper =
        new TriggerGroupHelper(triggerDao.getCollection(), queryHelper);
    List<String> groups = groupHelper.groupsForJobIds(jobDao.idsOfMatching(groupMatcher));
    triggerDao.setStateInGroups(groups, Constants.STATE_WAITING);
    pausedJobGroupsDao.unpauseGroups(groups);
    return groups;
  }

  private TriggerState getTriggerState(String value) {
    if (value == null) {
      return TriggerState.NONE;
    }

    if (value.equals(Constants.STATE_DELETED)) {
      return TriggerState.NONE;
    }

    if (value.equals(Constants.STATE_COMPLETE)) {
      return TriggerState.COMPLETE;
    }

    if (value.equals(Constants.STATE_PAUSED)) {
      return TriggerState.PAUSED;
    }

    if (value.equals(Constants.STATE_PAUSED_BLOCKED)) {
      return TriggerState.PAUSED;
    }

    if (value.equals(Constants.STATE_ERROR)) {
      return TriggerState.ERROR;
    }

    if (value.equals(Constants.STATE_BLOCKED)) {
      return TriggerState.BLOCKED;
    }

    // waiting or acquired
    return TriggerState.NORMAL;
  }
}
