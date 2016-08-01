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
import java.util.HashSet;
import java.util.Set;

import org.quartz.JobKey;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;

import com.orientechnologies.orient.core.id.ORID;

import io.smartspaces.scheduling.quartz.orientdb.dao.StandardJobDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.StandardPausedJobGroupsDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.StandardPausedTriggerGroupsDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.StandardTriggerDao;

public class TriggerStateManager {

  private final StandardTriggerDao triggerDao;
  private final StandardJobDao jobDao;
  private StandardPausedJobGroupsDao pausedJobGroupsDao;
  private final StandardPausedTriggerGroupsDao pausedTriggerGroupsDao;

  public TriggerStateManager(StandardTriggerDao triggerDao, StandardJobDao jobDao,
      StandardPausedJobGroupsDao pausedJobGroupsDao, StandardPausedTriggerGroupsDao pausedTriggerGroupsDao) {
    this.triggerDao = triggerDao;
    this.jobDao = jobDao;
    this.pausedJobGroupsDao = pausedJobGroupsDao;
    this.pausedTriggerGroupsDao = pausedTriggerGroupsDao;
  }

  public Set<String> getPausedTriggerGroups() {
    return new HashSet<String>(pausedTriggerGroupsDao.getPausedGroups());
  }

  public TriggerState getState(TriggerKey triggerKey) {
    return getTriggerState(triggerDao.getState(triggerKey));
  }

  public void pause(TriggerKey triggerKey) {
    triggerDao.setState(triggerKey, Constants.STATE_PAUSED);
  }

  public Set<String> pause(GroupMatcher<TriggerKey> matcher) {
    triggerDao.setStateInMatching(matcher, Constants.STATE_PAUSED);

    Set<String> set = triggerDao.getTriggerGroupsThatMatch(matcher);
    pausedTriggerGroupsDao.pauseGroups(set);

    return set;
  }

  public void pauseAll() {
    triggerDao.setStateInAll(Constants.STATE_PAUSED);
    pausedTriggerGroupsDao.pauseGroups(triggerDao.getGroupNames());
  }

  public void pauseJob(JobKey jobKey) {
    ORID jobId = jobDao.getJob(jobKey).getIdentity();
    triggerDao.setStateByJobId(jobId, Constants.STATE_PAUSED);
    Set<String> groups = triggerDao.getGroupsByJobId(jobId);
    pausedTriggerGroupsDao.pauseGroups(groups);
  }

  public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) {
    Set<String> groups = jobDao.groupsOfMatching(groupMatcher);
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

    Set<String> triggerGroupsThatMatch = triggerDao.getTriggerGroupsThatMatch(matcher);
    pausedTriggerGroupsDao.unpauseGroups(triggerGroupsThatMatch);
    return triggerGroupsThatMatch;
  }

  public void resume(JobKey jobKey) {
    ORID jobId = jobDao.getJob(jobKey).getIdentity();
    // TODO: port blocking behavior and misfired triggers handling from
    // StdJDBCDelegate in Quartz
    triggerDao.setStateByJobId(jobId, Constants.STATE_WAITING);
  }

  public void resumeAll() {
    triggerDao.setStateInAll(Constants.STATE_WAITING);
    pausedTriggerGroupsDao.removeAll();
  }

  public Set<String> resumeJobs(GroupMatcher<JobKey> groupMatcher) {
    Set<String> groups = jobDao.groupsOfMatching(groupMatcher);
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
