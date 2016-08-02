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

package io.smartspaces.scheduling.quartz.orientdb.impl.dao;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import io.smartspaces.scheduling.quartz.orientdb.impl.Constants;
import io.smartspaces.scheduling.quartz.orientdb.impl.StandardOrientDbStoreAssembler;
import io.smartspaces.scheduling.quartz.orientdb.impl.util.QueryHelper;

public class StandardPausedJobGroupsDao {

  private final StandardOrientDbStoreAssembler storeAssembler;

  private final QueryHelper queryHelper;

  public StandardPausedJobGroupsDao(StandardOrientDbStoreAssembler storeAssembler,
      QueryHelper queryHelper) {
    this.storeAssembler = storeAssembler;
    this.queryHelper = queryHelper;
  }

  public List<String> getPausedGroups() {
    // TODO(keith): class and field names should come from external constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select DISTINCT(keyGroup) from PausedJobGroup");

    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<String> result = database.command(query).execute();

    return result;
  }

  public void pauseGroups(Set<String> groups) {
    if (groups == null) {
      throw new IllegalArgumentException("groups cannot be null!");
    }

    for (String s : groups) {
      new ODocument("PausedJobGroups").field(Constants.KEY_GROUP, s).save();
    }
  }

  public void removeAll() {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    for (ODocument pausedJobGroup : database.browseClass("PausedJobGroup")) {
      pausedJobGroup.delete();
    }
  }

  public void unpauseGroups(Collection<String> groups) {
    // TODO(keith): class and field names should come from external constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select from PausedJobGroup where " + queryHelper.inGroups(groups));

    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> result = database.command(query).execute();

    for (ODocument pausedJobGroup : result) {
      pausedJobGroup.delete();
    }
  }
}
