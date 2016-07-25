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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import static io.smartspaces.scheduling.quartz.orientdb.util.Keys.KEY_GROUP;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class PausedTriggerGroupsDao {

  private final MongoCollection<Document> triggerGroupsCollection;

  public PausedTriggerGroupsDao(MongoCollection<Document> triggerGroupsCollection) {
    this.triggerGroupsCollection = triggerGroupsCollection;
  }

  public HashSet<String> getPausedGroups() {
    return triggerGroupsCollection.distinct(KEY_GROUP, String.class).into(new HashSet<String>());
  }

  public void pauseGroups(Collection<String> groups) {
    List<Document> list = new ArrayList<Document>();
    for (String s : groups) {
      list.add(new Document(KEY_GROUP, s));
    }
    triggerGroupsCollection.insertMany(list);
  }

  public void remove() {
    triggerGroupsCollection.deleteMany(new Document());
  }

  public void unpauseGroups(Collection<String> groups) {
    triggerGroupsCollection.deleteMany(Filters.in(KEY_GROUP, groups));
  }
}
