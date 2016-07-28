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

import java.util.List;

import org.quartz.Calendar;
import org.quartz.JobPersistenceException;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import io.smartspaces.scheduling.quartz.orientdb.StandardOrientDbStoreAssembler;
import io.smartspaces.scheduling.quartz.orientdb.util.SerialUtils;

public class StandardCalendarDao {

  static final String CALENDAR_NAME = "name";
  static final String CALENDAR_SERIALIZED_OBJECT = "serializedObject";

  private final StandardOrientDbStoreAssembler storeAssembler;

  public StandardCalendarDao(StandardOrientDbStoreAssembler storeAssembler) {
    this.storeAssembler = storeAssembler;
  }

  public void clear() {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    for (ODocument calendar : database.browseClass("Calendar")) {
      calendar.delete();
    }
  }

  public void createIndex() {
    // calendarCollection.createIndex(Projections.include(CALENDAR_NAME),
    // new IndexOptions().unique(true));
  }

  public int getCount() {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    return (int) database.countClass("Calendar");
  }

  public boolean remove(String calName) {
    List<ODocument> result = getCalendarsByName(calName);

    if (!result.isEmpty()) {
      result.get(0).delete();
      return true;
    }
    return false;
  }

  public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
    if (calName != null) {
      List<ODocument> result = getCalendarsByName(calName);
      if (!result.isEmpty()) {
        ORecordBytes serializedCalendar = result.get(0).field(CALENDAR_SERIALIZED_OBJECT);
        return SerialUtils.deserialize(serializedCalendar.toStream(), Calendar.class);
      }
    }
    return null;
  }

  public void store(String name, Calendar calendar) throws JobPersistenceException {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    
    ORecordBytes serializedCalendar = new ORecordBytes(SerialUtils.serialize(calendar));
    ODocument doc = new ODocument("Calendar").field(CALENDAR_NAME, name)
        .field(CALENDAR_SERIALIZED_OBJECT, serializedCalendar);
    doc.save();
  }
  

  private List<ODocument> getCalendarsByName(String name) {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();

    // TODO(keith): class and field names should come from external constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>("select from Calendar where name=?");
    List<ODocument> result = database.command(query).execute(name);
    return result;
  }
}
