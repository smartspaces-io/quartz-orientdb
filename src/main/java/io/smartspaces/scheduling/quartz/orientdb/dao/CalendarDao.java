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

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.quartz.Calendar;
import org.quartz.JobPersistenceException;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;

import io.smartspaces.scheduling.quartz.orientdb.db.StandardOrientDbConnector;
import io.smartspaces.scheduling.quartz.orientdb.util.SerialUtils;

public class CalendarDao {

    static final String CALENDAR_NAME = "name";
    static final String CALENDAR_SERIALIZED_OBJECT = "serializedObject";

    private final StandardOrientDbConnector orientDbConnection;

    public CalendarDao(StandardOrientDbConnector orientDbConnection) {
        this.orientDbConnection = orientDbConnection;
    }

    public void clear() {
        calendarCollection.deleteMany(new Document());
    }

    public void createIndex() {
        calendarCollection.createIndex(
                Projections.include(CALENDAR_NAME),
                new IndexOptions().unique(true));
    }

    public MongoCollection<Document> getCollection() {
        return calendarCollection;
    }

    public int getCount() {
        return (int) calendarCollection.count();
    }

    public boolean remove(String name) {
        Bson searchObj = Filters.eq(CALENDAR_NAME, name);
        if (calendarCollection.count(searchObj) > 0) {
            calendarCollection.deleteMany(searchObj);
            return true;
        }
        return false;
    }

    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
        if (calName != null) {
            Bson searchObj = Filters.eq(CALENDAR_NAME, calName);
            Document doc = calendarCollection.find(searchObj).first();
            if (doc != null) {
                Binary serializedCalendar = doc.get(CALENDAR_SERIALIZED_OBJECT, Binary.class);
                return SerialUtils.deserialize(serializedCalendar, Calendar.class);
            }
        }
        return null;
    }
    
    public void store(String name, Calendar calendar) throws JobPersistenceException {
        Document doc = new Document(CALENDAR_NAME, name)
                .append(CALENDAR_SERIALIZED_OBJECT, SerialUtils.serialize(calendar));
        calendarCollection.insertOne(doc);
    }
}
