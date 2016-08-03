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

package io.smartspaces.scheduling.quartz.orientdb.internal.db;

import org.quartz.JobPersistenceException;
import org.quartz.SchedulerConfigException;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OUser;

import io.smartspaces.scheduling.quartz.orientdb.internal.Constants;

/**
 * The responsibility of this class is create an OrientDB connection with given
 * parameters.
 */
public class StandardOrientDbConnector {

  public static OrientDbConnectorBuilder builder() {
    return new OrientDbConnectorBuilder();
  }

  /**
   * The pool of database connections.
   */
  private OPartitionedDatabasePool pool;

  /**
   * The thread local that will provide the database connection.
   */
  private final ThreadLocal<ODatabaseDocumentTx> documentProvider =
      new ThreadLocal<ODatabaseDocumentTx>() {
        @Override
        protected ODatabaseDocumentTx initialValue() {
          return newConnection();
        }
      };

  /**
   * Construct a new connector.
   * 
   * <p>
   * The builder must be used.
   */
  private StandardOrientDbConnector() {
    // use the builder
  }

  public void shutdown() {
    pool.close();
  }

  /**
   * Get a new connection to the database.
   * 
   * @return the new connection
   */
  private ODatabaseDocumentTx newConnection() {
    return pool.acquire();
  }

  /**
   * Get the connection to the database.
   * 
   * <p>
   * The first time the connection is obtained in a thread, a transaction will
   * be started.
   * 
   * @return the connection
   */
  public ODatabaseDocumentTx getConnection() {
    return documentProvider.get();
  }

  public <T> T doInTransaction(TransactionMethod<T> method) throws JobPersistenceException {
    ODatabaseDocumentTx db = getConnection();
    try {
      db.begin();

      T result = method.doInTransaction();

      db.commit();

      return result;
    } catch (JobPersistenceException e) {
      db.rollback();

      e.printStackTrace();

      throw e;
    } catch (Throwable e) {
      db.rollback();

      e.printStackTrace();

      throw new JobPersistenceException("Transaction failed", e);
    } finally {
      documentProvider.remove();
    }

  }

  public interface TransactionMethod<T> {
    T doInTransaction() throws JobPersistenceException;
  }

  public static class OrientDbConnectorBuilder {
    private StandardOrientDbConnector connector = new StandardOrientDbConnector();

    private String orientdbUri;
    private String username = "superdooper";
    private String password = "superdooper";
    private String dbName;
    private String authDbName;
    private int writeTimeout;

    public StandardOrientDbConnector build() throws SchedulerConfigException {
      connect();
      return connector;
    }

    public OrientDbConnectorBuilder withUri(String orientdbUri) {
      this.orientdbUri = orientdbUri;
      return this;
    }

    public OrientDbConnectorBuilder withCredentials(String username, String password) {
      this.username = username;
      this.password = password;
      return this;
    }

    private void connect() throws SchedulerConfigException {
      if (connector.pool == null) {
        initializeOrientDb();
      } else {
        if (orientdbUri != null || username != null || password != null) {
          throw new SchedulerConfigException(
              "Configure either a OrientDB instance or OrientDB connection parameters.");
        }
      }
    }

    private void initializeOrientDb() throws SchedulerConfigException {
      connector.pool = connectToOrientDb();
      if (connector.pool == null) {
        throw new SchedulerConfigException(
            "Could not connect to MongoDB! Please check that quartz-mongodb configuration is correct.");
      }
      setWriteConcern();
    }

    private OPartitionedDatabasePool connectToOrientDb() throws SchedulerConfigException {
      if (orientdbUri == null) {
        throw new SchedulerConfigException(
            "At least one OrientDB address or a OrientDB URI must be specified .");
      }

      if (orientdbUri != null) {
        return connectToOrientDB(orientdbUri);
      }

      // return createClient();
      throw new RuntimeException("Couldn't do any sort of connection to OrientDB");
    }

    // private MongoClient createClient() throws SchedulerConfigException {
    // MongoClientOptions options = createOptions();
    // List<MongoCredential> credentials = createCredentials();
    // List<ServerAddress> serverAddresses = collectServerAddresses();
    // try {
    // return new MongoClient(serverAddresses, credentials, options);
    // } catch (MongoException e) {

    // throw new SchedulerConfigExceDption("Could not connect to MongoDB",
    // e);
    // }
    // }
    //
    // private MongoClientOptions createOptions() {
    // return optionsBuilder.build();
    // }
    //
    // private List<MongoCredential> createCredentials() {
    // List<MongoCredential> credentials = new
    // ArrayList<MongoCredential>(1);
    // if (username != null) {
    // if (authDbName != null) {
    // // authenticating to db which gives access to all other dbs (role -
    // // readWriteAnyDatabase)
    // // by default in mongo it should be "admin"
    // credentials
    // .add(MongoCredential.createCredential(username, authDbName,
    // password.toCharArray()));
    // } else {
    // credentials
    // .add(MongoCredential.createCredential(username, dbName,
    // password.toCharArray()));
    // }
    // }
    // return credentials;
    // }
    //
    // private List<ServerAddress> collectServerAddresses() {
    // List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
    // for (String a : addresses) {
    // serverAddresses.add(new ServerAddress(a));
    // }
    // return serverAddresses;
    // }

    private OPartitionedDatabasePool connectToOrientDB(String orientdbUriAsString)
        throws SchedulerConfigException {
      try {
        checkDataBaseExists();

        return new OPartitionedDatabasePool(orientdbUri, username, password);
      } catch (Throwable e) {
        throw new SchedulerConfigException("OrientDB driver thrown an exception", e);
      }
    }

    /**
     * Create the database if necessary.
     * 
     * @param storeAssembler
     *          the store assembler that is assembling the database
     */
    public void checkDataBaseExists() {
      ODatabaseDocumentTx db = new ODatabaseDocumentTx(orientdbUri);
      if (!db.exists()) {
        db.create();

        OSecurity sm = db.getMetadata().getSecurity();
        OUser user = sm.createUser(username, password, new String[] { "admin" });

        // TODO(keith): MOve this elsewhere
        OSchema schema = db.getMetadata().getSchema();

        OClass jobClass = schema.createClass("Job");
        jobClass.createProperty(Constants.KEY_NAME, OType.STRING).setNotNull(true);
        jobClass.createProperty(Constants.KEY_GROUP, OType.STRING).setNotNull(true);
        jobClass.createProperty(Constants.JOB_DESCRIPTION, OType.STRING);
        jobClass.createProperty(Constants.JOB_CLASS, OType.STRING);
        jobClass.createProperty(Constants.JOB_DATA, OType.STRING);
        jobClass.createProperty(Constants.JOB_DURABILITY, OType.BOOLEAN);
        jobClass.createProperty(Constants.JOB_REQUESTS_RECOVERY, OType.BOOLEAN);

        jobClass.createIndex("JOBS.key_NAME.key_group", OClass.INDEX_TYPE.UNIQUE,
            Constants.KEY_GROUP, Constants.KEY_NAME);

        OClass triggerClass = schema.createClass("Trigger");
        triggerClass.createProperty(Constants.TRIGGER_CLASS, OType.STRING);
        triggerClass.createProperty(Constants.KEY_NAME, OType.STRING).setNotNull(true);
        triggerClass.createProperty(Constants.KEY_GROUP, OType.STRING).setNotNull(true);
        triggerClass.createProperty(Constants.TRIGGER_CALENDAR_NAME, OType.STRING);
        triggerClass.createProperty(Constants.TRIGGER_DESCRIPTION, OType.STRING);
        triggerClass.createProperty(Constants.TRIGGER_FIRE_INSTANCE_ID, OType.STRING);
        triggerClass.createProperty(Constants.TRIGGER_MISFIRE_INSTRUCTION, OType.INTEGER);
        triggerClass.createProperty(Constants.TRIGGER_NEXT_FIRE_TIME, OType.DATE);
        triggerClass.createProperty(Constants.TRIGGER_PREVIOUS_FIRE_TIME, OType.DATE);
        triggerClass.createProperty(Constants.TRIGGER_PRIORITY, OType.INTEGER);
        triggerClass.createProperty(Constants.TRIGGER_START_TIME, OType.DATE);
        triggerClass.createProperty(Constants.TRIGGER_END_TIME, OType.DATE);
        triggerClass.createProperty(Constants.TRIGGER_STATE, OType.STRING);
        triggerClass.createProperty(Constants.TRIGGER_FINAL_FIRE_TIME, OType.DATE);
        triggerClass.createProperty(Constants.TRIGGER_JOB_ID, OType.LINK, jobClass);
        triggerClass.createProperty(Constants.TRIGGER_CRON_EXPRESSION, OType.STRING);
        triggerClass.createProperty(Constants.TRIGGER_TIMEZONE, OType.STRING);

        triggerClass.createIndex("TRIGGERS.key_NAME.key_group", OClass.INDEX_TYPE.UNIQUE,
            Constants.KEY_GROUP, Constants.KEY_NAME);

        OClass lockClass = schema.createClass("QuartzLock");
        lockClass.createProperty(Constants.LOCK_TYPE, OType.STRING).setNotNull(true);
        lockClass.createProperty(Constants.KEY_GROUP, OType.STRING).setNotNull(true);
        lockClass.createProperty(Constants.KEY_NAME, OType.STRING).setNotNull(true);
        lockClass.createProperty(Constants.LOCK_INSTANCE_ID, OType.STRING);
        lockClass.createProperty(Constants.LOCK_TIME, OType.DATE);

        lockClass.createIndex("LOCKS.type_group_name", OClass.INDEX_TYPE.UNIQUE,
            Constants.KEY_GROUP, Constants.KEY_NAME, Constants.LOCK_TYPE);

        OClass schedulerClass = schema.createClass("Scheduler");
        schedulerClass.createProperty(Constants.SCHEDULER_NAME_FIELD, OType.STRING)
            .setNotNull(true);
        schedulerClass.createProperty(Constants.SCHEDULER_INSTANCE_ID_FIELD, OType.STRING)
            .setNotNull(true);
        schedulerClass.createProperty(Constants.SCHEDULER_LAST_CHECKIN_TIME_FIELD, OType.LONG);
        schedulerClass.createProperty(Constants.SCHEDULER_CHECKIN_INTERVAL_FIELD, OType.LONG);

        schedulerClass.createIndex("SCHEDULERS.name_instance", OClass.INDEX_TYPE.UNIQUE,
            Constants.SCHEDULER_NAME_FIELD, Constants.SCHEDULER_INSTANCE_ID_FIELD);

        OClass calendarClass = schema.createClass("Calendar");
        calendarClass.createProperty(Constants.CALENDAR_NAME, OType.STRING);
        calendarClass.createProperty(Constants.CALENDAR_SERIALIZED_OBJECT, OType.BINARY);

        calendarClass.createIndex("CALENDARS.NAME", OClass.INDEX_TYPE.UNIQUE,
            Constants.CALENDAR_NAME);

        OClass pausedJobGroupClass = schema.createClass("PausedJobGroup");
        pausedJobGroupClass.createProperty(Constants.KEY_GROUP, OType.STRING);

        OClass pausedTriggerGroupClass = schema.createClass("PausedTriggerGroup");
        pausedTriggerGroupClass.createProperty(Constants.KEY_GROUP, OType.STRING);
      }
    }

    private void setWriteConcern() {
      // Use MAJORITY to make sure that writes (locks, updates, check-ins)
      // are propagated to secondaries in a Replica Set. It allows us to
      // have consistent state in case of failure of the primary.
      //
      // Since MongoDB 3.2, when MAJORITY is used and protocol version ==
      // 1
      // for replica set, then Journaling in enabled by default for
      // primary
      // and secondaries.
      // WriteConcern writeConcern =
      // WriteConcern.MAJORITY.withWTimeout(writeTimeout,
      // TimeUnit.MILLISECONDS).withJournal(true);
      // connector.mongo.setWriteConcern(writeConcern);
    }

    public OrientDbConnectorBuilder withAuthDatabaseName(String authDbName) {
      this.authDbName = authDbName;
      return this;
    }

    public OrientDbConnectorBuilder withDatabaseName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public OrientDbConnectorBuilder withMaxConnectionsPerHost(Integer maxConnectionsPerHost) {
      if (maxConnectionsPerHost != null) {
        // optionsBuilder.connectionsPerHost(maxConnectionsPerHost);
      }
      return this;
    }

    public OrientDbConnectorBuilder withConnectTimeoutMillis(Integer connectTimeoutMillis) {
      if (connectTimeoutMillis != null) {
        // optionsBuilder.connectTimeout(connectTimeoutMillis);
      }
      return this;
    }

    public OrientDbConnectorBuilder withSocketTimeoutMillis(Integer socketTimeoutMillis) {
      if (socketTimeoutMillis != null) {
        // optionsBuilder.socketTimeout(socketTimeoutMillis);
      }
      return this;
    }

    public OrientDbConnectorBuilder withSocketKeepAlive(Boolean socketKeepAlive) {
      if (socketKeepAlive != null) {
        // optionsBuilder.socketKeepAlive(socketKeepAlive);
      }
      return this;
    }

    public OrientDbConnectorBuilder withThreadsAllowedToBlockForConnectionMultiplier(
        Integer threadsAllowedToBlockForConnectionMultiplier) {
      if (threadsAllowedToBlockForConnectionMultiplier != null) {
        // optionsBuilder.threadsAllowedToBlockForConnectionMultiplier(
        // threadsAllowedToBlockForConnectionMultiplier);
      }
      return this;
    }

    public OrientDbConnectorBuilder withSSL(Boolean enableSSL, Boolean sslInvalidHostNameAllowed) {
      if (enableSSL != null) {
        // optionsBuilder.sslEnabled(enableSSL);
        if (sslInvalidHostNameAllowed != null) {
          // optionsBuilder.sslInvalidHostNameAllowed(sslInvalidHostNameAllowed);
        }
      }
      return this;
    }

    public OrientDbConnectorBuilder withWriteTimeout(int writeTimeout) {
      this.writeTimeout = writeTimeout;
      return this;
    }
  }
}
