package io.smartspaces.scheduling.quartz.orientdb.db;

import org.quartz.SchedulerConfigException;

import com.mongodb.MongoClientOptions;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * The responsibility of this class is create an OrientDB connection with given
 * parameters.
 */
public class StandardOrientDbConnector {

  public static OrientDbConnectorBuilder builder() {
    return new OrientDbConnectorBuilder();
  }

  private OPartitionedDatabasePool pool;

  private final ThreadLocal<ODatabaseDocumentTx> documentProvider =
      new ThreadLocal<ODatabaseDocumentTx>() {
        @Override
        protected ODatabaseDocumentTx initialValue() {
          return pool.acquire();
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

  //public MongoDatabase selectDatabase(String dbName) {
  //  return mongo.getDatabase(dbName);
  //}

  /**
   * Get the connection to the database.
   * 
   * <p>
   * The first time the connection is obtained in a thread, a transaction will be started.
   * 
   * @return the connection
   */
  public ODatabaseDocumentTx getConnection() {
    return documentProvider.get();
  }
  
  public voif

  public static class OrientDbConnectorBuilder {
    private StandardOrientDbConnector connector = new StandardOrientDbConnector();
    private MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();

    private String orientdbUri;
    private String username;
    private String password;
    private String[] addresses;
    private String dbName;
    private String authDbName;
    private int writeTimeout;

    public StandardOrientDbConnector build() throws SchedulerConfigException {
      connect();
      return connector;
    }

    public OrientDbConnectorBuilder withClient(OPartitionedDatabasePool pool) {
      connector.pool = pool;
      return this;
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

    public OrientDbConnectorBuilder withAddresses(String[] addresses) {
      this.addresses = addresses;
      return this;
    }

    private void connect() throws SchedulerConfigException {
      if (connector.pool == null) {
        initializeMongo();
      } else {
        if (orientdbUri != null || username != null || password != null || addresses != null) {
          throw new SchedulerConfigException(
              "Configure either a Mongo instance or MongoDB connection parameters.");
        }
      }
    }

    private void initializeMongo() throws SchedulerConfigException {
      connector.pool = connectToMongoDB();
      if (connector.pool == null) {
        throw new SchedulerConfigException(
            "Could not connect to MongoDB! Please check that quartz-mongodb configuration is correct.");
      }
      setWriteConcern();
    }

    private OPartitionedDatabasePool connectToMongoDB() throws SchedulerConfigException {
      if (orientdbUri == null && (addresses == null || addresses.length == 0)) {
        throw new SchedulerConfigException(
            "At least one MongoDB address or a MongoDB URI must be specified .");
      }

      if (orientdbUri != null) {
        return connectToMongoDB(orientdbUri);
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
    // throw new SchedulerConfigException("Could not connect to MongoDB", e);
    // }
    // }
    //
    // private MongoClientOptions createOptions() {
    // return optionsBuilder.build();
    // }
    //
    // private List<MongoCredential> createCredentials() {
    // List<MongoCredential> credentials = new ArrayList<MongoCredential>(1);
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

    private OPartitionedDatabasePool connectToMongoDB(final String orientdbUriAsString)
        throws SchedulerConfigException {
      try {
        return new OPartitionedDatabasePool(orientdbUri, username, password);
      } catch (Throwable e) {
        throw new SchedulerConfigException("OrientDB driver thrown an exception", e);
      }
    }

    private void setWriteConcern() {
      // Use MAJORITY to make sure that writes (locks, updates, check-ins)
      // are propagated to secondaries in a Replica Set. It allows us to
      // have consistent state in case of failure of the primary.
      //
      // Since MongoDB 3.2, when MAJORITY is used and protocol version == 1
      // for replica set, then Journaling in enabled by default for primary
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
        optionsBuilder.connectionsPerHost(maxConnectionsPerHost);
      }
      return this;
    }

    public OrientDbConnectorBuilder withConnectTimeoutMillis(Integer connectTimeoutMillis) {
      if (connectTimeoutMillis != null) {
        optionsBuilder.connectTimeout(connectTimeoutMillis);
      }
      return this;
    }

    public OrientDbConnectorBuilder withSocketTimeoutMillis(Integer socketTimeoutMillis) {
      if (socketTimeoutMillis != null) {
        optionsBuilder.socketTimeout(socketTimeoutMillis);
      }
      return this;
    }

    public OrientDbConnectorBuilder withSocketKeepAlive(Boolean socketKeepAlive) {
      if (socketKeepAlive != null) {
        optionsBuilder.socketKeepAlive(socketKeepAlive);
      }
      return this;
    }

    public OrientDbConnectorBuilder withThreadsAllowedToBlockForConnectionMultiplier(
        Integer threadsAllowedToBlockForConnectionMultiplier) {
      if (threadsAllowedToBlockForConnectionMultiplier != null) {
        optionsBuilder.threadsAllowedToBlockForConnectionMultiplier(
            threadsAllowedToBlockForConnectionMultiplier);
      }
      return this;
    }

    public OrientDbConnectorBuilder withSSL(Boolean enableSSL, Boolean sslInvalidHostNameAllowed) {
      if (enableSSL != null) {
        optionsBuilder.sslEnabled(enableSSL);
        if (sslInvalidHostNameAllowed != null) {
          optionsBuilder.sslInvalidHostNameAllowed(sslInvalidHostNameAllowed);
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
