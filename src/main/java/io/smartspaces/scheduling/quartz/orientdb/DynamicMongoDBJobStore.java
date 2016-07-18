package io.smartspaces.scheduling.quartz.orientdb;

import com.mongodb.MongoClient;

import io.smartspaces.scheduling.quartz.orientdb.clojure.DynamicClassLoadHelper;

import org.quartz.spi.ClassLoadHelper;

public class DynamicMongoDBJobStore extends MongoDBJobStore {

    public DynamicMongoDBJobStore() {
        super();
    }

    public DynamicMongoDBJobStore(MongoClient mongo) {
        super(mongo);
    }

    public DynamicMongoDBJobStore(String orientdbUri, String username, String password) {
        super(orientdbUri, username, password);
    }

    @Override
    protected ClassLoadHelper getClassLoaderHelper(ClassLoadHelper original) {
        return new DynamicClassLoadHelper();
    }
}
