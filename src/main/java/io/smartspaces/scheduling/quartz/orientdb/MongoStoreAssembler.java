package io.smartspaces.scheduling.quartz.orientdb;

import org.bson.Document;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerSignaler;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import io.smartspaces.scheduling.quartz.orientdb.cluster.CheckinExecutor;
import io.smartspaces.scheduling.quartz.orientdb.cluster.CheckinTask;
import io.smartspaces.scheduling.quartz.orientdb.cluster.RecoveryTriggerFactory;
import io.smartspaces.scheduling.quartz.orientdb.cluster.TriggerRecoverer;
import io.smartspaces.scheduling.quartz.orientdb.dao.CalendarDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.JobDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.LocksDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.PausedJobGroupsDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.PausedTriggerGroupsDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.SchedulerDao;
import io.smartspaces.scheduling.quartz.orientdb.dao.TriggerDao;
import io.smartspaces.scheduling.quartz.orientdb.db.MongoConnector;
import io.smartspaces.scheduling.quartz.orientdb.trigger.MisfireHandler;
import io.smartspaces.scheduling.quartz.orientdb.trigger.TriggerConverter;
import io.smartspaces.scheduling.quartz.orientdb.util.Clock;
import io.smartspaces.scheduling.quartz.orientdb.util.ExpiryCalculator;
import io.smartspaces.scheduling.quartz.orientdb.util.QueryHelper;

public class MongoStoreAssembler {

    public MongoConnector mongoConnector;
    public JobCompleteHandler jobCompleteHandler;
    public LockManager lockManager;
    public TriggerStateManager triggerStateManager;
    public TriggerRunner triggerRunner;
    public TriggerAndJobPersister persister;

    public CalendarDao calendarDao;
    public JobDao jobDao;
    public LocksDao locksDao;
    public SchedulerDao schedulerDao;
    public PausedJobGroupsDao pausedJobGroupsDao;
    public PausedTriggerGroupsDao pausedTriggerGroupsDao;
    public TriggerDao triggerDao;

    public TriggerRecoverer triggerRecoverer;
    public CheckinExecutor checkinExecutor;

    private MongoDatabase db;
    private QueryHelper queryHelper = new QueryHelper();
    private TriggerConverter triggerConverter;

    public void build(MongoDBJobStore jobStore, ClassLoadHelper loadHelper, SchedulerSignaler signaler)
            throws SchedulerConfigException {
        mongoConnector = createMongoConnector(jobStore);

        db = mongoConnector.selectDatabase(jobStore.dbName);

        jobDao = createJobDao(jobStore, loadHelper);

        triggerConverter = new TriggerConverter(jobDao);

        triggerDao = createTriggerDao(jobStore);
        calendarDao = createCalendarDao(jobStore);
        locksDao = createLocksDao(jobStore);
        pausedJobGroupsDao = createPausedJobGroupsDao(jobStore);
        pausedTriggerGroupsDao = createPausedTriggerGroupsDao(jobStore);
        schedulerDao = createSchedulerDao(jobStore);

        persister = createTriggerAndJobPersister();

        jobCompleteHandler = createJobCompleteHandler(signaler);

        lockManager = createLockManager(jobStore);

        triggerStateManager = createTriggerStateManager();

        MisfireHandler misfireHandler = createMisfireHandler(jobStore, signaler);

        RecoveryTriggerFactory recoveryTriggerFactory
                = new RecoveryTriggerFactory(jobStore.instanceId);

        triggerRecoverer = new TriggerRecoverer(locksDao, persister,
                lockManager, triggerDao, jobDao, recoveryTriggerFactory,
                misfireHandler);

        triggerRunner = createTriggerRunner(misfireHandler);

        checkinExecutor = createCheckinExecutor(jobStore);
    }

    private CheckinExecutor createCheckinExecutor(MongoDBJobStore jobStore) {
        return new CheckinExecutor(new CheckinTask(schedulerDao),
                jobStore.clusterCheckinIntervalMillis, jobStore.instanceId);
    }

    private CalendarDao createCalendarDao(MongoDBJobStore jobStore) {
        return new CalendarDao(getCollection(jobStore, "calendars"));
    }

    private JobDao createJobDao(MongoDBJobStore jobStore, ClassLoadHelper loadHelper) {
        JobConverter jobConverter = new JobConverter(jobStore.getClassLoaderHelper(loadHelper));
        return new JobDao(getCollection(jobStore, "jobs"), queryHelper, jobConverter);
    }

    private JobCompleteHandler createJobCompleteHandler(SchedulerSignaler signaler) {
        return new JobCompleteHandler(persister, signaler, jobDao, locksDao, triggerDao);
    }

    private LocksDao createLocksDao(MongoDBJobStore jobStore) {
        return new LocksDao(getCollection(jobStore, "locks"), Clock.SYSTEM_CLOCK, jobStore.instanceId);
    }

    private LockManager createLockManager(MongoDBJobStore jobStore) {
        ExpiryCalculator expiryCalculator = new ExpiryCalculator(schedulerDao,
                Clock.SYSTEM_CLOCK, jobStore.jobTimeoutMillis, jobStore.triggerTimeoutMillis);
        return new LockManager(locksDao, expiryCalculator);
    }

    private MisfireHandler createMisfireHandler(MongoDBJobStore jobStore, SchedulerSignaler signaler) {
        return new MisfireHandler(calendarDao, signaler, jobStore.misfireThreshold);
    }

    private MongoConnector createMongoConnector(MongoDBJobStore jobStore) throws SchedulerConfigException {
        return MongoConnector.builder()
                .withClient(jobStore.mongo)
                .withUri(jobStore.orientdbUri)
                .withCredentials(jobStore.username, jobStore.password)
                .withAddresses(jobStore.addresses)
                .withDatabaseName(jobStore.dbName)
                .withAuthDatabaseName(jobStore.authDbName)
                .withMaxConnectionsPerHost(jobStore.mongoOptionMaxConnectionsPerHost)
                .withConnectTimeoutMillis(jobStore.mongoOptionConnectTimeoutMillis)
                .withSocketTimeoutMillis(jobStore.mongoOptionSocketTimeoutMillis)
                .withSocketKeepAlive(jobStore.mongoOptionSocketKeepAlive)
                .withThreadsAllowedToBlockForConnectionMultiplier(
                        jobStore.mongoOptionThreadsAllowedToBlockForConnectionMultiplier)
                .withSSL(jobStore.mongoOptionEnableSSL, jobStore.mongoOptionSslInvalidHostNameAllowed)
                .withWriteTimeout(jobStore.mongoOptionWriteConcernTimeoutMillis)
                .build();
    }

    private PausedJobGroupsDao createPausedJobGroupsDao(MongoDBJobStore jobStore) {
        return new PausedJobGroupsDao(getCollection(jobStore, "paused_job_groups"));
    }

    private PausedTriggerGroupsDao createPausedTriggerGroupsDao(MongoDBJobStore jobStore) {
        return new PausedTriggerGroupsDao(getCollection(jobStore, "paused_trigger_groups"));
    }

    private SchedulerDao createSchedulerDao(MongoDBJobStore jobStore) {
        return new SchedulerDao(getCollection(jobStore, "schedulers"),
                jobStore.schedulerName, jobStore.instanceId, jobStore.clusterCheckinIntervalMillis,
                Clock.SYSTEM_CLOCK);
    }

    private TriggerAndJobPersister createTriggerAndJobPersister() {
        return new TriggerAndJobPersister(triggerDao, jobDao, triggerConverter);
    }

    private TriggerDao createTriggerDao(MongoDBJobStore jobStore) {
        return new TriggerDao(getCollection(jobStore, "triggers"), queryHelper, triggerConverter);
    }

    private TriggerRunner createTriggerRunner(MisfireHandler misfireHandler) {
        return new TriggerRunner(persister, triggerDao, jobDao, locksDao, calendarDao,
                misfireHandler, triggerConverter, lockManager, triggerRecoverer);
    }

    private TriggerStateManager createTriggerStateManager() {
        return new TriggerStateManager(triggerDao, jobDao,
                pausedJobGroupsDao, pausedTriggerGroupsDao, queryHelper);
    }

    private MongoCollection<Document> getCollection(MongoDBJobStore jobStore, String name) {
        return db.getCollection(jobStore.collectionPrefix + name);
    }
}
