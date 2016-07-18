package io.smartspaces.scheduling.quartz.orientdb.cluster

import com.mongodb.MongoException
import io.smartspaces.scheduling.quartz.orientdb.dao.SchedulerDao

import io.smartspaces.scheduling.quartz.orientdb.cluster.CheckinTask;
import spock.lang.Specification

class CheckinTaskTest extends Specification {

    def schedulerDao = Mock(SchedulerDao)
    def checkinTask = new CheckinTask(schedulerDao)

    def 'should store scheduler data to checkin'() {
        when:
        checkinTask.run()
        checkinTask.run()

        then:
        2 * schedulerDao.checkIn()
    }

    def 'should stop scheduler when hit by exception'() {
        given:
        def errorHandler = Mock(Runnable)
        1 * schedulerDao.checkIn() >> {
            throw new MongoException('Checkin Error!')
        }

        checkinTask.setErrorHandler(errorHandler)

        when:
        checkinTask.run()

        then:
        1 * errorHandler.run()
    }
}