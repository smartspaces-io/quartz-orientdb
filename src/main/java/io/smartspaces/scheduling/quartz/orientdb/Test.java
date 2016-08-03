/**
 * 
 */
package io.smartspaces.scheduling.quartz.orientdb;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;
import static org.quartz.CronScheduleBuilder.*;
import static org.quartz.DateBuilder.*;

import java.util.Date;
import java.util.Properties;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

/**
 * @author keith
 *
 */
public class Test {
  public static void main(String[] args) throws Exception {
    Properties properties = new Properties();
    properties.put("org.quartz.jobStore.class", OrientDbJobStore.class.getName());
    properties.put("org.quartz.jobStore.orientDbUri", "PLOCAL:/var/tmp/quartz");
    properties.put("org.quartz.scheduler.skipUpdateCheck", "true");
    properties.put("org.quartz.threadPool.threadCount", "10");

    SchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
    Scheduler scheduler = schedulerFactory.getScheduler();

    scheduler.start();

   // scheduleSimpleJob(scheduler, "simple1");
    //scheduleCronJob(scheduler, "0 0/1 * * * ?", "cron");
  }

  private static void scheduleSimpleJob(Scheduler scheduler, String data) throws SchedulerException {
    JobDetail job =
        newJob(HelloJob.class).withIdentity("myJob"+data, "group1").usingJobData("foo", data).build();

    // Trigger the job to run now, and then every 20 seconds
    Trigger trigger = newTrigger().withIdentity("myTrigger"+data, "group1").startNow()
        .withSchedule(simpleSchedule().withIntervalInSeconds(40).repeatForever()).build();

    // Tell quartz to schedule the job using our trigger
    scheduler.scheduleJob(job, trigger);
  }

  private static void scheduleCronJob(Scheduler scheduler, String cron, String data) throws SchedulerException {
    JobDetail job =
        newJob(HelloJob.class).withIdentity("myJob"+data, "group1").usingJobData("foo", data).build();

    // Trigger the job to run now, and then every 20 seconds
    Trigger trigger = newTrigger().withIdentity("myTrigger"+data, "group1")
        .withSchedule(cronSchedule(cron)).build();

    // Tell quartz to schedule the job using our trigger
    scheduler.scheduleJob(job, trigger);
  }

  public static class HelloJob implements Job {
    public void execute(JobExecutionContext context) throws JobExecutionException {
      // Say Hello to the World and display the date/time
      Object data = context.getMergedJobDataMap().get("foo");
      System.out.println("Hello World! - " + data + " - " + new Date());
    }
  }
}
