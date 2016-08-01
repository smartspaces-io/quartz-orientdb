/**
 * 
 */
package io.smartspaces.scheduling.quartz.orientdb;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

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
		properties.put("org.quartz.jobStore.orientdbUri", "PLOCAL:/var/tmp/quartz");
		properties.put("org.quartz.scheduler.skipUpdateCheck", "true");
		properties.put("org.quartz.threadPool.threadCount", "10");

		SchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
		Scheduler scheduler = schedulerFactory.getScheduler();

		scheduler.start();

		scheduleJob(scheduler);
	}

	private static void scheduleJob(Scheduler scheduler) throws SchedulerException {
		JobDetail job = newJob(HelloJob.class).withIdentity("myJob", "group1") // name
																				// "myJob",
																				// group
																				// "group1"
				.build();

		// Trigger the job to run now, and then every 40 seconds
		Trigger trigger = newTrigger().withIdentity("myTrigger", "group1").startNow()
				.withSchedule(simpleSchedule().withIntervalInSeconds(40).repeatForever()).build();

		// Tell quartz to schedule the job using our trigger
		scheduler.scheduleJob(job, trigger);
	}
	
	public static class HelloJob implements Job {
		public void execute(JobExecutionContext context) throws JobExecutionException {
		    // Say Hello to the World and display the date/time
		    System.out.println("Hello World! - " + new Date());
		}
	}
}
