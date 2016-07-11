package org.shihyu.clustering.scheduler.quartz;

import java.util.concurrent.atomic.AtomicBoolean;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.shihyu.clustering.scheduler.quorum.LeaderElection;

import lombok.AllArgsConstructor;

/**
 * Execute only if current node elected as leadership
 * 
 * @author Matt S.Y. Ho
 *
 */
@AllArgsConstructor
public class ElectedJob implements Job {

  private final LeaderElection leaderElection;
  private final Job job;
  private final AtomicBoolean play;

  public ElectedJob(LeaderElection leaderElection, Job job) {
    this(leaderElection, job, new AtomicBoolean(true));
  }

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    if (play.get() && leaderElection.isLeader()) {
      job.execute(context);
    }
  }

}
