package tw.com.shihyu.clustering.scheduler.quartz;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import lombok.AllArgsConstructor;
import tw.com.shihyu.clustering.scheduler.quorum.LeaderElection;

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

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    if (leaderElection.isLeader()) {
      job.execute(context);
    }
  }

}
