package tw.com.shihyu.clustering.scheduler.quartz;

import static java.util.Objects.requireNonNull;

import org.quartz.Job;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.InitializingBean;

import lombok.NoArgsConstructor;
import lombok.Setter;
import tw.com.shihyu.clustering.scheduler.quorum.LeaderElection;

/**
 * A {@link SpringBeanJobFactory} decorator to ensure {@link Job Jobs} runs only if current node
 * elected as leadership
 * 
 * @author Matt S.Y. Ho
 *
 */
@NoArgsConstructor
public class LeaderElectionJobFactory implements JobFactory, InitializingBean {

  private @Setter LeaderElection leaderElection;
  private @Setter JobFactory jobFactory;

  public LeaderElectionJobFactory(LeaderElection leaderElection, JobFactory jobFactory) {
    super();
    this.leaderElection = requireNonNull(leaderElection, "leaderElection");
    this.jobFactory = requireNonNull(jobFactory, "jobFactory");
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    requireNonNull(leaderElection, "'leaderElection' is required");
    requireNonNull(jobFactory, "'jobFactory' is required");
  }

  @Override
  public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
    Job job = jobFactory.newJob(bundle, scheduler);
    if (job instanceof ElectedJob) {
      return job;
    } else {
      return new ElectedJob(leaderElection, job);
    }
  }

}
