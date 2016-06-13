package tw.com.shihyu.clustering.scheduler.quartz;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.quartz.Job;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

import lombok.NoArgsConstructor;
import lombok.Setter;
import tw.com.shihyu.clustering.scheduler.ScheduleManager;
import tw.com.shihyu.clustering.scheduler.quorum.Contender;
import tw.com.shihyu.clustering.scheduler.quorum.LeaderElection;

/**
 * A {@link SpringBeanJobFactory} decorator to ensure {@link Job Jobs} runs only if current node
 * elected as leadership
 * 
 * @author Matt S.Y. Ho
 *
 */
@NoArgsConstructor
public class LeaderElectionJobFactory implements ScheduleManager, JobFactory, InitializingBean {

  private @Setter LeaderElection leaderElection;
  private @Setter JobFactory jobFactory;
  private AtomicBoolean play = new AtomicBoolean(true);

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
      return new ElectedJob(leaderElection, job, play);
    }
  }

  @Override
  public void pause() {
    play.set(false);
  }

  @Override
  public ScheduledFuture<?> pause(long timeout, TimeUnit unit) {
    pause();
    return Executors.newScheduledThreadPool(1).schedule(() -> resume(), timeout, unit);
  }

  @Override
  public void resume() {
    play.set(true);
  }

  @Override
  public void relinquishLeadership() {
    leaderElection.relinquishLeadership();
  }

  @Override
  public Contender getCurrent() {
    return new Contender(leaderElection.getContenderId(), leaderElection.isLeader());
  }

  @Override
  public Collection<Contender> getContenders() {
    return leaderElection.getContenders();
  }

}
