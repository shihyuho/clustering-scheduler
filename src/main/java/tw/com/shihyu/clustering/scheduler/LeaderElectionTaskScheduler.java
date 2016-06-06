package tw.com.shihyu.clustering.scheduler;

import static java.util.Objects.requireNonNull;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

import lombok.NoArgsConstructor;
import lombok.Setter;
import tw.com.shihyu.clustering.scheduler.quorum.LeaderElection;
import tw.com.shihyu.clustering.scheduler.quorum.Relinquishable;

/**
 * A {@link TaskScheduler} decorator to ensure {@link Runnable Runnables} runs only if current node
 * elected as leadership
 * 
 * @author Matt S.Y. Ho
 */
@NoArgsConstructor
public class LeaderElectionTaskScheduler
    implements ScheduleManager, TaskScheduler, InitializingBean {

  private @Setter LeaderElection leaderElection;
  private @Setter TaskScheduler taskScheduler;
  private AtomicBoolean play = new AtomicBoolean(true);

  public LeaderElectionTaskScheduler(LeaderElection leaderElection, TaskScheduler taskScheduler) {
    super();
    this.leaderElection = requireNonNull(leaderElection, "leaderElection");
    this.taskScheduler = requireNonNull(taskScheduler, "taskScheduler");
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    requireNonNull(leaderElection, "'leaderElection' is required");
    requireNonNull(taskScheduler, "'taskScheduler' is required");
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable task, Trigger trigger) {
    if (!(task instanceof ElectedRunnable)) {
      task = new ElectedRunnable(leaderElection, task, play);
    }
    return taskScheduler.schedule(task, trigger);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable task, Date startTime) {
    if (!(task instanceof ElectedRunnable)) {
      task = new ElectedRunnable(leaderElection, task, play);
    }
    return taskScheduler.schedule(task, startTime);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, Date startTime, long period) {
    if (!(task instanceof ElectedRunnable)) {
      task = new ElectedRunnable(leaderElection, task, play);
    }
    return taskScheduler.scheduleAtFixedRate(task, startTime, period);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
    if (!(task instanceof ElectedRunnable)) {
      task = new ElectedRunnable(leaderElection, task, play);
    }
    return taskScheduler.scheduleAtFixedRate(task, period);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, Date startTime, long delay) {
    if (!(task instanceof ElectedRunnable)) {
      task = new ElectedRunnable(leaderElection, task, play);
    }
    return taskScheduler.scheduleWithFixedDelay(task, startTime, delay);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable task, long delay) {
    if (!(task instanceof ElectedRunnable)) {
      task = new ElectedRunnable(leaderElection, task, play);
    }
    return taskScheduler.scheduleWithFixedDelay(task, delay);
  }

  @Override
  public void pause() {
    play.set(false);
  }

  @Override
  public void resume() {
    play.set(true);
  }

  @Override
  public void relinquishLeadership() {
    if (leaderElection instanceof Relinquishable) {
      ((Relinquishable) leaderElection).relinquish();
    }
  }

  @Override
  public boolean isLeader() {
    return leaderElection.isLeader();
  }

}
