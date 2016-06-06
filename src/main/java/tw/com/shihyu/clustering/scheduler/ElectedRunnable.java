package tw.com.shihyu.clustering.scheduler;

import java.util.concurrent.atomic.AtomicBoolean;

import lombok.AllArgsConstructor;
import tw.com.shihyu.clustering.scheduler.quorum.LeaderElection;

/**
 * Run only if current node elected as leadership
 * 
 * @author Matt S.Y. Ho
 *
 */
@AllArgsConstructor
public class ElectedRunnable implements Runnable {

  private final LeaderElection leaderElection;
  private final Runnable runnable;
  private final AtomicBoolean play;

  public ElectedRunnable(LeaderElection leaderElection, Runnable runnable) {
    this(leaderElection, runnable, new AtomicBoolean(true));
  }

  @Override
  public void run() {
    if (play.get() && leaderElection.isLeader()) {
      runnable.run();
    }
  }

}
