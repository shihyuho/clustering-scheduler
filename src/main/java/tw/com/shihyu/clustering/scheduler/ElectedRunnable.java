package tw.com.shihyu.clustering.scheduler;

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

  @Override
  public void run() {
    if (leaderElection.isLeader()) {
      runnable.run();
    }
  }
}
