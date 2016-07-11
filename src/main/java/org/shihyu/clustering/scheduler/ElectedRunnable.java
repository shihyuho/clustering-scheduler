package org.shihyu.clustering.scheduler;

import java.util.concurrent.atomic.AtomicBoolean;

import org.shihyu.clustering.scheduler.quorum.LeaderElection;

import lombok.AllArgsConstructor;

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
