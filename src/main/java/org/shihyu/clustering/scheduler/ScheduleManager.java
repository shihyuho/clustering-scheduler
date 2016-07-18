package org.shihyu.clustering.scheduler;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.shihyu.clustering.scheduler.leader.Contender;

/**
 * A manager to controls scheduled jobs
 * 
 * @author Matt S.Y. Ho
 *
 */
public interface ScheduleManager {

  /**
   * Pause scheduled jobs
   */
  void pause();

  /**
   * Pause scheduled jobs until the given timeout.
   * 
   * @param timeout
   * @param unit
   * @return A {@link ScheduledFuture} to call {@link #resume()}
   */
  default ScheduledFuture<?> pause(long timeout, TimeUnit unit) {
    pause();
    return Executors.newScheduledThreadPool(1).schedule(() -> resume(), timeout, unit);
  }

  /**
   * Continue scheduled jobs
   */
  void resume();

  /**
   * Relinquish the leadership
   */
  void relinquishLeadership();

  /**
   * 
   * @return current node information
   */
  Contender getCurrent();

  /**
   * Returns the set of current participants in the leader election
   * 
   * @return all contenders in leader election
   */
  Collection<Contender> getContenders();

}
