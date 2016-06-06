package tw.com.shihyu.clustering.scheduler;

import java.util.Map;

/**
 * Control scheduled jobs
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
   * Continue scheduled jobs
   */
  void resume();

  /**
   * Relinquish leadership
   */
  void relinquishLeadership();

  /**
   * 
   * @return <code>true</code> if current node is leader, otherwise <code>false</code>
   */
  boolean isLeader();

  /**
   * Returns the set of current participants in the leader election
   * 
   * @return Map of key: contenderId, value: isLeader
   */
  Map<String, Boolean> getParticipants();

}
