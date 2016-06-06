package tw.com.shihyu.clustering.scheduler.quorum;

import java.util.Map;

/**
 * 
 * @author Matt S.Y. Ho
 *
 */
public interface LeaderElection {

  boolean isLeader();

  void relinquishLeadership();

  /**
   * Returns the set of current participants in the leader election
   * 
   * @return Map of key: contenderId, value: isLeader
   */
  Map<String, Boolean> getParticipants();

}
