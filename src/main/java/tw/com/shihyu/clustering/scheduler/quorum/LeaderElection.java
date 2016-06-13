package tw.com.shihyu.clustering.scheduler.quorum;

import java.util.Collection;

/**
 * 
 * @author Matt S.Y. Ho
 *
 */
public interface LeaderElection {

  String getContenderId();

  boolean isLeader();

  void relinquishLeadership();

  /**
   * Returns the set of current participants in the leader election
   * 
   * @return all contenders in leader election
   */
  Collection<Contender> getContenders();

}
