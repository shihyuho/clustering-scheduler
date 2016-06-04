package tw.com.shihyu.clustering.scheduler.quorum;

/**
 * 
 * @author Matt S.Y. Ho
 *
 */
@FunctionalInterface
public interface LeaderElection {

  boolean isLeader();

}
