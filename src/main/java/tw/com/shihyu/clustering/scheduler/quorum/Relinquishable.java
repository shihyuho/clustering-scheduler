package tw.com.shihyu.clustering.scheduler.quorum;

/**
 * Indicate that the leadership can be relinquished
 * 
 * @author Matt S.Y. Ho
 *
 */
public interface Relinquishable {

  void relinquish();

}
