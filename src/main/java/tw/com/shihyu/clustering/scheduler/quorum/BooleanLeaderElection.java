package tw.com.shihyu.clustering.scheduler.quorum;

import java.util.function.BooleanSupplier;

import org.springframework.beans.factory.InitializingBean;

import lombok.Setter;

/**
 * An adapter passing {@link BooleanSupplier} to {@link LeaderElection}
 * 
 * @author Matt S.Y. Ho
 *
 */
public abstract class BooleanLeaderElection implements LeaderElection, InitializingBean {

  private @Setter BooleanSupplier booleanSupplier;

  @Override
  public boolean isLeader() {
    return booleanSupplier.getAsBoolean();
  }

}
