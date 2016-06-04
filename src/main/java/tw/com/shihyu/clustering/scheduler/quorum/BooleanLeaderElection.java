package tw.com.shihyu.clustering.scheduler.quorum;

import static java.util.Objects.requireNonNull;

import java.util.function.BooleanSupplier;

import org.springframework.beans.factory.InitializingBean;

import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A {@link LeaderElection} implementation that retrieves from {@link BooleanSupplier}
 * 
 * @author Matt S.Y. Ho
 *
 */
@NoArgsConstructor
public class BooleanLeaderElection implements LeaderElection, InitializingBean {

  private @Setter BooleanSupplier booleanSupplier;

  public BooleanLeaderElection(BooleanSupplier booleanSupplier) {
    super();
    this.booleanSupplier = booleanSupplier;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    requireNonNull(booleanSupplier, "booleanSupplier");
  }

  @Override
  public boolean isLeader() {
    return booleanSupplier.getAsBoolean();
  }

}
