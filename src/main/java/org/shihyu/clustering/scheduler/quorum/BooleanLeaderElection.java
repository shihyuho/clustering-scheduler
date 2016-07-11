package org.shihyu.clustering.scheduler.quorum;

import java.io.Closeable;
import java.util.function.BooleanSupplier;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import lombok.Setter;

/**
 * An adapter passing {@link BooleanSupplier} to {@link LeaderElection}
 * 
 * @author Matt S.Y. Ho
 *
 */
public abstract class BooleanLeaderElection
    implements LeaderElection, Closeable, DisposableBean, InitializingBean {

  private @Setter BooleanSupplier booleanSupplier;

  @Override
  public boolean isLeader() {
    return booleanSupplier.getAsBoolean();
  }

  @Override
  public void destroy() throws Exception {
    close();
  }
}
