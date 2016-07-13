package org.shihyu.clustering.scheduler.quorum;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
    implements LeaderElection, AutoCloseable, DisposableBean, InitializingBean {

  protected @Setter Charset charset = StandardCharsets.UTF_8;
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
