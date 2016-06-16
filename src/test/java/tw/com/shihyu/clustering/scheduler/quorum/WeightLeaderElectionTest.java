package tw.com.shihyu.clustering.scheduler.quorum;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class WeightLeaderElectionTest {

  @Test
  public void test() throws Exception {
    try (WeightLeaderElection election = new WeightLeaderElection()) {
      election.setConnectString("localhost:2181");
      election.afterPropertiesSet();

      while (true) {
        TimeUnit.SECONDS.sleep(1);
        System.out.println(election);
      }
    }

  }

}