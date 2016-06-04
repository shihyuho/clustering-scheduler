package tw.com.shihyu.clustering.scheduler.quorum;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)

// test spring @Scheduled
// @ContextConfiguration(locations = {"classpath*:spring-scheduling.xml"}, inheritInitializers =
// false)

// test quartz job
@ContextConfiguration(locations = {"classpath*:quartz.xml"}, inheritInitializers = false)
public class LeaderElectionTest {

  @Test
  public void test() throws Exception {
    Thread.sleep(Long.MAX_VALUE);
  }
}
