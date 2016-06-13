package tw.com.shihyu.clustering.scheduler;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import tw.com.shihyu.clustering.scheduler.quorum.LeaderElection;

public class Example {

  public static void main(String[] args) {
    String config = "spring-scheduling.xml"; // or quartz.xml
    int contenders = 3;
    Collection<ConfigurableApplicationContext> contexts = new ArrayList<>();
    try {
      for (int contender = 0; contender < contenders; contender++) {
        contexts.add(new ClassPathXmlApplicationContext(config));
      }
      Collection<LeaderElection> elections =
          contexts.stream().map(ctx -> ctx.getBean(LeaderElection.class)).collect(toList());

      List<LeaderElection> leaders =
          elections.stream().filter(LeaderElection::isLeader).collect(toList());
      Assert.assertEquals(1, leaders.size());

      LeaderElection leader = leaders.get(0);
      Assert.assertEquals(contenders, leader.getContenders().size());
      leader.getContenders().forEach(c -> {
        Assert.assertEquals(1,
            elections.stream()
                .filter(e -> e.getContenderId().equals(c.getId()) && e.isLeader() == c.isLeader())
                .count());
      });

      leader.relinquishLeadership();
      Assert.assertFalse(leader.isLeader());

      List<LeaderElection> newLeaders =
          elections.stream().filter(LeaderElection::isLeader).collect(toList());
      Assert.assertEquals(1, newLeaders.size());
      LeaderElection newLeader = newLeaders.get(0);
      Assert.assertNotEquals(leader.getContenderId(), newLeader.getContenderId());
    } finally {
      contexts.forEach(ConfigurableApplicationContext::close);
    }
  }

}
