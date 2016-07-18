package org.shihyu.clustering.scheduler;

import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.shihyu.clustering.scheduler.leader.LeaderElection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Example {

  public static void main(String[] args) throws InterruptedException {
    int contenders = 3;
    Collection<ConfigurableApplicationContext> contexts = new ArrayList<>();
    try {
      for (int contender = 0; contender < contenders; contender++) {
        contexts.add(new ClassPathXmlApplicationContext("scheduling.xml"));
      }
      Collection<LeaderElection> elections =
          contexts.stream().map(ctx -> ctx.getBean(LeaderElection.class)).collect(toList());

      Map<Boolean, List<LeaderElection>> partition =
          elections.stream().collect(partitioningBy(LeaderElection::isLeader));

      List<LeaderElection> leaders = partition.get(true);
      Assert.assertEquals(1, leaders.size());

      LeaderElection leader = leaders.get(0);
      Assert.assertEquals(contenders, leader.getContenders().size());
      leader.getContenders().forEach(c -> {
        Assert.assertEquals(1,
            elections.stream()
                .filter(e -> e.getContenderId().equals(c.getId()) && e.isLeader() == c.isLeader())
                .count());
      });

      List<LeaderElection> followers = partition.get(false);
      if (followers.size() > 0) {
        followers.get(0).relinquishLeadership();
        partition = elections.stream().collect(partitioningBy(LeaderElection::isLeader));
        List<LeaderElection> newLeaders = partition.get(true);
        Assert.assertEquals(1, newLeaders.size());

        LeaderElection newLeader = newLeaders.get(0);
        Assert.assertEquals(leader.getContenderId(), newLeader.getContenderId());
        Assert.assertEquals(contenders, newLeader.getContenders().size());
        newLeader.getContenders().forEach(c -> {
          Assert.assertEquals(1,
              elections.stream()
                  .filter(e -> e.getContenderId().equals(c.getId()) && e.isLeader() == c.isLeader())
                  .count());
        });
      }

      leader.relinquishLeadership();
      Assert.assertFalse(leader.isLeader());

      TimeUnit.SECONDS.sleep(1); // just a short wait for the followers action to NodeDeleted
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
