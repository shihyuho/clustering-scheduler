package tw.com.shihyu.clustering.scheduler.quorum;

import static java.util.stream.Collectors.toMap;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link LeaderElection} implementation that uses {@link LeaderSelector}
 * 
 * @author Matt S.Y. Ho
 */
@Slf4j
public class CuratorLeaderSelector implements LeaderElection, LeaderSelectorListener,
    PathChildrenCacheListener, InitializingBean, DisposableBean, Closeable {

  private @Setter String connectString;
  private @Setter int baseSleepTimeMs = 1000;
  private @Setter int maxRetries = Integer.MAX_VALUE;
  private @Setter String rootPath = "/election";
  private @Getter @Setter String contenderId;
  private final AtomicBoolean leader = new AtomicBoolean();
  private LeaderSelector leaderSelector;
  private CuratorFramework client;
  private PathChildrenCache cache;

  @Override
  public boolean isLeader() {
    return leader.get();
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    if (connectString == null || connectString.isEmpty()) {
      throw new IllegalArgumentException("'connectString' is required");
    }
    if (rootPath == null || rootPath.isEmpty()) {
      throw new IllegalArgumentException("'rootPath' is required");
    } else if (!rootPath.startsWith("/")) {
      rootPath = "/" + rootPath;
    }
    if (contenderId == null || contenderId.isEmpty()) {
      contenderId = UUID.randomUUID().toString();
      log.debug("Generating random UUID [{}] for 'contenderId'", contenderId);
    }

    start();
  }

  @Override
  public void close() throws IOException {
    CloseableUtils.closeQuietly(cache);
    CloseableUtils.closeQuietly(leaderSelector);
    CloseableUtils.closeQuietly(client);
  }

  @Override
  public void destroy() throws Exception {
    close();
  }

  private void start() throws Exception {
    client = CuratorFrameworkFactory.newClient(connectString,
        new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries));
    client.start();
    try {
      client.getZookeeperClient().blockUntilConnectedOrTimedOut();
    } catch (InterruptedException e) {
      start();
    }

    leaderSelector = new LeaderSelector(client, rootPath, this);
    leaderSelector.autoRequeue();
    leaderSelector.setId(contenderId);
    leaderSelector.start();

    cache = new PathChildrenCache(client, rootPath, true);
    cache.start();
    cache.getListenable().addListener(this);
  }

  @Override
  public void relinquishLeadership() {
    leader.set(false);
  }

  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {
    leader.set(true);
    while (isLeader()) {
    }
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    if ((newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST)) {
      relinquishLeadership();
      throw new CancelLeadershipException();
    }
  }

  public Participant getCurrentLeader() throws Exception {
    return leaderSelector.getLeader();
  }

  @Override
  public String toString() {
    return "CuratorLeaderSelector" + "{" + "contenderId='" + contenderId + '\'' + ", isLeader="
        + isLeader() + '}';
  }

  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
    switch (event.getType()) {
      case CHILD_REMOVED:
        String removedId = new String(event.getData().getData(), Charset.forName("UTF-8"));
        if (removedId.equals(contenderId)) {
          if (isLeader()) {
            relinquishLeadership();
          } else {
            close();
            start();
          }
        }
      default:
        break;
    }
  }

  @Override
  public Map<String, Boolean> getParticipants() {
    try {
      return leaderSelector.getParticipants().stream()
          .collect(toMap(Participant::getId, Participant::isLeader));
    } catch (Exception e) {
      throw new Error(e);
    }
  }
}
