package tw.com.shihyu.clustering.scheduler.quorum;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.lang3.RandomUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.springframework.beans.factory.DisposableBean;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link LeaderElection} implementation that uses {@link LeaderLatch}
 * 
 * @author Matt S.Y. Ho
 */
@Slf4j
public class CuratorLeaderLatch extends BooleanLeaderElection
    implements PathChildrenCacheListener, DisposableBean, Closeable {

  private @Setter String connectString;
  private @Setter int baseSleepTimeMs = 1000;
  private @Setter int maxRetries = Integer.MAX_VALUE;
  private @Setter String rootPath = "/election";
  private @Getter @Setter String contenderId;
  private LeaderLatch leaderLatch;
  private CuratorFramework client;
  private PathChildrenCache cache;

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
      contenderId = Long.toString(RandomUtils.nextLong(0, Long.MAX_VALUE));
      log.debug("Generating random number [{}] for 'contenderId'", contenderId);
    }

    start();
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

    leaderLatch = new LeaderLatch(client, rootPath, contenderId);
    leaderLatch.start();
    setBooleanSupplier(leaderLatch::hasLeadership);

    cache = new PathChildrenCache(client, rootPath, true);
    cache.start();
    cache.getListenable().addListener(this);
  }

  @Override
  public void destroy() throws Exception {
    close();
  }

  @Override
  public void close() throws IOException {
    CloseableUtils.closeQuietly(cache);
    CloseableUtils.closeQuietly(leaderLatch);
    CloseableUtils.closeQuietly(client);
  }

  public Participant getCurrentLeader() throws Exception {
    return leaderLatch.getLeader();
  }

  @Override
  public String toString() {
    return "CuratorLeaderLatch{" + "contenderId='" + contenderId + '\'' + ", isLeader=" + isLeader()
        + '}';
  }

  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
    switch (event.getType()) {
      case CHILD_REMOVED:
        String removedId = new String(event.getData().getData(), Charset.forName("UTF-8"));
        if (removedId.equals(contenderId)) {
          close();
          start();
        }
      default:
        break;
    }
  }

}
