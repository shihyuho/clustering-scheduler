package tw.com.shihyu.clustering.scheduler.quorum;

import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.UUID;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
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
  private @Setter @Getter String contenderId;
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
      contenderId = InetAddress.getLocalHost() + "/" + UUID.randomUUID();
      log.debug("Generating random UUID [{}] for 'contenderId'", contenderId);
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
      client.close();
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

  @Override
  public Collection<Contender> getContenders() {
    try {
      return leaderLatch.getParticipants().stream().map(p -> new Contender(p.getId(), p.isLeader()))
          .collect(toList());
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  @Override
  public void relinquishLeadership() {
    if (isLeader()) {
      try {
        close();
        start();
      } catch (Exception e) {
        throw new Error(e);
      }
    }
  }

}
