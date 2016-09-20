package org.shihyu.clustering.scheduler.leader;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link LeaderElection} implementation that tries to take leadership from a ZooKeeper server
 * 
 * @author Matt S.Y. Ho
 * @see Recipe:
 *      <a href="http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection">http://
 *      zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection</a>
 */
@Slf4j
public class ZooKeeperLeaderElection
    implements LeaderElection, Watcher, InitializingBean, DisposableBean, AutoCloseable {

  private ZooKeeper zk;
  private @Setter Charset charset = StandardCharsets.UTF_8;
  private @Setter String connectString;
  private @Setter int sessionTimeoutMs = 15000;
  private @Setter String rootPath = "/election";
  private @Getter @Setter String contenderId;
  private String contenderSequence;
  private final AtomicBoolean leader = new AtomicBoolean();
  private @Setter CreateMode contenderMode = CreateMode.EPHEMERAL_SEQUENTIAL;
  private @Setter String contenderPath = "/m-"; // 'm' is just is the middle letter of the alphabet

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
      log.debug("Generating random ID [{}] for 'contenderId'", contenderId);
    }
    requireNonNull(contenderMode, "'contenderMode' is required");
    if (contenderPath == null || contenderPath.isEmpty()) {
      throw new IllegalArgumentException("'contenderPath' is required");
    } else if (!contenderPath.startsWith("/")) {
      contenderPath = "/" + contenderPath;
    }

    log.info("Registering ZooKeeper leader election Contender '{}' for [{}{}]", contenderId,
        connectString, rootPath);

    start();
  }

  @Override
  public void close() throws Exception {
    zk.close();
  }

  private void start() throws IOException {
    zk = new ZooKeeper(connectString, sessionTimeoutMs, this);
    createParent();
    createContender();
  }

  private void createContender() {
    zk.create(rootPath + contenderPath, contenderId.getBytes(charset), Ids.OPEN_ACL_UNSAFE,
        contenderMode, acquireContenderSequence, null);
  }

  private StringCallback acquireContenderSequence = new StringCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          leader.set(false);
          createContender();
          break;
        case OK:
          contenderSequence = name.replaceFirst(rootPath + "/", "");
          log.debug("[{}] Contender [{}] created", contenderId, contenderSequence);
          checkLeader();
          break;
        case NODEEXISTS:
          log.debug("[{}] Contender [{}] already registered", contenderId, name);
          checkLeader();
          break;
        default:
          log.error("[{}] Something went wrong when acquiring Contender sequence [{}]", contenderId,
              rootPath, KeeperException.create(Code.get(rc), path));
          leader.set(false);
      }
    }
  };

  private void checkLeader() {
    zk.getChildren(rootPath, false, attemptToTakeLeadership, null);
  }

  private Watcher znodeDeleted = new Watcher() {
    @Override
    public void process(WatchedEvent e) {
      switch (e.getType()) {
        case NodeDeleted:
          assert rootPath.equals(e.getPath());
          checkLeader();
          break;
        default:
          break;
      }
    }
  };

  private ChildrenCallback attemptToTakeLeadership = new ChildrenCallback() {
    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
      switch (Code.get(rc)) {
        case CONNECTIONLOSS:
          leader.set(false);
          checkLeader();
          break;
        case OK:
          sortAsc(children);
          int index = children.indexOf(contenderSequence);
          if (index == -1) { // Perhaps someone delete znode from somewhere else
            createContender();
          } else if (index == 0) {
            if (leader.compareAndSet(false, true)) {
              log.info("[{}] Acquired the leadership", contenderId);
            }
          } else {
            if (leader.compareAndSet(true, false)) {
              log.info("[{}] Released the leadership", contenderId);
            }
            try {
              zk.getChildren(rootPath + "/" + children.get(index - 1), znodeDeleted);
            } catch (KeeperException | InterruptedException e) {
              throw new Error(e);
            }
          }
          break;
        default:
          log.error("[{}] Something went wrong when attempting to take leadership", contenderId,
              rootPath, KeeperException.create(Code.get(rc), path));
          leader.set(false);
      }
    }
  };

  private void createParent() {
    String path = rootPath;
    try {
      path = zk.create(rootPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (ConnectionLossException e) {
      createParent();
    } catch (NodeExistsException e) {
      log.debug("[{}] Parent already registered: [{}]", contenderId, path);
    } catch (KeeperException | InterruptedException e) {
      log.error("[{}] Something went wrong when creating parent [{}]", contenderId, rootPath, e);
    }
  }

  @Override
  public void relinquishLeadership() {
    try {
      zk.delete(rootPath + "/" + contenderSequence, -1);
      leader.set(false);
      createContender();
    } catch (InterruptedException | KeeperException e) {
      throw new Error(e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    log.debug("[{}] {}", contenderId, event);
    switch (event.getState()) {
      case AuthFailed:
      case Disconnected:
        checkLeader();
      case Expired:
        try {
          close();
        } catch (Exception e) {
        }
        try {
          start();
        } catch (Exception e) {
          log.error("[{}] Something went wrong when running for leader", contenderId, e);
        }
      default:
        break;
    }
  }

  private void sortAsc(List<String> children) {
    Collections.sort(children);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{" + "contenderId='" + contenderId + '\''
        + ", contenderSequence='" + contenderSequence + "', isLeader=" + isLeader() + '}';
  }

  @Override
  public boolean isLeader() {
    return leader.get();
  }

  @Override
  public Collection<Contender> getContenders() {
    try {
      return zk.getChildren(rootPath, false).stream().map(this::contender).collect(toList());
    } catch (KeeperException | InterruptedException e) {
      throw new Error(e);
    }
  }

  private Contender contender(String child) {
    try {
      byte[] contenderId = zk.getData(rootPath + "/" + child, false, null);
      return new Contender(new String(contenderId, charset), child.equals(contenderSequence));
    } catch (KeeperException | InterruptedException e) {
      throw new Error(e);
    }
  }

  @Override
  public void destroy() throws Exception {
    clone();
  }

}
