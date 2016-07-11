package org.shihyu.clustering.scheduler;

import java.util.Collection;

import org.shihyu.clustering.scheduler.quorum.Contender;
import org.shihyu.clustering.scheduler.quorum.LeaderElection;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Automatically gather all {@link ScheduleManager} registered in Spring
 * 
 * @author Matt S.Y. Ho
 *
 */
public class ScheduleManagers
    implements ScheduleManager, InitializingBean, ApplicationContextAware {

  private ApplicationContext applicationContext;
  private Collection<ScheduleManager> managers;
  private LeaderElection leaderElection;

  @Override
  public void afterPropertiesSet() throws Exception {
    managers = applicationContext.getBeansOfType(ScheduleManager.class).values();
    try {
      leaderElection = applicationContext.getBean(LeaderElection.class);
    } catch (final NoSuchBeanDefinitionException e) {
    }
  }

  @Override
  public void pause() {
    managers.forEach(ScheduleManager::pause);
  }

  @Override
  public void resume() {
    managers.forEach(ScheduleManager::resume);
  }

  @Override
  public void relinquishLeadership() {
    managers.forEach(ScheduleManager::relinquishLeadership);
  }

  @Override
  public Contender getCurrent() {
    if (leaderElection == null) {
      throw new NoSuchBeanDefinitionException(LeaderElection.class);
    }
    return new Contender(leaderElection.getContenderId(), leaderElection.isLeader());
  }

  @Override
  public Collection<Contender> getContenders() {
    if (leaderElection == null) {
      throw new NoSuchBeanDefinitionException(LeaderElection.class);
    }
    return leaderElection.getContenders();
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }
}
