# clustering-scheduler

Integrate Spring Scheduling and Quartz into ZooKeeper

## LeaderElection

Choose a implementation of `LeaderElection` and register it into Spring.

- CuratorLeaderLatch
	- An implementation by using [Apache Curator](http://curator.apache.org) `LeaderLatch`
- CuratorLeaderSelector
	- An implementation by using [Apache Curator](http://curator.apache.org) `LeaderSelector`
- ZooKeeperLeaderElection
	- An simple implementation of [ZooKeeper Recipe](http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection)

```xml
<bean id="leaderElection" class="tw.com.shihyu.clustering.scheduler.quorum.CuratorLeaderSelector">
	<property name="connectString" value="localhost:2181"/>
</bean>
```

## Adapt into scheduler

### Spring Scheduling

`LeaderElectionTaskScheduler` is a `TaskScheduler` decorator to ensure Runnables runs only if current node elected as leadership.

```xml
<bean id="myScheduler" class="tw.com.softleader.domain.scheduling.LeaderElectionTaskScheduler">
	<property name="leaderElection" ref="leaderElection"/>
	<property name="taskScheduler">
		<bean class="org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler">
			[...]
		</bean>
	</property>
</bean>
	
<task:annotation-driven scheduler="myScheduler"/>
```

and your scheduled jobs looks like:

```java
@Slf4j
@Component
public class MyScheduled {

  @Scheduled(fixedRate = 5000)
  public void print() {
    log.info("{}", LocalDateTime.now());
  }
}
```

### Quartz

`LeaderElectionSpringBeanJobFactory` ensures Jobs runs only if current node elected as leadership as well.

```xml
<bean id="myJobFactory" class="tw.com.shihyu.clustering.scheduler.quartz.LeaderElectionJobFactory">
	<property name="jobFactory">
		<bean class="org.springframework.scheduling.quartz.SpringBeanJobFactory"></bean>
	</property>
	<property name="leaderElection" ref="leaderElection"/>
</bean>

<bean id="quartzScheduler" class="org.springframework.scheduling.quartz.SchedulerFactoryBean">
	<property name="jobFactory" ref="myJobFactory"/>
	<property name="schedulerName" value="MyScheduler" />
	<property name="triggers">
		<list>
		    <ref bean="myTrigger" />
		</list>
	</property>
</bean>

<bean id="myTrigger" class="org.springframework.scheduling.quartz.CronTriggerFactoryBean">
    <property name="jobDetail" ref="myJobDetail"/>
    <property name="cronExpression" value="0/5 * * * * ?" />
</bean> 
<bean id="myJobDetail" class="org.springframework.scheduling.quartz.JobDetailFactoryBean">
    <property name="jobClass" value="tw.com.shihyu.clustering.scheduler.MyJob"/>
</bean>
```

and this is your job:

```java
@Slf4j
public class MyJob implements Job {

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    log.info("{}", LocalDateTime.now());
  }
}
```

## Exmaple

`tw.com.shihyu.clustering.scheduler.quorum.LeaderElectionTest`
