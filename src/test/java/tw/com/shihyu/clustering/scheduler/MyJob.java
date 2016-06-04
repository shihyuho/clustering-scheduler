package tw.com.shihyu.clustering.scheduler;

import java.time.LocalDateTime;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MyJob implements Job {

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    log.info("{}", LocalDateTime.now());
  }

}
