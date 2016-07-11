package org.shihyu.clustering.scheduler;

import java.time.LocalDateTime;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class MyScheduled {

  @Scheduled(fixedRate = 5000)
  public void print() {
    log.info("{}", LocalDateTime.now());
  }

}
