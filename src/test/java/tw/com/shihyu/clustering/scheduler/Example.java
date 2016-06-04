package tw.com.shihyu.clustering.scheduler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Example {

  public static void main(String[] args) throws InterruptedException {
    String config = "spring-scheduling.xml"; // or quartz.xml
    int contenders = 3;
    Executor executor = Executors.newFixedThreadPool(contenders);
    for (int i = 0; i < contenders; i++) {
      CompletableFuture.runAsync(() -> new ClassPathXmlApplicationContext(config), executor);
    }
    Thread.sleep(Long.MAX_VALUE);
  }

}
