package tw.com.shihyu.clustering.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Example {

  public static void main(String[] args) throws InterruptedException {
    String config = "spring-scheduling.xml"; // or quartz.xml
    int contenders = 3;
    Executor executor = Executors.newFixedThreadPool(contenders);

    Collection<ClassPathXmlApplicationContext> contexts = new ArrayList<>();
    try {
      for (int i = 0; i < contenders; i++) {
        CompletableFuture.runAsync(() -> contexts.add(new ClassPathXmlApplicationContext(config)),
            executor);
      }
      TimeUnit.SECONDS.sleep(30);
    } finally {
      contexts.forEach(ClassPathXmlApplicationContext::close);
    }
  }

}
