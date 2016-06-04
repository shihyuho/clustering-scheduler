package tw.com.shihyu.clustering.scheduler.quorum;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Properties;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import lombok.Getter;
import lombok.Setter;

/**
 * A {@link LeaderElection} implementation that retrieves values from {@link Properties}
 * 
 * @author Matt S.Y. Ho
 *
 */
public class PropertyLeaderElection implements LeaderElection, InitializingBean {

  private @Setter ResourceLoader resourceLoader = new DefaultResourceLoader();
  private @Setter String location;
  private @Setter String lookup;
  private @Setter String expected;
  private String actual;
  private @Getter boolean leader;

  @Override
  public void afterPropertiesSet() throws Exception {
    requireNonNull(location, "'location' is required");
    requireNonNull(lookup, "'lookup' is required");
    requireNonNull(expected, "'expectd' is required");
    requireNonNull(resourceLoader, "'resourceLoader' is required");

    Resource resource = resourceLoader.getResource(location);
    if (!resource.exists()) {
      throw new IllegalArgumentException(
          "Can not load property from [" + location + "]: resource not found");
    }
    Properties properties = new Properties();
    properties.load(resource.getInputStream());
    actual = properties.getProperty(lookup);
    if (Objects.equals(expected, actual)) {
      leader = true;
    }
  }

  @Override
  public String toString() {
    return "PropertyLeaderElection{" + "location='" + location + '\'' + ", lookup='" + lookup
        + ", expected='" + expected + "', actual='" + actual + "', isLeader=" + leader + '}';
  }

}
