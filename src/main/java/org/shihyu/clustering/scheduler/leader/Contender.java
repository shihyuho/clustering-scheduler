package org.shihyu.clustering.scheduler.leader;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A contender in leader election
 * 
 * @author Matt S.Y. Ho
 *
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class Contender {

  private String id;
  private boolean leader;

}
