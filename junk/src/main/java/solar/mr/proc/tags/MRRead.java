package solar.mr.proc.tags;

import java.lang.annotation.*;


import solar.mr.proc.MRState;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 11:29
 */
@Documented
@Target(ElementType.METHOD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface MRRead {
  String output();

  String input();
}
