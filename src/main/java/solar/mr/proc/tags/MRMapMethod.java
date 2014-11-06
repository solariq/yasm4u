package solar.mr.proc.tags;

import java.lang.annotation.*;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 11:28
 */
@Documented
@Target(ElementType.METHOD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface MRMapMethod {
  String[] input();
  String[] output();
}
