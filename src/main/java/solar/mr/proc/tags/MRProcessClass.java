package solar.mr.proc.tags;

import java.lang.annotation.*;

/**
 * User: solar
 * Date: 13.10.14
 * Time: 9:24
 */
@Documented
@Target(ElementType.TYPE)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@SuppressWarnings("UnusedDeclaration")
public @interface MRProcessClass {
  String[] goal() default {"var:result"};
}
