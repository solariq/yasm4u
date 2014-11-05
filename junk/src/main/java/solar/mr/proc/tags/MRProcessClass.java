package solar.mr.proc.tags;

/**
 * User: solar
 * Date: 13.10.14
 * Time: 9:24
 */
public @interface MRProcessClass {
  String goal() default "var:result";
}
