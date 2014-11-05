package solar.mr.proc.tags;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 11:29
 */
public @interface MRReduceMethod {
  String[] input();
  String[] output();
}
