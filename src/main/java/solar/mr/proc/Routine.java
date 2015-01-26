package solar.mr.proc;

/**
 * User: solar
 * Date: 21.01.15
 * Time: 13:54
 */
public interface Routine {
  int dim();
  boolean isRelevant(String resource, int index);
  Joba build(String[] input);
}
