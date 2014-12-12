package solar.mr.proc;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:13
 */
public interface CompositeJoba extends Joba {
  Whiteboard wb();
  State execute();

  <T> T result();
}
