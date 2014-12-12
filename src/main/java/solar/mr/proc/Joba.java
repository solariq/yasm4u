package solar.mr.proc;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:20
 */
public interface Joba {
  String name();
  boolean run(Whiteboard wb);

  String[] consumes();
  String[] produces();
}
