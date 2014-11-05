package solar.mr.proc;

import solar.mr.MREnv;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:20
 */
public interface MRJoba {
  boolean run(MRWhiteboard wb);

  String[] consumes();
  String[] produces();
}
