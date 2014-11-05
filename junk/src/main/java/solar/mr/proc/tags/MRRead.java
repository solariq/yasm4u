package solar.mr.proc.tags;

import solar.mr.proc.MRState;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 11:29
 */
public @interface MRRead {
  String output();

  String input();
}
