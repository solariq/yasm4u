package solar.mr.proc;


import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public interface Whiteboard extends State {
  State snapshot();

  <T> void set(String var, T data);
  void remove(String var);

  void wipe();

  // to ext
  MRErrorsHandler errorsHandler();
  MREnv env();
}
