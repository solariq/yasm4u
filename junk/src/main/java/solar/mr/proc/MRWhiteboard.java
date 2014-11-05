package solar.mr.proc;


import com.spbsu.commons.func.types.SerializationRepository;
import solar.mr.MREnv;
import solar.mr.MRErrorsHandler;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 10:23
 */
public interface MRWhiteboard {
  /** This method assigns real resource to the resource name and resolves variables inside uri */
  <T> T refresh(final String uri);
  <T> T resolve(final String uri);
  <T> void set(String var, T data);
  void remove(String var);
  boolean check(String... productName);

  void wipe();
  MREnv env();

  // to ext
  MRState snapshot();
  SerializationRepository marshaling();
  MRErrorsHandler errorsHandler();
  void setErrorsHandler(MRErrorsHandler errorsHandler);
}
