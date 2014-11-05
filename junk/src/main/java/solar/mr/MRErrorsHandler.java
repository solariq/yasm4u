package solar.mr;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 11:09
 */
public interface MRErrorsHandler {
  void error(String type, String cause, String table, CharSequence record);
  void error(Throwable th, String table, CharSequence record);
}
