package solar.mr;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 11:09
 */
public interface MROutput {
  void add(final String key, final String subkey, final CharSequence value);
  void add(int tableNo, String key, String subkey, CharSequence value);
  void error(String type, String cause, CharSequence data);
  void error(Throwable th, CharSequence record);
}
