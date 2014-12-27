package solar.mr;

import java.util.List;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 11:09
 */
public interface MROutput extends MRErrorsHandler {
  void add(final String key, final String subkey, final CharSequence value);
  void add(int tableNo, String key, String subkey, CharSequence value);

  String[] names();
}
