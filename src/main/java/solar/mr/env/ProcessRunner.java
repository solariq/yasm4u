package solar.mr.env;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.List;

/**
 * User: solar
 * Date: 31.10.14
 * Time: 19:02
 */
public interface ProcessRunner {
  Process start(List<String> options, final InputStream input);
}
