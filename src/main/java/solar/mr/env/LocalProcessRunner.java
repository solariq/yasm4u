package solar.mr.env;

import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.system.RuntimeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by minamoto on 01/02/15.
 */
public class LocalProcessRunner implements ProcessRunner {
  private final String binaryPath;
  public LocalProcessRunner(final String binaryPath) {
    this.binaryPath = binaryPath;
  }

  @Override
  public Process start(final List<String> options, final InputStream input) {
    final ArrayList<String> command = new ArrayList<>();
    command.add(binaryPath);
    for (final String o:options) {
      command.add(o.replace("$", "."));
    }

    final ArrayList<String> envp = new ArrayList<>();
    for(Map.Entry<String,String> e:System.getenv().entrySet()){
      envp.add(e.getKey() + "=" + e.getValue());
    }
    try {

      final Process process = Runtime.getRuntime().exec(command.toArray(new String[command.size()]), envp.toArray(new String[envp.size()]));
      if (input != null) {
        StreamTools.transferData(input, process.getOutputStream());
      }
      return process;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
