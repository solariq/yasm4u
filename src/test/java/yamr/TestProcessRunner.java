package yamr;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.*;
import solar.mr.env.ProcessRunner;

/**
* User: solar
* Date: 12.10.14
* Time: 12:16
*/
public class TestProcessRunner implements ProcessRunner {
  public Map<String, String> tableContents = new HashMap<>();

  public SeqBuilder<CharSeq> commands = new ArraySeqBuilder<>(CharSeq.class);
  private boolean readEnabled;

  @Override
  public Process start(final List<String> mrOptions, final InputStream content) {
    try {
      final File input = File.createTempFile("input", ".txt");
      input.deleteOnExit();
      int index = 0;
      String command = "cat " + input.getAbsolutePath();
      File jarFile = null;

      while(index < mrOptions.size()) {
        String opt = mrOptions.get(index++);
        switch (opt) {
          case "-src": {
            final String contents = tableContents.get(mrOptions.get(index++));
            if (contents != null)
              StreamTools.writeChars(contents, input);
            break;
          }
          case "-map":
          case "-reduce":
            assert jarFile != null;
            command += " | " + CharSeqTools.replace(mrOptions.get(index++).replace(jarFile.getName(), jarFile.getAbsolutePath()), "$", "\\$");
            break;
          case "-read": {
            if (readEnabled) {
              final String contents = tableContents.get(mrOptions.get(index++));
              if (contents != null)
                StreamTools.writeChars(contents, input);
            }
            break;
          }
          case "-count":
            command += " | head -" + mrOptions.get(index++);
            break;
          case "-file":
            jarFile = new File(mrOptions.get(index++));
            command = command.replace(jarFile.getName(), jarFile.getAbsolutePath());
            break;
          case "-drop":
            return null;
        }
      }

      commands.add(new CharSeqAdapter(command));
      final Process result = Runtime.getRuntime().exec("bash -s");
      StreamTools.writeChars(command, result.getOutputStream());
      return result;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void setContent(final String tableName, final CharSequence content) {
    tableContents.put(tableName, content.toString());
  }

  public void setReadEnabled(final boolean readEnabled) {
    this.readEnabled = readEnabled;
  }
}
