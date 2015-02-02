package solar.mr.env;

import com.spbsu.commons.io.StreamTools;


import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by minamoto on 01/02/15.
 */
public class LocalProcessRunner implements ProcessRunner {
  private final String binaryPath;

  public LocalProcessRunner(final String binaryPath) {
    this.binaryPath = binaryPath;
    initShell();
  }

  @Override
  public Process start(final List<String> options, final InputStream input) {

    final Process shell;
    Writer toShell;
    LineNumberReader fromShell;
    try {
      shell = Runtime.getRuntime().exec("bash -s");
      toShell = new OutputStreamWriter(shell.getOutputStream(), Charset.forName("UTF-8"));
      fromShell = new LineNumberReader(new InputStreamReader(shell.getInputStream(), Charset.forName("UTF-8")));
      toShell.append("echo Ok\n");
      toShell.flush();
      if (!"Ok".equals(fromShell.readLine())){
        throw new RuntimeException("shell service isn't lunched!!!");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final ArrayList<String> command = new ArrayList<>();
    command.add(binaryPath);
    for (final String o:options) {
        for (final String part:o.replace("$", ".").split(" "))
	    command.add(part);
    }

    final StringBuilder sb = new StringBuilder();
    if (input != null) {
      sb.append("cat - |");
    }
    for (final String c:command) {
      sb.append(c).append(" ");
    }

    try {
      final File output = File.createTempFile("yasm4u", "out");
      output.deleteOnExit();

      final File error = File.createTempFile("yasm4u", "err");
      error.deleteOnExit();
      sb.append(" 1> " + output.getAbsolutePath() + " 2>" + error.getAbsolutePath()
          + "; echo \"<EOF>\";exit\n");

      final String cmd = sb.toString();
      println(cmd);
      if (input != null) {
        toShell.append(sb.toString());
        toShell.flush();
        StreamTools.transferData(new InputStreamReader(input, Charset.forName("UTF-8")), toShell);
        toShell.close();
        fromShell.readLine(); /* eats <EOF> */
        fromShell.close();
        shell.waitFor();
        shell.destroy();
        return null;
      }
      else {

        toShell.append(sb.toString());
        toShell.flush();
        fromShell.readLine(); /* eats <EOF> */
        toShell.close();
        fromShell.close();
        shell.waitFor();
        shell.destroy();
        return new Process() {
          @Override
          public OutputStream getOutputStream() {
            return null;
          }

          @Override
          public InputStream getInputStream() {
            try {
              return new FileInputStream(output);
            } catch (FileNotFoundException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public InputStream getErrorStream() {
            try {
              return new FileInputStream(error);
            } catch (FileNotFoundException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public int waitFor() throws InterruptedException {
            return 0;
          }

          @Override
          public int exitValue() {
            return 0;
          }

          @Override
          public void destroy() {
            //shell.destroy();
          }
        };
      }
    }
    catch (IOException|InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void initShell() {
  }

  private final void println(final String cmd) {
    System.out.println(System.currentTimeMillis() + ":" + cmd);
  }
}
