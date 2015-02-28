package solar.mr.env;

import com.spbsu.commons.func.Action;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.logging.Interval;
import com.sun.tools.javac.util.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import solar.mr.proc.impl.WhiteboardImpl;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;

/**
 * User: solar
 * Date: 25.09.14
 * Time: 12:28
 */
public class SSHProcessRunner implements ProcessRunner {
  public static final String SSH_COMMAND = "ssh -o PasswordAuthentication=no -o ConnectionAttempts=1 -o ConnectTimeout=1";
  private static Logger LOG = Logger.getLogger(SSHProcessRunner.class);
  private final String proxyHost;
  private final String mrBinaryPath;
  private volatile Process process;
  private volatile Writer toProxy;
  private volatile LineNumberReader fromProxy;

  public SSHProcessRunner(final String proxyHost, final String binaryPath) {
    this.proxyHost = proxyHost;
    this.mrBinaryPath = binaryPath;
    initProxyLink();
  }

  private void initProxyLink() {
    if (process != null) {
      try {
        if (process.exitValue() != 0)
          LOG.warn("SSH connection dropped, exit code " + process.exitValue());
      }
      catch (IllegalThreadStateException is) { // the process is alive
        return;
      }
    }
    while (true) {
      try {
        process = Runtime.getRuntime().exec(SSH_COMMAND + " " + proxyHost + " bash -s");
        toProxy = new OutputStreamWriter(process.getOutputStream(), Charset.forName("UTF-8"));
        fromProxy = new LineNumberReader(new InputStreamReader(process.getInputStream(), Charset.forName("UTF-8")));
        toProxy.append("export YT_ERROR_FORMAT=json\n");
        toProxy.flush();
        toProxy.append("echo Ok\n");
        toProxy.flush();
        final String response = fromProxy.readLine();
        if (response != null && response.equals("Ok"))
          break;
        StreamTools.transferData(process.getErrorStream(), System.err);
        process.getErrorStream().close();
        toProxy.close();
        fromProxy.close();
        process.waitFor();
      }
      catch (IOException e) {
        // skip
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    final Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          StreamTools.transferData(process.getErrorStream(), System.err, true);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    thread.setDaemon(true);
    thread.start();
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    close();
  }

  @Override
  public Process start(final List<String> options, final InputStream input) {
    try {
      initProxyLink();

      { // Run command

        final List<File> localResources = new ArrayList<>();
        final List<File> remoteResources = new ArrayList<>();
        { //hunting for resources
          int index = 0;
          while (index < options.size()) {
            final String opt = options.get(index++);
            switch (opt) {
              case "--local-file":
              case "--reduce-local-file":
              case "-file": {
                final File localResource = new File(options.get(index));
                localResources.add(localResource);
                final String name = localResource.getName();
                final String ext = name.lastIndexOf('.') >= 0 ? name.substring(name.lastIndexOf('.')) : "";
                final String remoteFile = transferFile(localResource.toURI().toURL(), ext, remoteResources, false);
                options.set(index, remoteFile);
                index++;
                break;
              }
            }
          }
        }
        int index = 0;
        CharSequence command;
        {
          CharSeqBuilder commandBuilder = new CharSeqBuilder();
          commandBuilder.append(mrBinaryPath);
          while (index < options.size()) {
            final String opt = options.get(index++);
            commandBuilder.append(" \"").append(opt.replace("$", ".").replace("\"", "\\\"")).append("\"");
          }
          command = commandBuilder.build();
          for (int i = 0; i < remoteResources.size(); i++) {
            final File remoteResource = remoteResources.get(i);
            final File localResource = remoteResources.get(i);
            command = CharSeqTools.replace(command, localResource.getName(), remoteResource.getName());
          }
        }

        if (input == null) {
          final String runner = transferFile(SSHProcessRunner.class.getResource("/mr/ssh/runner.pl"), ".pl", remoteResources, true);
          final StringBuilder finalCommand = new StringBuilder();
          println(command.toString() + "\n");
          finalCommand.append("perl ").append(runner).append(" run ").append(command).append(" 2>>/tmp/runner-errors-").append(WhiteboardImpl.USER).append(".txt\n");
          final String commandOutput = communicate(finalCommand.toString());
          final CharSequence[] split = CharSeqTools.split(commandOutput, "\t");
          final String pid = split[0].toString();
          final String output = split[1].toString();

          final File tempFile = generateWaitScript(remoteResources, runner, pid, output);
          final ProcessBuilder builder = new ProcessBuilder("bash", tempFile.getAbsolutePath());
          return builder.start();
        }
        else {
          final Process process = Runtime.getRuntime().exec(SSH_COMMAND + " " + proxyHost + " bash -s");
          final OutputStream toProxy = process.getOutputStream();
          final LineNumberReader fromProxy = new LineNumberReader(new InputStreamReader(process.getInputStream(), Charset.forName("UTF-8")));

          final String finalCommand = "cat - | " + command + " >/dev/null; echo $?\n";
          println(finalCommand);
          toProxy.write(finalCommand.getBytes(StreamTools.UTF));
          toProxy.flush();
          StreamTools.transferData(input, toProxy);
          toProxy.close();
          final int rc = Integer.parseInt(fromProxy.readLine());
          final CharSequence errors = StreamTools.readStream(process.getErrorStream());
          if (errors.length() > 1)
            System.err.print(errors);
          if (rc != 0)
            throw new RuntimeException("Write process exited with status other then 0");
          fromProxy.close();
          process.waitFor();
          return null;
        }
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private File generateWaitScript(List<File> remoteResources, String runner, String pid, String output) throws IOException {
    final File tempFile = File.createTempFile("wait", ".sh");
    //noinspection ResultOfMethodCallIgnored
    tempFile.delete();
    tempFile.deleteOnExit();

    final StringBuilder waitCmd = new StringBuilder();
    waitCmd.append("SSH=\"").append(SSH_COMMAND).append("\"\n");
    waitCmd.append("remote=").append(proxyHost).append("\n");
    waitCmd.append("runner=").append(runner).append("\n");
    waitCmd.append("pid=").append(pid).append("\n");
    waitCmd.append("dir=").append(output).append("\n");
    waitCmd.append("\n");
    waitCmd.append("function gc() {\n");
    for (final File remoteResource : remoteResources) {
      waitCmd.append("  " + SSH_COMMAND + " ").append(proxyHost).append(" rm -rf ").append(remoteResource.getAbsolutePath()).append(";\n");
    }
    waitCmd.append("}\n");
    waitCmd.append(StreamTools.readStream(SSHProcessRunner.class.getResourceAsStream("/mr/ssh/wait.sh")));
    StreamTools.writeChars(waitCmd, tempFile);
    return tempFile;
  }

  @Override
  public Process start(String... options) {
    return start(Arrays.asList(options), null);
  }

  @Override
  public Process start(InputStream input, String... options) {
    return start(Arrays.asList(options), input);
  }

  @Override
  public void close() {
    try {
      process.destroy();
      toProxy.close();
      fromProxy.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String transferFile(URL url, @Nullable String suffix, List<File> remoteResources, boolean isText) throws IOException, InterruptedException {
    final String remoteTmpRunner = communicate("mktemp" + (suffix != null ? " --suffix " + suffix : "") + ";");
    //System.out.println("remoteTmpRunner = " + remoteTmpRunner);

    final File remoteResource = new File(remoteTmpRunner);
    final String outputResult = communicate("rm -f " + remoteResource.getAbsolutePath() + "; echo Ok;");
    if (!"Ok".equals(outputResult))
      throw new RuntimeException("Ssh Ok-test wasn't passed!");
    //System.out.println("outputResult = " + outputResult);
    if (isText) {
      final StringBuilder builder = new StringBuilder();
      final int[] countBytes = new int[1];
      CharSeqTools.processLines(new InputStreamReader(url.openStream()), new Action<CharSequence>() {
        @Override
        public void invoke(CharSequence sequence) {
          countBytes[0] += sequence.toString().getBytes(StreamTools.UTF).length + 1;
          builder.append(sequence).append("\n");
        }
      });
      final String result = communicate("dd bs=1 count=" + countBytes[0] + " 2>/dev/null >" + remoteResource.getAbsolutePath() + ";\n" + builder.toString() + "echo Ok;");
      if (!"Ok".equals(result))
        throw new RuntimeException();
    }
    else if ("file".equals(url.getProtocol())) {
      int rc = 1;
      int delay = 1000;
      int tries = 0;
      while (rc != 0) {
        rc = Runtime.getRuntime().exec("scp " + url.getFile() + " " + proxyHost + ":" + remoteResource.getAbsolutePath()).waitFor();
        if (rc != 0) {
          Thread.sleep(delay * tries++);
        }
      }
    }
    else {
      final Process exec = Runtime.getRuntime().exec("ssh -o PasswordAuthentication=no proxyHost 'bash cat - > " + remoteResource.getAbsolutePath() + "'");
      final InputStream in = url.openStream();
      final OutputStream out = exec.getOutputStream();
      StreamTools.transferData(in, out);
      in.close();
      out.close();
      exec.waitFor();
    }
    remoteResources.add(remoteResource);
    return remoteResource.getAbsolutePath();
  }

  private String communicate(String command) throws IOException {
    String result;
    do {
      initProxyLink();
      toProxy.append(command).append('\n');
      toProxy.flush();
      result = fromProxy.readLine();
    }
    while (result == null);
    return result;
  }

  private static void println(String finalCommand) {
    System.out.print(System.currentTimeMillis() + ": " + finalCommand);
  }

}
