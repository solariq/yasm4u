package solar.mr.env;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeqBuilder;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.system.RuntimeUtils;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.Nullable;

/**
 * User: solar
 * Date: 25.09.14
 * Time: 12:28
 */
public class SSHProcessRunner implements ProcessRunner {
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
        process = Runtime.getRuntime().exec("ssh " + proxyHost + " bash -s");
        toProxy = new OutputStreamWriter(process.getOutputStream(), Charset.forName("UTF-8"));
        fromProxy = new LineNumberReader(new InputStreamReader(process.getInputStream(), Charset.forName("UTF-8")));
        toProxy.append("echo Ok\n");
        toProxy.flush();
        if (fromProxy.readLine().equals("Ok"))
          break;
        toProxy.close();
        fromProxy.close();
        process.waitFor();
      }
      catch (Exception e) {
        LOG.warn(e);
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
    toProxy.close();
    fromProxy.close();
    process.destroy();
  }

  @Override
  public Process start(final List<String> options, final InputStream input) {
    try {
      initProxyLink();

      { // Run command

//        toProxy.append("export YT_PREFIX=//userdata/;\n");
//        toProxy.append("export MR_RUNTIME=YT;\n");
//        toProxy.flush();
//        toProxy.append("echo $MR_RUNTIME;\n"); toProxy.flush();
//        System.out.println(fromProxy.readLine());
        List<File> localResources = new ArrayList<>();
        List<File> remoteResources = new ArrayList<>();
        int index = 0;
        CharSeqBuilder command = new CharSeqBuilder();
        command.append(mrBinaryPath);
        while (index < options.size()) {
          String opt = options.get(index++);
          switch (opt) {
            case "-map":
            case "-reduce":
            case "-reducews": {
              command.append(" ").append(opt);
              String routine = options.get(index++);
              for (int i = 0; i < remoteResources.size(); i++) {
                File remoteFile = remoteResources.get(i);
                File localFile = localResources.get(i);
                routine = routine.replace(localFile.getName(), remoteFile.getName());
              }
              command.append(" \'").append(routine).append("\'");
            }
              break;
            case "-file": {
              command.append(" ").append(opt);
              final File localResource = new File(options.get(index++));
              localResources.add(localResource);
              command.append(" ").append(transferFile(localResource.toURI().toURL(), ".jar", remoteResources));
              break;
            }
            case "--local-file":
            case "--reduce-local-file": {
              command.append(" ").append(opt);
              final File localResource = new File(options.get(index++));
              localResources.add(localResource);
              command.append(" ").append(transferFile(localResource.toURI().toURL(), ".jar", remoteResources));
              /* in Yt --local-file appears after map/reduce */
              for (int i = 0; i < remoteResources.size(); i++) {
                File remoteFile = remoteResources.get(i);
                File localFile = localResources.get(i);
                int optAbsoluteIndex = options.indexOf(localFile.getAbsolutePath());
                options.remove(optAbsoluteIndex);
                options.add(optAbsoluteIndex, remoteFile.getAbsolutePath());
                int optIndex = options.indexOf(localFile.getName());
                options.remove(optIndex);
                options.add(optIndex, remoteFile.getName());
              }
              break;
            }
            default:
              command.append(" ").append(opt.replace("$", "."));
          }
        }
        if (input == null) {
          final String runner = transferFile(SSHProcessRunner.class.getResource("/mr/ssh/runner.pl"), ".pl", remoteResources);
          final StringBuilder finalCommand = new StringBuilder();
          finalCommand.append("perl ").append(runner).append(" run ").append(command).append(" 2>>/tmp/runner-errors-" + System.getenv("USER") + ".txt\n");
          println(finalCommand.toString());
          toProxy.append(finalCommand);
          toProxy.flush();
          final CharSequence[] split = CharSeqTools.split(fromProxy.readLine(), "\t");
          final String pid = split[0].toString();
          final String output = split[1].toString();

          final File tempFile = File.createTempFile("wait", ".sh");
          //noinspection ResultOfMethodCallIgnored
          tempFile.delete();
          tempFile.deleteOnExit();

          final StringBuilder waitCmd = new StringBuilder();
          waitCmd.append("remote=").append(proxyHost).append("\n");
          waitCmd.append("runner=").append(runner).append("\n");
          waitCmd.append("pid=").append(pid).append("\n");
          waitCmd.append("dir=").append(output).append("\n");
          waitCmd.append("\n");
          waitCmd.append("function gc() {\n");
          for (final File remoteResource : remoteResources) {
            waitCmd.append("  ssh ").append(proxyHost).append(" rm -rf ").append(remoteResource.getAbsolutePath()).append(";\n");
          }
          waitCmd.append("}\n");
          waitCmd.append(StreamTools.readStream(SSHProcessRunner.class.getResourceAsStream("/mr/ssh/wait.sh")));
          StreamTools.writeChars(waitCmd, tempFile);
          return Runtime.getRuntime().exec("bash " + tempFile.getAbsolutePath());
        }
        else {
          final Process process = Runtime.getRuntime().exec("ssh " + proxyHost + " bash -s");
          final OutputStream toProxy = process.getOutputStream();
          final LineNumberReader fromProxy = new LineNumberReader(new InputStreamReader(process.getInputStream(), Charset.forName("UTF-8")));

          final String finalCommand = "cat - | " + command + "; echo $?\n";
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

  private String transferFile(URL url, @Nullable String suffix, List<File> remoteResources) throws IOException, InterruptedException {
    toProxy.append("mktemp").append(suffix != null ? " --suffix " + suffix : "").append(";\n");
    toProxy.flush();
    final File remoteResource = new File(fromProxy.readLine());
    toProxy.append("rm -f ").append(remoteResource.getAbsolutePath()).append(";echo Ok;\n");
    toProxy.flush();
    fromProxy.readLine();
    if ("file".equals(url.getProtocol())) {
      Runtime.getRuntime().exec("scp " + url.getFile() + " " + proxyHost + ":" + remoteResource.getAbsolutePath()).waitFor();
    }
    else {
      final Process exec = Runtime.getRuntime().exec("ssh proxyHost 'bash cat - > " + remoteResource.getAbsolutePath() + "'");
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

  private static void println(String finalCommand) {
    System.out.print(System.currentTimeMillis() + ": " + finalCommand);
  }

}
