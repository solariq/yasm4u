package com.expleague.yasm4u.domains.mr.env;

import com.expleague.commons.io.StreamTools;
import com.expleague.commons.seq.CharSeqBuilder;
import com.expleague.commons.seq.CharSeqTools;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import com.expleague.yasm4u.domains.wb.impl.WhiteboardImpl;

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
  private static Logger LOG = Logger.getLogger(SSHProcessRunner.class);
  private final String proxyHost;
  private final String mrBinaryPath;
  private final Set<String> optionsWithFile = new HashSet<>();
  private volatile Process process;
  private volatile Writer toProxy;
  private volatile LineNumberReader fromProxy;
  private String privateKey;

  public SSHProcessRunner(final String proxyHost, final String binaryPath) {
    this.proxyHost = proxyHost;
    this.mrBinaryPath = binaryPath;
  }

  public void addOptionWithFile(String opt) {
    optionsWithFile.add(opt);
  }

  private synchronized boolean initProxyLink() {
    if (process != null) {
      try {
        if (process.exitValue() != 0)
          LOG.warn("SSH connection dropped, exit code " + process.exitValue());
      }
      catch (IllegalThreadStateException is) { // the process is alive
        return false;
      }
    }
    while (true) {
      try {
        process = Runtime.getRuntime().exec(sshCommand() + " " + proxyHost + " bash -s");
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
    final Thread thread = new Thread(() -> {
      try {
        StreamTools.transferData(process.getErrorStream(), System.err, true);
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
    thread.setDaemon(true);
    thread.start();
    return true;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    close();
  }

  @Override
  public Process start(final List<String> options, final Set<String> files, final InputStream input) {
    try {
      initProxyLink();

      { // Run command

        final List<File> localResources = new ArrayList<>();
        final List<String> remoteResources = new ArrayList<>();
        { //hunting for resources
          int index = 0;
          while (index < options.size()) {
            final String opt = options.get(index++);
            if (files.contains(opt) || optionsWithFile.contains(opt)) {
              if (files.contains(opt))
                index--;
              final File localResource = new File(options.get(index));
              localResources.add(localResource);
              final String name = localResource.getName();
              final String ext = name.lastIndexOf('.') >= 0 ? name.substring(name.lastIndexOf('.')) : "";
              final String remoteFile = transferFile(localResource.toURI().toURL(), ext, remoteResources, false);
              options.set(index, remoteFile);
              index++;
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
            final File remoteResource = new File(remoteResources.get(i));
            final File localResource = localResources.get(i);
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

          final File waitScript = generateWaitScript(remoteResources, runner, pid, output);
          // TODO: Hack to make Windows work
          final ProcessBuilder builder;
          if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            builder = new ProcessBuilder("cmd", "/c", "bash -c '(" + waitScript.getAbsolutePath().replaceAll("\\\\", "/") + ")'");
          } else {
            builder = new ProcessBuilder("bash", waitScript.getAbsolutePath());
          }
          return builder.start();
        }
        else {
          final Process process = Runtime.getRuntime().exec(sshCommand()  + " " + proxyHost + " bash -s");
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

  @NotNull
  private String sshCommand() {
    return "ssh -o PasswordAuthentication=no -o ConnectionAttempts=1 -o ConnectTimeout=1" + (privateKey != null ? " -i " + privateKey: "");
  }

  @NotNull
  private String scpCommand() {
    return "scp" + (privateKey != null ? " -i " + privateKey : "");
  }

  private File generateWaitScript(List<String> remoteResources, String runner, String pid, String output) throws IOException {
    final File tempFile = File.createTempFile("wait", ".sh");
    //noinspection ResultOfMethodCallIgnored
    tempFile.delete();
    tempFile.deleteOnExit();

    final StringBuilder waitCmd = new StringBuilder();
    // waitCmd.append("set -x\n");
    // waitCmd.append("USER=").append(WhiteboardImpl.USER).append("\n");
    waitCmd.append("SSH=\"").append(sshCommand()).append("\"\n");
    waitCmd.append("remote=").append(proxyHost).append("\n");
    waitCmd.append("runner=").append(runner).append("\n");
    waitCmd.append("pid=").append(pid).append("\n");
    waitCmd.append("dir=").append(output).append("\n");
    waitCmd.append("\n");
    waitCmd.append("function gc() {\n");
    for (final String remoteResource : remoteResources) {
      waitCmd.append("  ").append(sshCommand()).append(" ").append(proxyHost).append(" rm -rf ").append(remoteResource).append(";\n");
    }
    waitCmd.append("}\n");
    waitCmd.append(StreamTools.readStream(SSHProcessRunner.class.getResourceAsStream("/mr/ssh/wait.sh")).toString().replaceAll("\r", "\n"));
    StreamTools.writeChars(waitCmd, tempFile);
    return tempFile;
  }

  @Override
  public synchronized void close() {
    try {
      process.destroy();
      toProxy.close();
      fromProxy.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String transferFile(URL url, @Nullable String suffix, List<String> remoteResources, boolean isText) throws IOException, InterruptedException {
    final String remoteTmpRunner = communicate("mktemp" + (suffix != null ? " --suffix " + suffix : "") + ";");
    //System.out.println("remoteTmpRunner = " + remoteTmpRunner);

    final String absolutePath = remoteTmpRunner;
    final String outputResult = communicate("rm -f " + absolutePath + " 2> /dev/null 1>&2; echo Ok;");
    if (!"Ok".equals(outputResult))
      throw new RuntimeException("Ssh Ok-test wasn't passed, found [" + outputResult + "]!");
    //System.out.println("outputResult = " + outputResult);
    if (isText) {
      final StringBuilder builder = new StringBuilder();
      final int[] countBytes = new int[1];
      CharSeqTools.processLines(new InputStreamReader(url.openStream()), sequence -> {
        String data = sequence.toString().replace('\r', ' ');
        countBytes[0] += data.getBytes(StreamTools.UTF).length + 1;
        builder.append(data).append("\n");
      });
      final String result = communicate("dd bs=1 count=" + countBytes[0] + " 2>/dev/null >" + absolutePath + ";\n" + builder.toString() + "echo Ok;");
      if (!"Ok".equals(result))
        throw new RuntimeException();
    }
    else if ("file".equals(url.getProtocol())) {
      int rc = 1;
      int delay = 1000;
      int tries = 0;
      while (rc != 0) {
        if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
          // TODO: Hack to make scp from cygwin to work on Windows
          rc = Runtime.getRuntime().exec(scpCommand() + " " + url.getFile().substring(1).replace("C:", "/cygdrive/c").replace("c:", "/cygdrive/c") + " " + proxyHost + ":" + absolutePath).waitFor();
        } else {
          rc = Runtime.getRuntime().exec(scpCommand() + " " + url.getFile() + " " + proxyHost + ":" + absolutePath).waitFor();
        }
        if (rc != 0) {
          Thread.sleep(delay * tries++);
        }
      }
    }
    else {
      final Process exec = Runtime.getRuntime().exec(sshCommand() + " 'bash cat - > " + absolutePath + "'");
      final InputStream in = url.openStream();
      final OutputStream out = exec.getOutputStream();
      StreamTools.transferData(in, out);
      in.close();
      out.close();
      exec.waitFor();
    }
    remoteResources.add(absolutePath);
    return absolutePath;
  }

  private synchronized String communicate(String command) throws IOException {
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

  public void setPrivateKey(String pathTo) {
    this.privateKey = pathTo;
  }
}
