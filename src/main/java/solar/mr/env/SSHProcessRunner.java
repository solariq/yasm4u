package solar.mr.env;

import java.io.*;
import java.nio.charset.Charset;
import java.util.List;


import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeqBuilder;

/**
 * User: solar
 * Date: 25.09.14
 * Time: 12:28
 */
public class SSHProcessRunner implements ProcessRunner {
  private final String proxyHost;
  private final String mrBinaryPath;

  public SSHProcessRunner(final String proxyHost, final String binaryPath) {
    this.proxyHost = proxyHost;
    this.mrBinaryPath = binaryPath;
  }

  @Override
  public Process start(final List<String> options, final Reader input) {
    try {
      final String pid;
      final String remoteOutput;
      Process process = Runtime.getRuntime().exec("ssh " + proxyHost + " bash -s");

      { // Run command
        final Writer toProxy = new OutputStreamWriter(process.getOutputStream(), Charset.forName("UTF-8"));
        final LineNumberReader fromProxy = new LineNumberReader(new InputStreamReader(process.getInputStream(), Charset.forName("UTF-8")));

//        toProxy.append("export YT_PREFIX=//userdata/;\n");
//        toProxy.append("export MR_RUNTIME=YT;\n");
//        toProxy.flush();
//        toProxy.append("echo $MR_RUNTIME;\n"); toProxy.flush();
//        System.out.println(fromProxy.readLine());
        File remoteJar = null;
        File jarFile = null;
        int index = 0;
        CharSeqBuilder command = new CharSeqBuilder();
        command.append(mrBinaryPath);
        while (index < options.size()) {
          String opt = options.get(index++);
          switch (opt) {
            case "-map":
            case "-reduce":
              command.append(" ").append(opt);
              String routine = options.get(index++);
              if (remoteJar != null) {
                routine = routine.replace(jarFile.getName(), remoteJar.getName());
              }
              command.append(" \'").append(routine).append("\'");
              break;
            case "-file":
              command.append(" ").append(opt);
              jarFile = new File(options.get(index++));
              toProxy.append("mktemp --suffix .jar;\n");
              toProxy.flush();
              remoteJar = new File(fromProxy.readLine());
              toProxy.append("rm -f ").append(remoteJar.getAbsolutePath()).append(";echo Ok;\n");
              toProxy.flush();
              fromProxy.readLine();
              Runtime.getRuntime().exec("scp " + jarFile.getAbsolutePath() + " " + proxyHost + ":" + remoteJar).waitFor();
              command.append(" ").append(remoteJar.getAbsolutePath());
              break;
            default:
              command.append(" ").append(opt);
          }
        }
        if (input == null) {
          final StringBuilder finalCommand = new StringBuilder();
          toProxy.append("mktemp --suffix .txt;\n");
          toProxy.flush();
          remoteOutput = fromProxy.readLine();

          finalCommand.append("nohup ").append(command).append(" >").append(remoteOutput).append(" 2>&1 & echo $!\n");
          System.out.println(finalCommand);
          toProxy.append(finalCommand);
          toProxy.flush();
          pid = fromProxy.readLine();
          toProxy.close();
          fromProxy.close();
          final CharSequence errors = StreamTools.readStream(process.getErrorStream());
          if (errors.length() > 1)
            System.err.print(errors);
          process.waitFor();
          final File tempFile = File.createTempFile("wait", ".sh");
          //noinspection ResultOfMethodCallIgnored
          tempFile.delete();
          tempFile.deleteOnExit();

          final String waitCmd =
              "result=\"live\";\n"
              + "while [ \"$?\" != \"0\" -o \"$result\" = \"live\" ]; do\n"
              + "  sleep 1;\n"
              + "  result=`ssh " + proxyHost + " bash -c \"'if kill -0 " + pid
              + " 2>/dev/null; then echo live; else echo dead; fi'\"`;\n"
              + "done\n"
              + "ssh " + proxyHost + " cat " + remoteOutput + ";\n"
              + "ssh " + proxyHost + " rm -rf " + remoteOutput + " " + remoteJar + ";\n";
          StreamTools.writeChars(waitCmd, tempFile);
          process = Runtime.getRuntime().exec("bash " + tempFile.getAbsolutePath());
          return process;
        }
        else {
          final String finalCommand = "cat - | " + command + "; echo $?";
          System.out.println(finalCommand);
          toProxy.append(finalCommand).append("\n");
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
}
