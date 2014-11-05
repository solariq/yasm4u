package solar.mr.env;

import javax.annotation.Nullable;
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
public class RemoteYaMREnvironment extends YaMREnvBase {
  private final String proxyHost;
  private final String mrBinaryPath;

  public RemoteYaMREnvironment(final String proxyHost, final String mrBinaryPath, final String yamrMaster, final String yamrUser) {
    super(yamrUser, yamrMaster);
    this.proxyHost = proxyHost;
    this.mrBinaryPath = mrBinaryPath;
  }

  @Override
  protected Process runYaMRProcess(final List<String> mrOptions, @Nullable final Reader content) {
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
        while (index < mrOptions.size()) {
          String opt = mrOptions.get(index++);
          switch (opt) {
            case "-map":
            case "-reduce":
              command.append(" ").append(opt);
              String routine = mrOptions.get(index++);
              if (remoteJar != null) {
                routine = routine.replace(jarFile.getName(), remoteJar.getName());
              }
              command.append(" \'").append(routine).append("\'");
              break;
            case "-file":
              command.append(" ").append(opt);
              jarFile = new File(mrOptions.get(index++));
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
        if (content == null) {
          final StringBuilder finalCommand = new StringBuilder();
          toProxy.append("mktemp --suffix .txt;\n");
          toProxy.flush();
          remoteOutput = fromProxy.readLine();

          finalCommand.append("nohup ").append(command).append(" >").append(remoteOutput).append(" 2>&1 & echo $!\n");
//          System.out.println(finalCommand);
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
              + "ssh " + proxyHost + " cat " + remoteOutput + ";\n";
          StreamTools.writeChars(waitCmd, tempFile);
          process = Runtime.getRuntime().exec("bash " + tempFile.getAbsolutePath());
          StreamTools.writeChars(waitCmd, process.getOutputStream());
          return process;
        }
        else {
//          System.out.println(command);
          toProxy.append(command).append("\n");
          toProxy.flush();
          StreamTools.transferData(content, toProxy);
          toProxy.close();
          fromProxy.close();
          final CharSequence errors = StreamTools.readStream(process.getErrorStream());
          if (errors.length() > 1)
            System.err.print(errors);
          process.waitFor();
          return null;
        }
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
