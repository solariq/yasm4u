package solar.mr;

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
public class RemoteMREnvironment extends MREnvironment {
  private final String proxyHost;
  private final String mrBinaryPath;

  public RemoteMREnvironment(final String proxyHost, final String mrBinaryPath) {
    this.proxyHost = proxyHost;
    this.mrBinaryPath = mrBinaryPath;
  }

  @Override
  protected Process generateExecCommand(final List<String> mrOptions) {
    try {
      final String pid;
      final String remoteOutput;
      { // Run command
        final Process sshLink = Runtime.getRuntime().exec("ssh " + proxyHost + " bash -s");
        final Writer toProxy = new OutputStreamWriter(sshLink.getOutputStream(), Charset.forName("UTF-8"));
        final LineNumberReader fromProxy = new LineNumberReader(new InputStreamReader(sshLink.getInputStream(), Charset.forName("UTF-8")));

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
        toProxy.append("mktemp --suffix .txt;\n");
        toProxy.flush();
        remoteOutput = fromProxy.readLine();
        StringBuilder finalCommand = new StringBuilder();
        finalCommand.append("nohup ").append(command).append(" >").append(remoteOutput).append(" 2>&1 & echo $!\n");
        System.out.println(finalCommand);
        toProxy.append(finalCommand);
        toProxy.flush();
        pid = fromProxy.readLine();
        toProxy.close();
        fromProxy.close();
        System.err.print(StreamTools.readStream(sshLink.getErrorStream()));
        sshLink.waitFor();
      }

      final String waitCmd =
          "while ssh " + proxyHost + " 'kill -0 " + pid + " 2>/dev/null'; do sleep 1; done; ssh " + proxyHost + " cat " + remoteOutput + ";";
      final Process exec = Runtime.getRuntime().exec("bash -s");
      StreamTools.writeChars(waitCmd, exec.getOutputStream());
      return exec;
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
