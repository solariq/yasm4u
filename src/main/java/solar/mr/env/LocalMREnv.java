package solar.mr.env;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.Pair;
import org.apache.commons.io.FileUtils;
import solar.mr.*;
import solar.mr.proc.impl.MRPath;
import solar.mr.routines.MRRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 11:34
 */
public class LocalMREnv implements MREnv {
  public static final String DEFAULT_HOME = System.getenv("HOME") + "/.MRSamples";
  private final File home;

  public static LocalMREnv createTemp() {
    try {
      final File temp = File.createTempFile("mrenv", "local");
      //noinspection ResultOfMethodCallIgnored
      temp.delete();
      //noinspection ResultOfMethodCallIgnored
      temp.mkdir();
      temp.deleteOnExit();
      return new LocalMREnv(temp.getAbsolutePath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public LocalMREnv(String home) {
    if(home.charAt(home.length() - 1) != '/')
      home = home + "/";

    this.home = new File(home);
    if (!this.home.isDirectory())
      //noinspection ResultOfMethodCallIgnored
      this.home.delete();
    if (!this.home.exists())
      try {
        FileUtils.forceMkdir(this.home);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
  }

  public LocalMREnv() {
    this(DEFAULT_HOME);
  }

  @Override
  public boolean execute(MRRoutineBuilder builder, final MRErrorsHandler errorsHandler) {
    final MROutputBase output = new MROutput2MREnv(this, builder.output(), errorsHandler);
    final MRRoutine routine = builder.build(output);

    for (final MRPath path : routine.input()) {
      read(path, routine);
    }
    routine.invoke(CharSeq.EMPTY);

    output.interrupt();
    output.join();
    return output.errorsCount() == 0;
  }

  @Override
  public MRTableState resolve(MRPath path) {
    final File file = file(path);
    if (file.exists()) {
      final long[] recordsAndKeys = countRecordsAndKeys(file);
      return new MRTableState(file.getAbsolutePath(), true, path.sorted, crc(file), length(file), recordsAndKeys[1], recordsAndKeys[0], modtime(file), System.currentTimeMillis());
    }
    return new MRTableState(file.getAbsolutePath(), path.sorted);
  }

  @Override
  public MRTableState[] resolveAll(MRPath[] paths) {
    final MRTableState[] result = new MRTableState[paths.length];
    for(int i = 0; i < result.length; i++) {
      result[i] = resolve(paths[i]);
    }
    return result;
  }

  @Override
  public int read(final MRPath shard, final Processor<MRRecord> seq) {
    try {
      final int[] counter = new int[]{0};
      final File file = file(shard);
      if (file.exists())
        CharSeqTools.processLines(new FileReader(file), new MRRoutine(shard) {
          @Override
          public void process(final MRRecord arg) {
            counter[0]++;
            seq.process(arg);
          }
        });
      return counter[0];
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(final MRPath shard, final Reader content) {
    writeFile(content, shard, false);
  }

  @Override
  public void append(final MRPath shard, final Reader content) {
    writeFile(content, shard, true);
  }

  private void writeFile(final Reader content, final MRPath shard, final boolean append) {
    final File file = file(shard);
    file.getParentFile().mkdirs();
    File tempFile = new File(file.getAbsolutePath() + ".temp");
    try {
      if (append)
        FileUtils.copyFile(file, tempFile);

      try (final FileWriter out = new FileWriter(tempFile, true)) {
        StreamTools.transferData(content, out);
      }
      if (crc(file) != crc(tempFile)) {
        file.delete();
        tempFile.renameTo(file);
        tempFile = null;
        if (!shard.sorted && !shard.isDirectory())
          //noinspection ResultOfMethodCallIgnored
          file(new MRPath(shard.mount, shard.path, true)).delete();
      }
    }
    catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    finally {
      if (tempFile != null)
        tempFile.delete();
    }
  }

  @Override
  public void delete(final MRPath shard) {
    try {
      final File sorted = file(new MRPath(shard.mount, shard.path, true));
      if (sorted.exists())
        FileUtils.forceDelete(sorted);
      final File unsorted = file(new MRPath(shard.mount, shard.path, false));
      if (unsorted.exists())
        FileUtils.forceDelete(unsorted);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sample(final MRPath shard, final Processor<MRRecord> seq) {
    // the table could be empty, so no sample for such a table
    final File file = file(shard);
    if (file.exists()) {
      try {
        CharSeqTools.processLines(new FileReader(file), new MRRoutine(shard) {
          @Override
          public void process(final MRRecord arg) {
            seq.process(arg);
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public MRPath[] list(MRPath prefix) {
    final File prefixFile = file(prefix);
    final Map<String, MRPath> result = new HashMap<>();
    StreamTools.visitFiles(prefixFile, new Processor<String>() {
      @Override
      public void process(String path) {
        final MRPath file = findByFile(new File(prefixFile, path));
        if (!file.isDirectory()) {
          if (file.sorted || !result.containsKey(file.absolutePath()))
            result.put(file.absolutePath(), file);
        }
      }
    });

    return result.values().toArray(new MRPath[result.size()]);
  }

  @Override
  public void copy(MRPath[] from, MRPath to, boolean append) {
    try {
      for(int i = 0; i < from.length; i++) {
        final File file = file(from[i]);
        if (file.exists())
          writeFile(new FileReader(file), to, append || i > 0);
      }
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sort(final MRPath shard) {
    final File sortedFile = file(new MRPath(shard.mount, shard.path, true));
    if (sortedFile.exists())
      return;
    final List<Pair<String, MRRecord>> sort = new ArrayList<>();
    read(shard, new Processor<MRRecord>() {
      @Override
      public void process(final MRRecord arg) {
        sort.add(Pair.create(arg.key + '\t' + arg.sub, arg));
      }
    });
    Collections.sort(sort, new Comparator<Pair<String, MRRecord>>() {
      @Override
      public int compare(Pair<String, MRRecord> o1, Pair<String, MRRecord> o2) {
        return o1.getFirst().compareTo(o2.getFirst());
      }
    });
    try {
      FileUtils.forceMkdir(sortedFile.getParentFile());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try (final FileWriter out = new FileWriter(sortedFile)) {
      for (int i = 0; i < sort.size(); i++) {
        out.append(sort.get(i).getSecond().toString());
        out.append("\n");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private long modtime(File file) {
    try {
      final BasicFileAttributes attributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
      return attributes.lastModifiedTime().toMillis();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String name() {
    return "LocalMR://" + home.getAbsolutePath() + "/";
  }

  public File file(final String path, boolean sorted) {
    final File file = new File(home, path + (sorted ? ".txt.sorted" : ".txt"));
    if (!file.getParentFile().exists()) {
      try {
        FileUtils.forceMkdir(file.getParentFile());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return file;
  }

  public MRPath findByFile(File file) {
    final String absolutePath = file.getAbsolutePath();
    final String homePrefix = home.getAbsolutePath();
    String path = absolutePath.substring(homePrefix.length());
    MRPath.Mount mnt;
    if (path.startsWith("/home/")) {
      mnt = MRPath.Mount.HOME;
      path = path.substring("/home/".length());
    }
    else if (path.startsWith("/temp/")) {
      mnt = MRPath.Mount.TEMP;
      path = path.substring("/temp/".length());
    }
    else if (path.startsWith("/")){
      mnt = MRPath.Mount.ROOT;
      path = path.substring("/".length());
    }
    else throw new IllegalArgumentException(path);

    boolean sorted = false;
    if (path.endsWith(".txt")) {
      path = path.substring(0, path.length() - ".txt".length());
    } else if (path.endsWith(".txt.sorted")) {
      path = path.substring(0, path.length() - ".txt.sorted".length());
      sorted = true;
    } else path = path + "/";

    return new MRPath(mnt, path, sorted);
  }

  public File file(final MRPath path) {
    final StringBuilder fullPath = new StringBuilder();
    fullPath.append(home.getAbsolutePath()).append("/");
    switch (path.mount) {
      case TEMP:
        fullPath.append("temp/");
        break;
      case HOME:
        fullPath.append("home/");
        break;
      case ROOT:
        break;
    }
    fullPath.append(path.path);
    if (path.isDirectory())
      return new File(fullPath.toString());
    else if (path.sorted)
      return new File(fullPath.toString() + ".txt.sorted");
    else
      return new File(fullPath.toString() + ".txt");
  }

  private String crc(File file) {
    return "" + file.length();
  }

  private long length(File file) {
    return file.length();
  }

  private long[] countRecordsAndKeys(File file) {
    final long[] recordsAnsKeys = {0, 0};
    if (!file.exists()) {
      return recordsAnsKeys;
    }
    final Set<String> keys = new HashSet<>();
    try {
      StreamTools.readFile(file, new Processor<CharSequence>() {
        CharSequence[] parts = new CharSequence[2];
        @Override
        public void process(CharSequence arg) {
          parts = CharSeqTools.split(arg, '\t', parts);
          keys.add(parts[0].toString());
          recordsAnsKeys[0]++;
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    recordsAnsKeys[1] = keys.size();
    return recordsAnsKeys;
  }
}
