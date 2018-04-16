package com.expleague.yasm4u.domains.mr.env;

import com.expleague.commons.io.StreamTools;
import com.expleague.commons.seq.CharSeq;
import com.expleague.commons.seq.CharSeqTools;
import com.expleague.commons.util.Pair;
import com.expleague.commons.util.cache.CacheStrategy;
import com.expleague.commons.util.cache.impl.FixedSizeCache;
import com.expleague.yasm4u.domains.mr.MRErrorsHandler;
import com.expleague.yasm4u.domains.mr.MRPath;
import com.expleague.yasm4u.domains.mr.ops.impl.MROperation;
import com.expleague.yasm4u.domains.mr.ops.impl.MRRoutineBuilder;
import com.expleague.yasm4u.domains.mr.ops.impl.MRTableState;
import org.apache.commons.io.FileUtils;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: solar
 * Date: 12.10.14
 * Time: 11:34
 */

public class LocalMREnv extends MREnvBase {
  @SuppressWarnings("WeakerAccess")
  public static final String DEFAULT_HOME = System.getProperty("user.home") + "/.MRSamples/" + System.currentTimeMillis();

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
    System.err.println("LocalMREnv: home: " + home);
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
    final MROperation routine = builder.build(output);
    try {
      Set<String> visitedFiles = new HashSet<>();
      for (final MRPath path : routine.input()) {
        final String absolutePath = file(path).getAbsolutePath();
        if (visitedFiles.contains(absolutePath))
          continue;
        read(path, routine);
        visitedFiles.add(absolutePath);
      }
      routine.recordParser().accept(CharSeq.EMPTY);
    }
    catch (Throwable th) {
      errorsHandler.error(th, routine.currentRecord());
    }

    output.interrupt();
    output.join();
    return output.errorsCount() == 0;
  }

  @Override
  public MRTableState resolve(MRPath path) {
    if (path.isDirectory())
      throw new IllegalArgumentException("Path must not be directory");
    final File file = file(path);
    if (file.getName().equals(".txt")) {
      System.err.println("path: " + path);
      throw new RuntimeException("empty file name");
    }
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
  public int read(final MRPath shard, final Consumer<MRRecord> seq) {
    final MROperation routine = new MROperation(shard) {
      @Override
      public void accept(final MRRecord arg) {
        seq.accept(arg);
      }
    };
    int count = read(shard, routine);
    routine.recordParser().accept(CharSeq.EMPTY);
    return count;
  }

  protected int read(MRPath shard, MROperation routine) {
    try {
      final File file = file(shard);
      if (file.exists()) {
        return CharSeqTools.processLines(new FileReader(file), routine.recordParser());
      }
      return 0;
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

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void writeFile(final Reader content, final MRPath shard, final boolean append) {
    final File file = file(shard);
    File tempFile = new File(file.getAbsolutePath() + ".temp");
    try {
      FileUtils.forceMkdir(file.getParentFile());
      if (append && file.exists())
        FileUtils.copyFile(file, tempFile);

      try (final FileWriter out = new FileWriter(tempFile, true)) {
        StreamTools.transferData(content, out);
      }

      file.delete();
      tempFile.renameTo(file);
      tempFile = null;
      if (!shard.isDirectory() && !shard.sorted)
        //noinspection ResultOfMethodCallIgnored
        file(shard.mksorted()).delete();
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
  public void sample(final MRPath shard, final Consumer<MRRecord> seq) {
    // the table could be empty, so no sample for such a table
    final File file = file(shard);
    if (file.exists()) {
      try {
        final MROperation routine = new MROperation(shard) {
          @Override
          public void accept(final MRRecord arg) {
            seq.accept(arg);
          }
        };
        CharSeqTools.processLines(new FileReader(file), routine.recordParser());
        routine.recordParser().accept(CharSeq.EMPTY);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void get(MRPath prefix) {
    if (prefix.isDirectory())
      throw new IllegalArgumentException("Prefix must be table");
    list(prefix.parent());
  }

  @Override
  public MRPath[] list(MRPath prefix) {
    if (!prefix.isDirectory())
      throw new IllegalArgumentException("Prefix must be directory");
    final File prefixFile = file(prefix);
    final Map<String, MRPath> result = new HashMap<>();
    StreamTools.visitFiles(prefixFile, path -> {
      final MRPath file = findByFile(new File(prefixFile, path));
      if (!file.isDirectory()) {
        if (file.sorted || !result.containsKey(file.absolutePath()))
          result.put(file.absolutePath(), file);
      }
    });

    return result.values().toArray(new MRPath[result.size()]);
  }

  @Override
  public void copy(MRPath[] from, MRPath to, boolean append) {
    try {
      for(int i = 0; i < from.length; i++) {
        final File file = file(from[i]);
        try (final FileReader reader = new FileReader(file)) {
          if (file.exists())
            writeFile(reader, to, append || i > 0);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sort(final MRPath shard) {
    final File sortedFile = file(new MRPath(shard.mount, shard.path, true));
    if (sortedFile.exists())
      return;
    final List<Pair<String, MRRecord>> sort = new ArrayList<>();
    read(shard, arg -> sort.add(Pair.create(arg.key + '\t' + arg.sub, arg)));
    sort.sort(Comparator.comparing(Pair::getFirst));
    try {
      FileUtils.forceMkdir(sortedFile.getParentFile());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try (final FileWriter out = new FileWriter(sortedFile)) {
      for (Pair<String, MRRecord> aSort : sort) {
        out.append(aSort.getSecond().toString());
        out.append("\n");
      }
        out.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long key(MRPath shard, String key, Consumer<MRRecord> seq) {
    return 0;
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

  private MRPath findByFile(File file) {
    final String absolutePath = file.getAbsolutePath();
    final String homePrefix = home.getAbsolutePath();
    String path = absolutePath.substring(homePrefix.length());
    MRPath.Mount mnt;
    final String HOME_PREFIX = File.separator + "home" + File.separator;
    final String TEMP_PREFIX = File.separator + "temp" + File.separator;
    final String ROOT_PREFIX = File.separator;
    if (path.startsWith(HOME_PREFIX)) {
      mnt = MRPath.Mount.HOME;
      path = path.substring(HOME_PREFIX.length());
    }
    else if (path.startsWith(TEMP_PREFIX)) {
      mnt = MRPath.Mount.TEMP;
      path = path.substring(TEMP_PREFIX.length());
    }
    else if (path.startsWith(ROOT_PREFIX)){
      mnt = MRPath.Mount.ROOT;
      path = path.substring(ROOT_PREFIX.length());
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

  private Pattern paradiso_pattern = Pattern.compile("/\\d{8}/\\d{8}(?:_\\d+)+$");
  private Pattern logbroker_pattern = Pattern.compile("mobilesearch-(\\w+)-log\\/\\d{4}-\\d{2}-\\d{2}\\/\\d{10}$");
  public File file(final MRPath path) {
    final StringBuilder fullPath = new StringBuilder();
    fullPath.append(home.getAbsolutePath()).append("/");
    switch (path.mount) {
      case LOG:
        fullPath.append("logs/");
        if (!Boolean.getBoolean("yasm4u.test")) {
          final Matcher matcher = paradiso_pattern.matcher(path.path);
          if(matcher.find()) // make single file for all dates
            fullPath.append(matcher.replaceAll(""));
          else
            fullPath.append(path.path);
        }
        else {
          fullPath.append(path.path);
        }
        break;
      case TEMP:
        fullPath.append("temp/");
        fullPath.append(path.path);
        break;
      case HOME:
        fullPath.append("home/");
        fullPath.append(path.path);
        break;
      case LOG_BROKER:
        fullPath.append("log_broker/");
        if (!Boolean.getBoolean("yasm4u.test")) {
          final Matcher matcher = logbroker_pattern.matcher(path.path);
          if(matcher.find()) // make single file for all dates
            fullPath.append(matcher.group(1));
          else
            fullPath.append(path.path);
        }
        else {
          fullPath.append(path.path);
        }
        break;
      case ROOT:
        fullPath.append(path.path);
        break;
    }
    if (path.isDirectory())
      return new File(fullPath.toString());
    else if (path.sorted)
      return new File(fullPath.toString() + ".txt.sorted");
    else {
      return new File(fullPath.toString() + ".txt");
    }
  }

  private String crc(File file) {
    return "" + file.length();
  }

  private long length(File file) {
    return file.length();
  }

  private FixedSizeCache<File, long[]> cache = new FixedSizeCache<>(1000, CacheStrategy.Type.LRU);
  private long[] countRecordsAndKeys(final File file) {
    return cache.get(file, argument -> {
      final long[] recordsAnsKeys = {0, 0};
      if (!file.exists()) {
        return recordsAnsKeys;
      }
      final Set<String> keys = new HashSet<>();
      try {
        StreamTools.readFile(file, new Consumer<CharSequence>() {
          CharSequence[] parts = new CharSequence[2];

          @Override
          public void accept(CharSequence arg) {
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
    });
  }

  public File home() {
    return home;
  }
}
