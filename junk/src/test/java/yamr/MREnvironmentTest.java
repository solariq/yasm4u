package yamr;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import com.spbsu.commons.func.Processor;
import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.seq.*;
import com.spbsu.commons.seq.regexp.SimpleRegExp;
import com.spbsu.commons.util.Pair;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import solar.mr.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 13:09
 */
public class MREnvironmentTest {
  final TestMREnvironment testEnvironment = new TestMREnvironment();
  private File samplesDirTmp;

  @Before
  public void setUp() throws IOException {
    samplesDirTmp = File.createTempFile("mrsamples", "");
    //noinspection ResultOfMethodCallIgnored
    samplesDirTmp.delete();
    FileUtils.forceMkdir(samplesDirTmp);
    testEnvironment.setMRSamplesDir(samplesDirTmp.getAbsolutePath());
    testEnvironment.setReadEnabled(false);
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.forceDelete(samplesDirTmp);
  }

  @Test
  public void testCommandLine() throws Exception {
    final CharSeqBuilder builder = new CharSeqBuilder();
    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg).append('\n');
      }
    });
    testEnvironment.execute(MyMap.class, new FixedMRTable("poh/neh"), new FixedMRTable("neh/poh"));
    final SimpleRegExp regExp = SimpleRegExp.create("java -Xmx1G -Xms1G -jar .*yamr-routine-.*jar .*\\$MyMap 2");

    final Seq<CharSeq> commands = testEnvironment.commands.build();
    assertTrue(regExp.match(commands.at(commands.length() - 2)));
  }

  @Test
  public void testJarExecutes() throws Exception {
    final CharSeqBuilder builder = new CharSeqBuilder();
    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg).append('\n');
      }
    });
    testEnvironment.execute(MyMap.class, new FixedMRTable("poh/neh"), new FixedMRTable("neh/poh"));
    assertEquals("Hello xyuJlo\n", builder.toString());
  }

  @Test
  public void testIllegalInput() throws Exception {
    final CharSeqBuilder builder = new CharSeqBuilder();
    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg).append('\n');
      }
    });
    testEnvironment.tableContents.put("poh/neh", "preved subkey medved");
    testEnvironment.execute(MyMap.class, new FixedMRTable("poh/neh"), new FixedMRTable("neh/poh"));
    assertEquals("2\nIllegal record\tContains less then 3 fields\tpreved subkey medved\n", builder.toString());
  }

  @Test
  public void testException() throws Exception {
    final CharSeqBuilder builder = new CharSeqBuilder();
    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg).append('\n');
      }
    });
    testEnvironment.execute(MyMapException.class, new FixedMRTable("poh/neh"), new FixedMRTable("neh/poh"));
    final SimpleRegExp regExp = SimpleRegExp.create("2\n.*java.lang.RuntimeException: Tut tozhe hello");

    assertTrue(regExp.match(builder));
  }

  @Test
  public void testMap() throws Exception {
    final CharSeqBuilder builder = new CharSeqBuilder();
    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg).append('\n');
      }
    });
    testEnvironment.execute(MyMapOutput.class, new FixedMRTable("poh/neh"), new FixedMRTable("neh/poh"));

    assertEquals("preved\t1\tmedved\n", builder.toString());
  }

  @Test
  public void testReduce1() throws Exception {
    final CharSeqBuilder builder = new CharSeqBuilder();
    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg).append('\n');
      }
    });
    testEnvironment.setContent("poh/neh-reduce", "preved\t1\tmedved\n"
                                                 + "preved\t2\tmedved\n"
                                                 + "preved\t3\tmedved\n"
    );
    testEnvironment.execute(MyReduceCounter.class, new FixedMRTable("poh/neh-reduce"), new FixedMRTable("neh/poh"));

    assertEquals("preved\tcount\t3\n", builder.toString());
  }

  @Test
  public void testReduce2() throws Exception {
    final CharSeqBuilder builder = new CharSeqBuilder();
    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
      @Override
      public void process(final CharSequence arg) {
        builder.append(arg).append('\n');
      }
    });
    testEnvironment.setContent("poh/neh-reduce", "preved\t1\tmedved\n"
                                                 + "preved\t2\tmedved\n"
                                                 + "preved\t3\tmedved\n"
                                                 + "nepreved\t1\tmedved\n");
    testEnvironment.execute(MyReduceCounter.class, new FixedMRTable("poh/neh-reduce"), new FixedMRTable("neh/poh"));

    assertEquals("preved\tcount\t3\nnepreved\tcount\t1\n", builder.toString());
  }

  private static class MyMap extends MRMap {
    public MyMap(final MROutput output) {
      super(output);
    }

    @Override
    public void map(final String key, final String sub, final CharSequence value) {
      System.out.println("Hello xyuJlo");
    }
  }

  private static class MyMapException extends MRMap {
    public MyMapException(final MROutput output) {
      super(output);
    }

    @Override
    public void map(final String key, final String sub, final CharSequence value) {
      throw new RuntimeException("Tut tozhe hello");
    }
  }

  private static class MyMapOutput extends MRMap {
    public MyMapOutput(final MROutput output) {
      super(output);
    }

    @Override
    public void map(final String key, final String sub, final CharSequence value) {
      output.add("preved", "1", "medved");
    }
  }

  private static class MyReduceCounter extends MRReduce {
    public MyReduceCounter(final MROutput output) {
      super(output);
    }

    @Override
    public void reduce(final String key, final Iterator<Pair<String, CharSequence>> reduce) {
      int count = 0;
      while (reduce.hasNext()) {
        reduce.next();
        count++;
      }
      output.add(key, "count", Integer.toString(count));
    }
  }

  private static class TestMREnvironment extends MREnvironment {
    private Map<String, String> tableContents = new HashMap<>();

    {
      tableContents.put("poh/neh", "preved\tsubkey\tmedved");
    }

    public SeqBuilder<CharSeq> commands = new ArraySeqBuilder<>(CharSeq.class);
    private boolean readEnabled;

    @Override
    protected Process generateExecCommand(final List<String> mrOptions) {
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
}
