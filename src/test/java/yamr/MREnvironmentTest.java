package yamr;

/**
 * User: solar
 * Date: 23.09.14
 * Time: 13:09
 */
public abstract class MREnvironmentTest {
// TODO: need to rework these tests
//  final YaMREnv testEnvironment = new YaMREnv(new TestProcessRunner(), "test", "localhost");
//  private File samplesDirTmp;
//
//  @Before
//  public void setUp() throws IOException {
//    samplesDirTmp = File.createTempFile("mrsamples", "");
//    //noinspection ResultOfMethodCallIgnored
//    samplesDirTmp.delete();
//    FileUtils.forceMkdir(samplesDirTmp);
//  }
//
//  @After
//  public void tearDown() throws IOException {
//    FileUtils.forceDelete(samplesDirTmp);
//  }
//
//  @Test
//  public void testCommandLine() throws Exception {
//    final CharSeqBuilder builder = new CharSeqBuilder();
//    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
//      @Override
//      public void process(final CharSequence arg) {
//        builder.append(arg).append('\n');
//      }
//    });
//    testEnvironment.execute(MyMap.class, new FixedMRTable("poh/neh"), new FixedMRTable("neh/poh"));
//    final SimpleRegExp regExp = SimpleRegExp.create("java -Xmx1G -Xms1G -jar .*yamr-routine-.*jar .*\\$MyMap 2");
//
//    final Seq<CharSeq> commands = testEnvironment.commands.build();
//    assertTrue(regExp.match(commands.at(commands.length() - 2)));
//  }
//
//  @Test
//  public void testJarExecutes() throws Exception {
//    final CharSeqBuilder builder = new CharSeqBuilder();
//    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
//      @Override
//      public void process(final CharSequence arg) {
//        builder.append(arg).append('\n');
//      }
//    });
//    testEnvironment.execute(MyMap.class, new FixedMRTable("poh/neh"), new FixedMRTable("neh/poh"));
//    assertEquals("Hello xyuJlo\n", builder.toString());
//  }
//
//  @Test
//  public void testIllegalInput() throws Exception {
//    final CharSeqBuilder builder = new CharSeqBuilder();
//    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
//      @Override
//      public void process(final CharSequence arg) {
//        builder.append(arg).append('\n');
//      }
//    });
//    testEnvironment.tableContents.put("poh/neh", "preved subkey medved");
//    testEnvironment.execute(MyMap.class, new FixedMRTable("poh/neh"), new FixedMRTable("neh/poh"));
//    assertEquals("2\nIllegal record\tContains less then 3 fields\tpreved subkey medved\n", builder.toString());
//  }
//
//  @Test
//  public void testException() throws Exception {
//    final CharSeqBuilder builder = new CharSeqBuilder();
//    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
//      @Override
//      public void process(final CharSequence arg) {
//        builder.append(arg).append('\n');
//      }
//    });
//    testEnvironment.execute(MyMapException.class, new FixedMRTable("poh/neh"), new FixedMRTable("neh/poh"));
//    final SimpleRegExp regExp = SimpleRegExp.create("2\n.*java.lang.RuntimeException: Tut tozhe hello");
//
//    assertTrue(regExp.match(builder));
//  }
//
//  @Test
//  public void testMap() throws Exception {
//    final CharSeqBuilder builder = new CharSeqBuilder();
//    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
//      @Override
//      public void process(final CharSequence arg) {
//        builder.append(arg).append('\n');
//      }
//    });
//    testEnvironment.execute(MyMapOutput.class, new FixedMRTable("poh/neh"), new FixedMRTable("neh/poh"));
//
//    assertEquals("preved\t1\tmedved\n", builder.toString());
//  }
//
//  @Test
//  public void testReduce1() throws Exception {
//    final CharSeqBuilder builder = new CharSeqBuilder();
//    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
//      @Override
//      public void process(final CharSequence arg) {
//        builder.append(arg).append('\n');
//      }
//    });
//    testEnvironment.setContent("poh/neh-reduce", "preved\t1\tmedved\n"
//                                                 + "preved\t2\tmedved\n"
//                                                 + "preved\t3\tmedved\n"
//    );
//    testEnvironment.execute(MyReduceCounter.class, new FixedMRTable("poh/neh-reduce"), new FixedMRTable("neh/poh"));
//
//    assertEquals("preved\tcount\t3\n", builder.toString());
//  }
//
//  @Test
//  public void testReduce2() throws Exception {
//    final CharSeqBuilder builder = new CharSeqBuilder();
//    testEnvironment.setOutputProcessor(new Processor<CharSequence>() {
//      @Override
//      public void process(final CharSequence arg) {
//        builder.append(arg).append('\n');
//      }
//    });
//    testEnvironment.setContent("poh/neh-reduce", "preved\t1\tmedved\n"
//                                                 + "preved\t2\tmedved\n"
//                                                 + "preved\t3\tmedved\n"
//                                                 + "nepreved\t1\tmedved\n");
//    testEnvironment.execute(MyReduceCounter.class, new FixedMRTable("poh/neh-reduce"), new FixedMRTable("neh/poh"));
//
//    assertEquals("preved\tcount\t3\nnepreved\tcount\t1\n", builder.toString());
//  }
//
//  private static class MyMap extends MRMap {
//    public MyMap(final String[] input, final MROutput output, final MRState state) {
//      super(input, output, state);
//    }
//
//    @Override
//    public void map(final String key, final String sub, final CharSequence value) {
//      System.out.println("Hello xyuJlo");
//    }
//  }
//
//  private static class MyMapException extends MRMap {
//    public MyMapException(final String[] input, final MROutput output, final MRState state) {
//      super(input, output, state);
//    }
//
//    @Override
//    public void map(final String key, final String sub, final CharSequence value) {
//      throw new RuntimeException("Tut tozhe hello");
//    }
//  }
//
//  private static class MyMapOutput extends MRMap {
//    public MyMapOutput(final String[] input, final MROutput output, final MRState state) {
//      super(input, output, state);
//    }
//
//    @Override
//    public void map(final String key, final String sub, final CharSequence value) {
//      output.add("preved", "1", "medved");
//    }
//  }
//
//  private static class MyReduceCounter extends MRReduce {
//    public MyReduceCounter(final String[] input, final MROutput output, final MRState state) {
//      super(input, output, state);
//    }
//
//    @Override
//    public void reduce(final String key, final Iterator<Pair<String, CharSequence>> reduce) {
//      int count = 0;
//      while (reduce.hasNext()) {
//        reduce.next();
//        count++;
//      }
//      output.add(key, "count", Integer.toString(count));
//    }
//  }

}
