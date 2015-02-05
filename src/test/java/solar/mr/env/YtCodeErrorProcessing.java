package solar.mr.env;

import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeqTools;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.concurrent.Callable;

/**
 * Created by minamoto on 04/02/15.
 */
public class YtCodeErrorProcessing {
  final static String YT_MESSAGE_500 = "{\"message\": \"Received an error while requesting http://n0013h.plato.yt.yandex.net/api/v2/read. Request headers are {\\n    \\\"Accept-Encoding\\\": \\\"gzip, identity\\\",\\n    \\\"Authorization\\\": \\\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\\\",\\n    \\\"User-Agent\\\": \\\"Python wrapper 0.5.151-0\\\",\\n    \\\"X-YT-Correlation-Id\\\": \\\"fdec52c0-45a61519-22d155f4-b985bc05\\\",\\n    \\\"X-YT-Parameters\\\": \\\"{\\\\\\\"path\\\\\\\": {\\\\\\\"$value\\\\\\\": \\\\\\\"//home/mobilesearch/yasm4u-tests/EmptinessTest-1-_test\\\\\\\", \\\\\\\"$attributes\\\\\\\": {\\\\\\\"upper_limit\\\\\\\": {\\\\\\\"row_index\\\\\\\": 10}, \\\\\\\"append\\\\\\\": \\\\\\\"false\\\\\\\"}}, \\\\\\\"output_format\\\\\\\": {\\\\\\\"$value\\\\\\\": \\\\\\\"yamr\\\\\\\", \\\\\\\"$attributes\\\\\\\": {\\\\\\\"enable_table_index\\\\\\\": \\\\\\\"false\\\\\\\", \\\\\\\"fs\\\\\\\": \\\\\\\"\\\\\\\\t\\\\\\\", \\\\\\\"lenval\\\\\\\": \\\\\\\"false\\\\\\\", \\\\\\\"has_subkey\\\\\\\": \\\\\\\"true\\\\\\\", \\\\\\\"rs\\\\\\\": \\\\\\\"\\\\\\\\n\\\\\\\"}}, \\\\\\\"ping_ancestor_transactions\\\\\\\": \\\\\\\"false\\\\\\\", \\\\\\\"transaction_id\\\\\\\": \\\\\\\"0-0-0-0\\\\\\\"}\\\"\\n}\", \"code\": 1, \"inner_errors\": [{\"attributes\": {\"pid\": 16873, \"datetime\": \"2015-02-05T09:03:06.048641Z\", \"host\": \"n0013h.plato.yt.yandex.net\", \"file\": \"/home/teamcity/source/Yt_PreciseGccRelWithDebInfo/yt/ytlib/table_client/table_reader.cpp\", \"tid\": 140202152552192, \"line\": 91}, \"message\": \"Error getting object type\", \"code\": 1, \"inner_errors\": [{\"attributes\": {\"pid\": 15235, \"datetime\": \"2015-02-05T09:03:06.048388Z\", \"host\": \"m02h-i.plato.yt.yandex.net\", \"verb\": \"Get\", \"file\": \"/home/teamcity/source/Yt_PreciseGccRelWithDebInfo/yt/core/ytree/ypath_client.cpp\", \"tid\": 140557942970112, \"line\": 198, \"resolved_path\": \"//home/mobilesearch/yasm4u-tests\"}, \"message\": \"Error resolving path //home/mobilesearch/yasm4u-tests/EmptinessTest-1-_test/@type\", \"code\": 500, \"inner_errors\": [{\"attributes\": {\"pid\": 15235, \"datetime\": \"2015-02-05T09:03:06.048347Z\", \"host\": \"m02h-i.plato.yt.yandex.net\", \"file\": \"/home/teamcity/source/Yt_PreciseGccRelWithDebInfo/yt/core/ytree/exception_helpers.cpp\", \"tid\": 140557942970112, \"line\": 46}, \"message\": \"Node //home/mobilesearch/yasm4u-tests has no child with key \\\"EmptinessTest-1-_test\\\"\", \"code\": 500, \"inner_errors\": []}]}]}]}";
  final static String YT_MESSAGE_501 = "{\"message\": \"Received an error while requesting http://plato.yt.yandex.net/api/v2/create. Request headers are {\\n    \\\"Accept-Encoding\\\": \\\"gzip, identity\\\",\\n    \\\"Authorization\\\": \\\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\\\",\\n    \\\"Content-Type\\\": \\\"application/json\\\",\\n    \\\"User-Agent\\\": \\\"Python wrapper 0.5.151-0\\\",\\n    \\\"X-YT-Correlation-Id\\\": \\\"9cc2d92-ad3a69a2-d2c8b015-11ffb007\\\"\\n}\", \"code\": 1, \"inner_errors\": [{\"attributes\": {\"pid\": 11517, \"datetime\": \"2015-02-05T09:05:32.152112Z\", \"host\": \"c02h.plato.yt.yandex.net\", \"file\": \"/home/teamcity/source/Yt_PreciseGccRelWithDebInfo/yt/ytlib/driver/command.h\", \"tid\": 139786660677376, \"line\": 198}, \"message\": \"Error parsing command arguments\", \"code\": 1, \"inner_errors\": [{\"attributes\": {\"pid\": 11517, \"datetime\": \"2015-02-05T09:05:32.152056Z\", \"host\": \"c02h.plato.yt.yandex.net\", \"file\": \"/home/teamcity/source/Yt_PreciseGccRelWithDebInfo/yt/./core/ytree/yson_serializable-inl.h\", \"tid\": 139786660677376, \"line\": 42}, \"message\": \"Error reading parameter /type\", \"code\": 1, \"inner_errors\": [{\"attributes\": {}, \"message\": \"Error parsing EObjectType value \\\"//home/mobilesearch/yasm4u-tests\\\"\", \"code\": 501, \"inner_errors\": []}]}]}]}";
  /* TODO: collect real case of 200 error */
  final static String YT_MESSAGE_200 = "{\"message\": \"Received an error while requesting http://plato.yt.yandex.net/api/v2/create. Request headers are {\\n    \\\"Accept-Encoding\\\": \\\"gzip, identity\\\",\\n    \\\"Authorization\\\": \\\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\\\",\\n    \\\"Content-Type\\\": \\\"application/json\\\",\\n    \\\"User-Agent\\\": \\\"Python wrapper 0.5.151-0\\\",\\n    \\\"X-YT-Correlation-Id\\\": \\\"9cc2d92-ad3a69a2-d2c8b015-11ffb007\\\"\\n}\", \"code\": 1, \"inner_errors\": [{\"attributes\": {\"pid\": 11517, \"datetime\": \"2015-02-05T09:05:32.152112Z\", \"host\": \"c02h.plato.yt.yandex.net\", \"file\": \"/home/teamcity/source/Yt_PreciseGccRelWithDebInfo/yt/ytlib/driver/command.h\", \"tid\": 139786660677376, \"line\": 198}, \"message\": \"Error parsing command arguments\", \"code\": 1, \"inner_errors\": [{\"attributes\": {\"pid\": 11517, \"datetime\": \"2015-02-05T09:05:32.152056Z\", \"host\": \"c02h.plato.yt.yandex.net\", \"file\": \"/home/teamcity/source/Yt_PreciseGccRelWithDebInfo/yt/./core/ytree/yson_serializable-inl.h\", \"tid\": 139786660677376, \"line\": 42}, \"message\": \"Error reading parameter /type\", \"code\": 200, \"inner_errors\": [{\"attributes\": {}, \"message\": \"Error parsing EObjectType value \\\"//home/mobilesearch/yasm4u-tests\\\"\", \"code\": 200, \"inner_errors\": []}]}]}]}";

  final static String YT_MR_FAILED = "2015-02-05 07:44:00.048 ( 0 min)\toperation 6146627-b1428e4e-ece94924-7a52acf8 failed\n"+
      "Operation 6146627-b1428e4e-ece94924-7a52acf8 failed. Result: Operation has failed to prepare    \n"+
      "    origin          s02h-i.plato.yt.yandex.net in 2015-02-05T04:44:00.017962Z (pid 31178, tid 7f9540876700)\n"+
      "Input table //home/mobilesearch/yasm4u-tests/MultiReduceTest-in-1-_test is not sorted    \n"+
      "    origin          s02h-i.plato.yt.yandex.net in 2015-02-05T04:43:59.993120Z (pid 31178, tid 7f953c06d700)    \n"+
      "    location        /home/teamcity/source/Yt_PreciseGccRelWithDebInfo/yt/server/scheduler/scheduler.cpp:1063";

  final static String YT_MR_WITH_HINT = "2015-02-04 16:20:53.553 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad initializing\n"+
      "2015-02-04 16:20:56.752 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=3239      completed=0     pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:05.480 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=3238      completed=1     pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:09.385 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=3229      completed=10    pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:12.641 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=3159      completed=80    pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:16.527 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=3068      completed=171   pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:18.758 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=2937      completed=302   pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:21.221 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=2518      completed=721   pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:25.164 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=1718      completed=1521  pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:28.110 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=1150      completed=2089  pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:31.334 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=754       completed=2485  pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:34.877 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=534       completed=2705  pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:38.763 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=237       completed=3002  pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:43.027 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=128       completed=3111  pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:47.712 ( 0 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=17        completed=3222  pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:21:57.893 ( 1 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=8 completed=3231  pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:22:02.978 ( 1 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=7 completed=3232  pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:22:33.527 ( 1 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad: running=1 completed=3238  pending=0       failed=0        aborted=0       lost=0  total=3239\n"+
      "2015-02-04 16:22:38.587 ( 1 min)        operation ab49c8a-8fee3a48-2cfefd1b-bb9729ad completed\n"+
      "2015-02-04 16:22:38,803 INFO    Chunks of output table //tmp/minamoto/home/mobilesearch/logprocessing_1/ghost_events/20141124_0-0-af859e5d are too small. This may cause suboptimal system performance. If this table is not temporary then consider running the following command:\n"+
      "yt merge --mode unordered --proxy aristotle.yt.yandex.net --src //tmp/minamoto/home/mobilesearch/logprocessing_1/ghost_events/20141124_0-0-af859e5d --dst //tmp/minamoto/home/mobilesearch/logprocessing_1/ghost_events/20141124_0-0-af859e5d --spec '{combine_chunks=true;data_size_per_job=2500162086}'\n"+
      "2015-02-04 16:22:38,974 INFO    Chunks of output table //home/mobilesearch/logprocessing_1/ghost_slices/20141124_0 are too small. This may cause suboptimal system performance. If this table is not temporary then consider running the following command:\n"+
      "yt merge --mode unordered --proxy aristotle.yt.yandex.net --src //home/mobilesearch/logprocessing_1/ghost_slices/20141124_0 --dst //home/mobilesearch/logprocessing_1/ghost_slices/20141124_0 --spec '{combine_chunks=true;data_size_per_job=524809800}'\n";
  final String YT_MR_SUCCESS = "2015-02-04 16:49:07.407 ( 0 min)        operation ac2e2d2-f373a0bc-86f5e18a-98de2170 initializing\n" +
      "2015-02-04 16:49:10.637 ( 0 min)        operation ac2e2d2-f373a0bc-86f5e18a-98de2170: running=557       completed=0     pending=0       failed=0        aborted=0       lost=0  total=557\n" +
      "2015-02-04 16:49:13.918 ( 0 min)        operation ac2e2d2-f373a0bc-86f5e18a-98de2170: running=555       completed=2     pending=0       failed=0        aborted=0       lost=0  total=557\n" +
      "2015-02-04 16:49:16.135 ( 0 min)        operation ac2e2d2-f373a0bc-86f5e18a-98de2170: running=528       completed=29    pending=0       failed=0        aborted=0       lost=0  total=557\n" +
      "2015-02-04 16:49:19.426 ( 0 min)        operation ac2e2d2-f373a0bc-86f5e18a-98de2170: running=416       completed=141   pending=0       failed=0        aborted=0       lost=0  total=557\n" +
      "2015-02-04 16:49:23.407 ( 0 min)        operation ac2e2d2-f373a0bc-86f5e18a-98de2170: running=80        completed=477   pending=0       failed=0        aborted=0       lost=0  total=557\n" +
      "2015-02-04 16:49:24.931 ( 0 min)        operation ac2e2d2-f373a0bc-86f5e18a-98de2170 completed\n";

  private static class StrictlyIgnoringProcessor implements Processor<CharSequence> {
    @Override
    public void process(CharSequence arg) {
      throw new IllegalStateException("Shouldn't be called");
    }
  }

  private static class ParsingOnlyProcessor extends YtMREnv.YtResponseProcessor {

    public ParsingOnlyProcessor(Processor<CharSequence> processor) {
      super(processor);
    }

    @Override
    public void reportError(CharSequence msg) {

    }

    @Override
    public void warn(String msg) {

    }
  }

  private static class ParsingOnlyMRProcessor extends YtMREnv.YtMRResponseProcessor{

    public ParsingOnlyMRProcessor(Processor<CharSequence> processor) {
      super(processor);
    }

    @Override
    public void reportError(CharSequence msg) {

    }

    @Override
    public void warn(String msg) {

    }
  }

  public static Processor<CharSequence> STRICTLY_IGNORING_PROCESSOR = new StrictlyIgnoringProcessor();
  public static Processor<CharSequence> IGNORING_PROCESSOR = new Processor<CharSequence>() {
    @Override
    public void process(CharSequence arg) {
      /* do nothing */
    }
  };
  @Test
  public void testWarning500() throws IOException {
    CharSeqTools.processLines(new StringReader(YT_MESSAGE_500), new ParsingOnlyProcessor(STRICTLY_IGNORING_PROCESSOR));
  }

  @Test
  public void testWarning501() throws IOException{
    CharSeqTools.processLines(new StringReader(YT_MESSAGE_501), new ParsingOnlyProcessor(STRICTLY_IGNORING_PROCESSOR));
  }

  @Test(expected = RuntimeException.class)
  public void testWarning200() throws IOException {
    CharSeqTools.processLines(new StringReader(YT_MESSAGE_200), new ParsingOnlyProcessor(IGNORING_PROCESSOR));
  }

  @Test
  public void testMRSuccess() throws IOException {
    CharSeqTools.processLines(new StringReader(YT_MR_SUCCESS), new ParsingOnlyMRProcessor(STRICTLY_IGNORING_PROCESSOR));
  }

  @Test(expected = RuntimeException.class)
  public void testMRFailure() throws IOException {
    CharSeqTools.processLines(new StringReader(YT_MR_FAILED), new ParsingOnlyMRProcessor(STRICTLY_IGNORING_PROCESSOR));
  }

  @Test
  public void testMRSuccessWithHint() throws IOException {
    CharSeqTools.processLines(new StringReader(YT_MR_WITH_HINT), new ParsingOnlyMRProcessor(IGNORING_PROCESSOR));
  }
}
