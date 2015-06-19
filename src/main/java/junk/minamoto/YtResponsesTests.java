package junk.minamoto;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.commons.func.Processor;
import com.spbsu.commons.seq.CharSeqTools;
import com.spbsu.commons.util.JSONTools;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by minamoto on 18/06/15.
 */
public class YtResponsesTests {
  final static Pattern pattern = Pattern.compile("^(.*)\\[");
  public static void main(String[] arg) throws IOException {

    CharSeqTools.processLines(new InputStreamReader(YtResponsesTests.class.getResourceAsStream("list.response")), new Processor<CharSequence>() {
      @Override
      public void process(CharSequence arg) {
        try {
          CharSequence result = cutOffNonJsonGarbage(arg);
          JsonParser jp = JSONTools.parseJSON(result);
          new ObjectMapper().readTree(jp);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

    });
  }

  private static CharSequence cutOffNonJsonGarbage(CharSequence arg) {
    Matcher matcher = pattern.matcher(arg);
    CharSequence result;
    if (matcher.find(0))
      result = arg.subSequence(matcher.group(0).length() - 1, arg.length());
    else
      result = arg;
    return result;
  }
}
