package junk.minamoto;

import com.expleague.commons.seq.CharSeqTools;
import com.expleague.commons.util.JSONTools;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by minamoto on 18/06/15.
 */
public class YtResponsesTests {
  final static Pattern pattern = Pattern.compile("^(.*)\\[");
  public static void main(String[] arg) throws IOException {

    CharSeqTools.processLines(new InputStreamReader(YtResponsesTests.class.getResourceAsStream("list.response")), line -> {
      try {
        CharSequence result = cutOffNonJsonGarbage(line);
        JsonParser jp = JSONTools.parseJSON(result);
        new ObjectMapper().readTree(jp);
      } catch (IOException e) {
        throw new RuntimeException(e);
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
