package junk.minamoto;

import com.spbsu.commons.seq.CharSeqReader;

import java.io.IOException;

/**
 * Created by minamoto on 30/03/15.
 */
public class YtError {
  static String msg = "{\"attributes\": {\"tid\": 140587525129984, \"host\": \"s01h-i.aristotle.yt.yandex.net\", \"pid\": 5005, \"datetime\": \"2015-03-30T13:32:15.723274Z\"}, \"message\": \"Operation has failed to prepare\", \"code\": 1, \"inner_errors\": [{\"attributes\": {\"pid\": 5005, \"datetime\": \"2015-03-30T13:32:15.723150Z\", \"host\": \"s01h-i.aristotle.yt.yandex.net\", \"file\": \"/home/teamcity/source/Yt_PreciseGccRelWithDebInfo/yt/server/scheduler/scheduler.cpp\", \"tid\": 140587449595648, \"line\": 1073}, \"message\": \"Input table //tmp/solar/users-aggr-243a4035 is not sorted\", \"code\": 1}]}";
  static String msg1 = "DEBUG:2015-03-28 14:50:10.863 ( 0 min)  operation 17803f34-223edb90-d543ea98-8d36eef1 initializing\n" +
            "17803f34-223edb90-d543ea98-8d36eef1: running=2006       completed=0     pending=663     failed=0        aborted=0       lost=0  total=2669\n" +
            "17803f34-223edb90-d543ea98-8d36eef1: running=2661       completed=0     pending=0       failed=0        aborted=0       lost=0  total=2661\n" +
            "17803f34-223edb90-d543ea98-8d36eef1: running=2595       completed=66    pending=0       failed=0        aborted=0       lost=0  total=2661\n" +
            "17803f34-223edb90-d543ea98-8d36eef1: running=2301       completed=360   pending=0       failed=0        aborted=0       lost=0  total=2661\n" +
            "17803f34-223edb90-d543ea98-8d36eef1: running=1710       completed=951   pending=0       failed=0        aborted=0       lost=0  total=2661\n" +
            "17803f34-223edb90-d543ea98-8d36eef1: running=1081       completed=1580  pending=0       failed=0        aborted=0       lost=0  total=2661\n" +
            "17803f34-223edb90-d543ea98-8d36eef1: running=475        completed=2186  pending=0       failed=0        aborted=0       lost=0  total=2661\n" +
            "17803f34-223edb90-d543ea98-8d36eef1: running=122        completed=2539  pending=0       failed=0        aborted=0       lost=0  total=2661\n" +
            "17803f34-223edb90-d543ea98-8d36eef1: running=18 completed=2643  pending=0       failed=0        aborted=0       lost=0  total=2661";


  public static void main(String[] arg) throws IOException {
    CharSeqReader reader = new CharSeqReader(msg1);
    /*CharSeqTools.processLines(reader, new YtMREnv.SshMRYtResponseProcessor(
            new Action<CharSequence>() {
              @Override
              public void invoke(CharSequence charSequence) {
              }
            },
        new Action<CharSequence>(){
          @Override
          public void invoke(CharSequence charSequence) {
          }
        }));*/
  }

}
