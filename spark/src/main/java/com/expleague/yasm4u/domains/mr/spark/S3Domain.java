package com.expleague.yasm4u.domains.mr.spark;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.expleague.commons.io.StreamTools;
import com.expleague.ml.data.tools.DataTools;
import com.expleague.yasm4u.Domain;
import com.expleague.yasm4u.Joba;
import com.expleague.yasm4u.Ref;
import com.expleague.yasm4u.Routine;
import com.expleague.yasm4u.domains.mr.ops.MRNamedRow;
import com.expleague.yasm4u.domains.mr.ops.MRRecord;
import com.expleague.yasm4u.impl.MainThreadJES;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.function.Consumer;

public class S3Domain implements Domain {
  private static Logger LOG = Logger.getLogger(S3Domain.class);
  private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

  @Override
  public void publishExecutables(List<Joba> jobs, List<Routine> routines) {}

  @Override
  public void publishReferenceParsers(Ref.Parser parser, Controller controller) {
    parser.registerProtocol("s3", from -> S3Path.createFromURI("s3:" + from));
  }

  public int read(S3Path shard, Consumer<MRRecord> seq) {
    try {
      int[] counter = new int[]{0};
      s3Client.listObjectsV2(shard.bucket(), shard.path()).getObjectSummaries().stream()
          .filter(summary -> shard.path().equals(summary.getKey()) || summary.getKey().charAt(shard.path().length()) == '/')
          .forEach(summary -> {
            final String key = summary.getKey();
            final int extensionIdx = key.lastIndexOf('.');
            if (extensionIdx < 0)
              return;
            final S3Object s3object = s3Client.getObject(shard.bucket(), key);
            switch(key.substring(extensionIdx)) {
              case ".csv":
                try (InputStreamReader reader = new InputStreamReader(s3object.getObjectContent(), StreamTools.UTF)) {
                  DataTools.readCSVWithHeader(reader, row -> seq.accept(new MRNamedRow(shard, row.at(0).toString(), row.at(1).toString(), row)));
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              default:
                // skip
            };
          });
      return counter[0];
    } catch (AmazonServiceException ase) {
      LOG.warn("Caught an AmazonServiceException, which" +
          " means your request made it " +
          "to Amazon S3, but was rejected with an error response" +
          " for some reason.");
      LOG.warn("Error Message:    " + ase.getMessage());
      LOG.warn("HTTP Status Code: " + ase.getStatusCode());
      LOG.warn("AWS Error Code:   " + ase.getErrorCode());
      LOG.warn("Error Type:       " + ase.getErrorType());
      LOG.warn("Request ID:       " + ase.getRequestId());
      throw new RuntimeException(ase);
    } catch (AmazonClientException ace) {
      LOG.warn("Caught an AmazonClientException, which means"+
          " the client encountered " +
          "an internal error while trying to " +
          "communicate with S3, " +
          "such as not being able to access the network.");
      LOG.warn("Error Message: " + ace.getMessage());
      throw new RuntimeException(ace);
    }
  }

  public static void main(String[] args) {
    MainThreadJES jes = new MainThreadJES(new S3Domain());
    S3Path s3path = jes.parse("s3://joom.emr.fs/home/solar/day_cohort_7first_days");
    jes.domain(S3Domain.class).read(s3path, System.out::println);
  }
}
