package com.expleague.yasm4u.domains.mr.spark;

import com.expleague.yasm4u.domains.mr.MRPath;

import java.net.URI;
import java.net.URISyntaxException;

public class S3Path extends MRPath {
  private final String bucket;
  public S3Path(Mount mount, String bucket, String path) {
    super(mount, path, false);
    this.bucket = bucket;
  }

  public static S3Path createFromURI(String uriS) {
    try {
      final URI uri = new URI(uriS);
      if ("s3".equals(uri.getScheme())) {
        String path = uri.getPath();
        if (path == null)
          path = "";
        else if (path.startsWith("/"))
          path = path.substring(1);
        return new S3Path(Mount.ROOT, uri.getHost(), path);
      }
      else throw new IllegalArgumentException("Unsupported protocol: " + uri.getScheme() + " in URI: [" + uriS + "]");
    }
    catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public String bucket() {
    return bucket;
  }

  public String path() {
    return path;
  }

  @Override
  public String toString() {
    return "s3://" + bucket + "/" + path;
  }

  @Override
  public URI toURI() {
    try {
      return new URI("s3", bucket, path, null);
    }
    catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}

