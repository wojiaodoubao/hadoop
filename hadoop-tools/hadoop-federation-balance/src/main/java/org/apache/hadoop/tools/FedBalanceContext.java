package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class FedBalanceContext implements Writable {

  private String srcNamespace;
  private String dstNamespace;
  private Path src;
  private Path dst;
  private Configuration conf;

  public FedBalanceContext() {}

  public FedBalanceContext(URI src, URI dst, Configuration conf) {
    this.src = new Path(src.getPath());
    this.dst = new Path(dst.getPath());
    this.srcNamespace = "hdfs://"+src.getAuthority();
    this.dstNamespace = "hdfs://"+dst.getAuthority();
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  public Path getSrc() {
    return src;
  }

  public Path getDst() {
    return dst;
  }

  public URI getSrcUri() {
    try {
      return new URI(srcNamespace);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  public URI getDstUri() {
    try {
      return new URI(dstNamespace);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    conf.write(out);
    Text.writeString(out, src.toString());
    Text.writeString(out, dst.toString());
    Text.writeString(out, srcNamespace);
    Text.writeString(out, dstNamespace);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    conf = new Configuration(false);
    conf.readFields(in);
    src = new Path(Text.readString(in));
    dst = new Path(Text.readString(in));
    srcNamespace = Text.readString(in);
    dstNamespace = Text.readString(in);
  }
}