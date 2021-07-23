package io.github.interestinglab.waterdrop.output.clickhouse;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class PartUploader
{
  private final Logger log;
  private final String hdfsUser;
  private final Path rootPath;
  private final File dataDir;
  private final List<Path> uploadedFiles;

  public PartUploader(Logger log, String hdfsUser, String rootPath, File dataDir)
  {
    this.log = log;
    this.hdfsUser = hdfsUser;
    this.rootPath = new Path(rootPath);
    this.dataDir = dataDir;
    this.uploadedFiles = new LinkedList<>();
  }

  public void upload() throws IOException, InterruptedException
  {
    Configuration hadoopConf = new Configuration();
    UserGroupInformation currentUgi = UserGroupInformation.createRemoteUser(hdfsUser);
    final String partPrefix = UUID.randomUUID().toString();
    for (File partDir : dataDir.listFiles()) {
      if (!partDir.isDirectory()) {
        continue;
      }
      if ("detached".equals(partDir.getName())) {
        continue;
      }

      currentUgi.doAs(new PrivilegedExceptionAction<Boolean>()
      {
        @Override
        public Boolean run() throws Exception
        {
          FileSystem fs = rootPath.getFileSystem(hadoopConf);
          String partName = String.format("%s__%s.zip", partPrefix, partDir.getName());
          Path tmpIndexPath = new Path(new Path(rootPath, "tmp"), partName);
          log.info("writing to " + tmpIndexPath);
          uploadedFiles.add(tmpIndexPath);
          try (FSDataOutputStream out = fs.create(tmpIndexPath)) {
            zip(log, partDir, out);
          }
          Path targetPath = new Path(rootPath, partName);
          if (!fs.rename(tmpIndexPath, targetPath)) {
            throw new IOException(String.format("failed to rename %s to %s", tmpIndexPath, targetPath));
          }
          uploadedFiles.add(targetPath);
          uploadedFiles.remove(tmpIndexPath);
          return true;
        }
      });
    }
  }

  public void clean()
  {
    Configuration hadoopConf = new Configuration();
    UserGroupInformation currentUgi = UserGroupInformation.createRemoteUser(hdfsUser);
    try {
      currentUgi.doAs(new PrivilegedExceptionAction<Boolean>()
      {
        @Override
        public Boolean run() throws Exception
        {
          FileSystem fs = rootPath.getFileSystem(hadoopConf);
          for (Path p : uploadedFiles) {
            try {
              fs.delete(p, false);
            }
            catch (IOException e) {
              log.info("failed to clean " + p, e);
            }
          }
          return true;
        }
      });
    }
    catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  /**
   * Zips the contents of the input directory to the output stream. Sub directories are skipped
   *
   * @param directory The directory whose contents should be added to the zip in the output stream.
   * @param out       The output stream to write the zip data to. Caller is responsible for closing this stream.
   *
   * @return The number of bytes (uncompressed) read from the input directory.
   *
   * @throws IOException
   */
  private long zip(Logger log, File directory, OutputStream out) throws IOException
  {
    if (!directory.isDirectory()) {
      throw new IOException(String.format("directory[%s] is not a directory", directory));
    }

    final ZipOutputStream zipOut = new ZipOutputStream(out);

    long totalSize = 0;
    for (File file : directory.listFiles()) {
      log.info(String.format(
          "Adding file[%s] with size[%,d].  Total size so far[%,d]",
          file,
          file.length(),
          totalSize
      ));
      if (file.length() >= Integer.MAX_VALUE) {
        zipOut.finish();
        throw new IOException(String.format("file[%s] too large [%,d]", file, file.length()));
      }
      zipOut.putNextEntry(new ZipEntry(file.getName()));
      totalSize += Files.asByteSource(file).copyTo(zipOut);
    }
    zipOut.closeEntry();
    // Workaround for http://hg.openjdk.java.net/jdk8/jdk8/jdk/rev/759aa847dcaf
    zipOut.flush();
    zipOut.finish();

    return totalSize;
  }
}
