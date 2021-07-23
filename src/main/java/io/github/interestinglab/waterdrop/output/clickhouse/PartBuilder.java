package io.github.interestinglab.waterdrop.output.clickhouse;

import org.apache.spark.sql.Row;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartBuilder
{
  private final ExecutorService exec;
  private final Logger log;
  private final String binaryDir;
  private final File workDir;
  private final String pipeFile;
  private final CsvType[] types;
  private final Object[] defaultValues;
  private final String buildSql;

  public PartBuilder(
      Logger log,
      String binaryDir,
      File workDir,
      String[] fields,
      Map<String, String> tableSchema,
      Map<String, Object> defaultValues,
      String buildSql
  )
  {
    this.exec = Executors.newFixedThreadPool(2);
    this.log = log;
    this.binaryDir = binaryDir;
    this.workDir = workDir;
    this.pipeFile = workDir + "/pipe_file";
    this.types = new CsvType[fields.length];
    this.defaultValues = new Object[fields.length];
    for (int i = 0; i < types.length; i++) {
      String type = tableSchema.get(fields[i]);
      types[i] = fixSpecialType(type);
      Object defaultValue = defaultValues.get(fields[i]);
      this.defaultValues[i] = defaultValue == null ? null : String.valueOf(defaultValue);
    }
    this.buildSql = buildSql;
  }

  private CsvType fixSpecialType(String inType)
  {
    if ("String".equals(inType) || "Nullable(String)".equals(inType)) {
      return CsvType.STRING;
    }
    if ("Decimal".equals(inType) || "Nullable(Decimal)".equals(inType)) {
      return CsvType.DECIMAL;
    }
    if (inType.equals("Array(String)")) {
      return CsvType.ARRAY_STRING;
    }
    if (inType.startsWith("Array")) {
      return CsvType.ARRAY_OTHER;
    }
    return CsvType.OTHER;
  }

  public void build(Iterator<Row> rows) throws Exception
  {
    try {
      buildInner(rows);
    }
    finally {
      exec.shutdown();
    }
  }

  private void buildInner(Iterator<Row> rows) throws Exception
  {
    createPipeFile(pipeFile);
    createBuildSqlFile();
    CountDownLatch starting = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(1);
    AtomicBoolean buildFailed = new AtomicBoolean(false);
    AtomicBoolean writeFailed = new AtomicBoolean(false);
    exec.submit(() -> {
      try {
        String[] cmd = new String[]{"/bin/bash", "-c", "cd " + workDir + ";sh " + binaryDir + "/build.sh"};
        starting.countDown();
        Process process = Runtime.getRuntime().exec(cmd);

        BufferedReader buffer = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String str;
        while ((str = (buffer.readLine())) != null) {
          log.info(str);
        }
        buffer.close();
        buffer = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        while ((str = (buffer.readLine())) != null) {
          log.info(str);
        }
        buffer.close();
        int exitValue = process.waitFor();
        if (0 != exitValue) {
          throw new Exception("build failed");
        }
        log.info("done build.");
      }
      catch (Exception e) {
        buildFailed.set(true);
      }
      finally {
        done.countDown();
      }
    });
    starting.await();
    // maybe failed to start building script
    if (buildFailed.get()) {
      log.info("failed to start building.");
      throw new IOException("failed to start building");
    }

    exec.submit(() -> {
      try {
        writeDataToPipeFile(rows);
      }
      catch (Exception e) {
        writeFailed.set(true);
      }
    });
    done.await();
    if (buildFailed.get()) {
      log.info("failed to build");
      throw new Exception("failed to build");
    }
    if (writeFailed.get()) {
      log.info("write data failed");
      throw new Exception("write data failed");
    }
  }

  private void createBuildSqlFile() throws IOException
  {
    FileWriter writer = new FileWriter(new File(workDir, "build.sql"));
    writer.write(buildSql);
    writer.close();
  }

  private void writeDataToPipeFile(Iterator<Row> iterator) throws IOException
  {
    new CsvWriter(pipeFile, types, defaultValues, log).writeRows(iterator);
  }

  private static void createPipeFile(String name) throws IOException, InterruptedException
  {
    String[] cmd = new String[]{"/bin/bash", "-c", "mkfifo " + name};
    Process process = Runtime.getRuntime().exec(cmd);
    int exitValue = process.waitFor();
    File pipe = new File(name);
    if (exitValue != 0 && !pipe.exists()) {
      throw new IOException("Errors while creating the pipe");
    }
  }
}
