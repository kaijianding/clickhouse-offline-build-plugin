package io.github.interestinglab.waterdrop.output.clickhouse;

import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import ru.yandex.clickhouse.util.ClickHouseArrayUtil;
import ru.yandex.clickhouse.util.ClickHouseValueFormatter;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.TimeZone;

public class CsvWriter
{
  private static final char SEPARATOR = ',';
  private static final char QUOTE_CHAR = '"';
  private static final char ESCAPE_CHAR = '"';
  private static final char LINE_END = '\n';
  private final BufferedWriter writer;
  private final CsvType[] types;
  private final Object[] defaultValues;
  private final Logger log;
  private long counter;

  public CsvWriter(String pipeFile, CsvType[] types, Object[] defaultValues, Logger log) throws FileNotFoundException
  {
    this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(pipeFile)));
    this.types = types;
    this.defaultValues = defaultValues;
    this.log = log;
  }

  public void writeRows(Iterator<Row> rows) throws IOException
  {
    try {
      while (rows.hasNext()) {
        writeRow(rows.next());
        counter++;
      }
    }
    finally {
      log.info(counter + " rows built");
      this.writer.close();
    }
  }

  private void writeRow(Row row) throws IOException
  {
    StringBuilder sb = new StringBuilder(256);
    boolean first = true;
    for (int i = 0; i < row.size(); i++) {
      if (!first) {
        sb.append(SEPARATOR);
      }
      first = false;
      writeColumn(sb, row, i, types[i]);
    }
    sb.append(LINE_END);
    this.writer.write(sb.toString());
  }

  private void writeColumn(StringBuilder sb, Row row, int fieldIndex, CsvType type)
  {
    if (row.isNullAt(fieldIndex)) {
      if (defaultValues[fieldIndex] == null) {
        return;
      }
      sb.append(defaultValues[fieldIndex]);
      return;
    }
    switch (type) {
      case STRING:
        writeCSVString(sb, row.getAs(fieldIndex));
        break;
      case DECIMAL:
        sb.append(ClickHouseValueFormatter.formatBigDecimal(row.getAs(fieldIndex)));
        break;
      case ARRAY_STRING:
        writeCSVString(sb, ClickHouseArrayUtil.toString(
            row.<Collection<?>>getList(fieldIndex),
            TimeZone.getDefault(),
            TimeZone.getDefault()
        ));
        break;
      case ARRAY_OTHER:
        sb.append(QUOTE_CHAR);
        sb.append(ClickHouseArrayUtil.toString(
            row.<Collection<?>>getList(fieldIndex),
            TimeZone.getDefault(),
            TimeZone.getDefault()
        ));
        sb.append(QUOTE_CHAR);
        break;
      default:
        sb.append(row.<Object>getAs(fieldIndex));
    }
  }

  private void writeCSVString(StringBuilder sb, String input)
  {
    sb.append(QUOTE_CHAR);
    sb.append(this.stringContainsSpecialCharacters(input) ? this.processLine(input) : input);
    sb.append(QUOTE_CHAR);
  }

  private boolean stringContainsSpecialCharacters(String input)
  {
    return input.indexOf(QUOTE_CHAR) != -1 || input.indexOf(ESCAPE_CHAR) != -1;
  }

  private StringBuilder processLine(String input)
  {
    StringBuilder sb = new StringBuilder(16);

    for (int j = 0; j < input.length(); ++j) {
      char nextChar = input.charAt(j);
      if (nextChar == QUOTE_CHAR) {
        sb.append(ESCAPE_CHAR).append(nextChar);
      } else {
        sb.append(nextChar);
      }
    }

    return sb;
  }
}
