package cn.td.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

@UDFType(deterministic=false)
public class UDFRowNumber extends UDF
{
  private static int MAX_VALUE = 50;

  private static String[] comparedColumn = new String[MAX_VALUE];

  private static int rowNum = 1;

  public UDFRowNumber() {
  }
  public int evaluate(Object[] args) {
    String[] columnValue = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i] == null)
        columnValue[i] = null;
      else {
        columnValue[i] = args[i].toString();
      }
    }
    if (rowNum == 1) {
      for (int i = 0; i < columnValue.length; i++) {
        comparedColumn[i] = columnValue[i];
      }
    }
    for (int i = 0; i < columnValue.length; i++)
    {
      if (comparedColumn[i] == null)
      {
        if (columnValue[i] != null)
        {
          for (int j = 0; j < columnValue.length; j++)
            comparedColumn[j] = columnValue[j];
          rowNum = 1;
          return rowNum++;
        }

      }
      else if (!comparedColumn[i].equals(columnValue[i]))
      {
        for (int j = 0; j < columnValue.length; j++)
          comparedColumn[j] = columnValue[j];
        rowNum = 1;
        return rowNum++;
      }

    }

    return rowNum++;
  }
}

/* Location:
 * Qualified Name:     com.zqh.hive.UDFRowNumber
 * Java Class Version: 6 (50.0)
 * JD-Core Version:    0.6.1-SNAPSHOT
 */