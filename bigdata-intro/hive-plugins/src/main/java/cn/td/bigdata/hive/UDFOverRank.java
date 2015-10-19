package cn.td.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

@UDFType(deterministic=false)
public class UDFOverRank extends UDF
{
  private static int MAX_VALUE = 50;

  private static String[] comparedColumn = new String[MAX_VALUE];
  private static String[] ComparedColumnReal = new String[MAX_VALUE];
  private static int rowNum = 1;
  private static int realNum = 1;

  public UDFOverRank() {
  }
  public int evaluate(Object[] args) {
    try { return rank(args);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 1;
  }

  public int rank(Object[] args)
  {
    int m = 0;
    for (int i = 0; i < args.length; i++) {
      if (("@".equalsIgnoreCase(args[i].toString())) || ("#".equalsIgnoreCase(args[i].toString())) || 
        ("@@".equalsIgnoreCase(args[i].toString())) || ("##".equalsIgnoreCase(args[i].toString())) || 
        ("orderby".equalsIgnoreCase(args[i].toString())))
      {
        m = i;
      }
    }
    String[] columnValue = new String[m];
    String[] columnValueReal = new String[args.length - m - 1];

    for (int i = 0; i < m; i++) {
      columnValue[i] = args[i].toString();
    }
    for (int i = 0; i < args.length - m - 1; i++) {
      columnValueReal[i] = args[(m + i + 1)].toString();
    }

    if (rowNum == 1) {
      for (int i = 0; i < columnValue.length; i++) {
        comparedColumn[i] = columnValue[i];
      }
      for (int i = 0; i < columnValueReal.length; i++) {
        ComparedColumnReal[i] = columnValueReal[i];
      }
    }
    for (int i = 0; i < columnValue.length; i++)
    {
      if (!comparedColumn[i].equals(columnValue[i]))
      {
        for (int j = 0; j < columnValue.length; j++)
          comparedColumn[j] = columnValue[j];
        rowNum = 1;

        realNum = 1;
        for (int k = 0; k < columnValueReal.length; k++)
          ComparedColumnReal[k] = columnValueReal[k];
        return rowNum++;
      }

    }

    for (int i = 0; i < columnValueReal.length; i++)
    {
      if (!ComparedColumnReal[i].equals(columnValueReal[i]))
      {
        for (int j = 0; j < columnValueReal.length; j++)
          ComparedColumnReal[j] = columnValueReal[j];
        realNum = rowNum;
      }
    }

    rowNum += 1;

    return realNum;
  }
}

/* Location:
 * Qualified Name:     com.zqh.hive.UDFOverRank
 * Java Class Version: 6 (50.0)
 * JD-Core Version:    0.6.1-SNAPSHOT
 */