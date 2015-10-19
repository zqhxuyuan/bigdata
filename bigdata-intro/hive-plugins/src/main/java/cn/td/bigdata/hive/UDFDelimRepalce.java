package cn.td.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFDelimRepalce extends UDF
{
  public UDFDelimRepalce()
  {
  }

  public String evaluate(String cloumnvalue)
  {
    if (cloumnvalue == null) {
      return null;
    }

    String result = null;
    byte[] b1 = { 1 };
    String str = new String(b1);

    if (cloumnvalue.contains(str)) {
      result = cloumnvalue.replace(str, "");
    }

    if (cloumnvalue.contains("\"")) {
      if (result == null)
        result = cloumnvalue.replace("\"", "");
      else {
        result = result.replace("\"", "");
      }
    }

    if (result == null) {
      return cloumnvalue;
    }

    return result;
  }
}

/* Location:
 * Qualified Name:     com.zqh.hive.UDFDelimRepalce
 * Java Class Version: 6 (50.0)
 * JD-Core Version:    0.6.1-SNAPSHOT
 */