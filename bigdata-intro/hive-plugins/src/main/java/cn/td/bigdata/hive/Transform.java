package cn.td.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Transform extends UDF
{
  public Transform()
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

  public static void main(String[] a)
  {
    Transform o = new Transform();

    byte[] b1 = { 1 };
    String str = new String(b1);
    System.out.println(str);
    String sss = "Nokia\001 \001 \001 RM-356 \001 \001/UCWEB8.9.0.\"253\"/50/999";
    System.out.println(sss);
    System.out.println(o.evaluate(sss));
    System.out.println(o.evaluate(sss).replaceAll(" ", ""));
  }
}

/* Location:
 * Qualified Name:     com.zqh.hive.Transform
 * Java Class Version: 6 (50.0)
 * JD-Core Version:    0.6.1-SNAPSHOT
 */