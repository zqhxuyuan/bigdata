package cn.td.bigdata.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.Text;

@UDFType(deterministic=false)
public class UDFEmpty extends UDF
{
  public UDFEmpty()
  {
  }

  public Text evaluate(Text cloumnValue, Text returnValue)
  {
    try
    {
      if (cloumnValue == null) {
        return returnValue;
      }

      if (StringUtils.isEmpty(cloumnValue.toString())) {
        return returnValue;
      }
      if (StringUtils.isEmpty(cloumnValue.toString().trim())) {
        return returnValue;
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      return returnValue;
    }

    return cloumnValue;
  }

  public static void main(String[] a)
  {
    UDFEmpty o = new UDFEmpty();

    System.out.println(o.evaluate(null, new Text("s")));
    System.out.println(o.evaluate(new Text(""), new Text("s")));
    System.out.println(o.evaluate(new Text(" "), new Text("s")));
    System.out.println(o.evaluate(new Text("sss"), new Text("s")));
  }
}

/* Location:
 * Qualified Name:     com.zqh.hive.UDFEmpty
 * Java Class Version: 6 (50.0)
 * JD-Core Version:    0.6.1-SNAPSHOT
 */