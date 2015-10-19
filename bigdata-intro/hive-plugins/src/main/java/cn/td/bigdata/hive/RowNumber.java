package cn.td.bigdata.hive;

/**
 * Created by zqhxuyuan on 15-3-10.
 */
public class RowNumber extends org.apache.hadoop.hive.ql.exec.UDF {

    private static int MAX_VALUE = 50;
    private static String comparedColumn[] = new String[MAX_VALUE];
    private static int rowNum = 1;

    public int evaluate(Object... args) {
        String columnValue[] = new String[args.length];
        for (int i = 0; i < args.length; i++)
            columnValue[i] = args[i].toString();
        if (rowNum == 1) {
            for (int i = 0; i < columnValue.length; i++)
                comparedColumn[i] = columnValue[i];
        }

        for (int i = 0; i < columnValue.length; i++) {
            if (!comparedColumn[i].equals(columnValue[i])) {
                for (int j = 0; j < columnValue.length; j++) {
                    comparedColumn[j] = columnValue[j];
                }
                rowNum = 1;
                return rowNum++;
            }
        }
        return rowNum++;
    }
}
