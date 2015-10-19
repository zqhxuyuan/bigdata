package storm.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.SpikeDetectionConstants.Conf;
import storm.applications.util.config.Configuration;
import storm.applications.util.stream.StreamValues;

/**
 *
 * @author mayconbordin
 */
public class SensorParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(SensorParser.class);
    
    private static final DateTimeFormatter formatterMillis = new DateTimeFormatterBuilder()
            .appendYear(4, 4).appendLiteral("-").appendMonthOfYear(2).appendLiteral("-")
            .appendDayOfMonth(2).appendLiteral(" ").appendHourOfDay(2).appendLiteral(":")
            .appendMinuteOfHour(2).appendLiteral(":").appendSecondOfMinute(2)
            .appendLiteral(".").appendFractionOfSecond(3, 6).toFormatter();
    
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 传感器数据
     * DATE     TIME            EP   MT TEMP    HUM     LIGHT VOLT
     2004-02-28 22:31:22.535618 2587 53 17.4208 44.7798 0.46 2.65143
     2004-02-28 22:31:23.050413 2587 49 17.7834 42.4509 0.46 2.65143
     2004-02-28 22:31:23.132193 2587 21 19.7042 38.8039 35.88 2.67532
     2004-02-28 22:31:23.323921 2587 42 17.9304 42.8187 3.68 2.66332
     2004-02-28 22:31:23.567956 2587 14 19.4984 39.8574 97.52 2.71196
     2004-02-28 22:31:48.019675 2588 49 17.7736 42.5178 0.46 2.65143
     2004-02-28 22:31:48.062533 2588 24 19.1162 40.0268 2.3 2.69964
     2004-02-28 22:31:48.107425 2588 54 17.1562 45.3743  2.65143
     2004-02-28 22:31:48.152519 2588 36 19.2632 42.0489 75.44 2.68742
     2004-02-28 22:31:48.189721 2588 32 18.9986 40.568 114.08 2.67532
     2004-02-28 22:31:48.410825 2588 28 18.5674 42.183  2.80151
     2004-02-28 22:31:48.454938 2588 33 19.616 39.0082 97.52 2.68742
     2004-02-28 22:31:48.472245 2588 18 19.9296 38.3946 172.96 2.51661
     2004-02-28 22:31:48.53521 2588 3 19.959 38.497 48.76 2.68742
     */
    private static final int DATE_FIELD   = 0;  //日期
    private static final int TIME_FIELD   = 1;  //时间
    private static final int EPOCH_FIELD  = 2;  //时期,纪元,表示某个阶段
    private static final int MOTEID_FIELD = 3;  //
    private static final int TEMP_FIELD   = 4;  //温度
    private static final int HUMID_FIELD  = 5;  //
    private static final int LIGHT_FIELD  = 6;  //亮度,发光
    private static final int VOLT_FIELD   = 7;  //伏特
    
    private static final ImmutableMap<String, Integer> fieldList = ImmutableMap.<String, Integer>builder()
            .put("temp", TEMP_FIELD)
            .put("humid", HUMID_FIELD)
            .put("light", LIGHT_FIELD)
            .put("volt", VOLT_FIELD)
            .build();
    
    private String valueField;
    private int valueFieldKey;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        
        valueField = config.getString(Conf.PARSER_VALUE_FIELD);
        valueFieldKey = fieldList.get(valueField);
    }

    @Override
    public List<StreamValues> parse(String input) {
        String[] fields = input.split("\\s+");
        
        if (fields.length != 8)
            return null;
        
        String dateStr = String.format("%s %s", fields[DATE_FIELD], fields[TIME_FIELD]);
        DateTime date = null;
        
        try {
            date = formatterMillis.parseDateTime(dateStr);
        } catch (IllegalArgumentException ex) {
            try {
                date = formatter.parseDateTime(dateStr);
            } catch (IllegalArgumentException ex2) {
                LOG.warn("Error parsing record date/time field, input record: " + input, ex2);
                return null;
            }
        }
        
        try {
            StreamValues values = new StreamValues();
            values.add(fields[MOTEID_FIELD]);
            values.add(date.toDate());
            values.add(Double.parseDouble(fields[valueFieldKey]));
            
            int msgId = String.format("%s:%s", fields[MOTEID_FIELD], date.toString()).hashCode();
            values.setMessageId(msgId);
            
            return ImmutableList.of(values);
        } catch (NumberFormatException ex) {
            LOG.warn("Error parsing record numeric field, input record: " + input, ex);
        }
        
        return null;
    }
    
}
