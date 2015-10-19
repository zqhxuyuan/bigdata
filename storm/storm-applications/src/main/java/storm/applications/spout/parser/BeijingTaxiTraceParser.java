package storm.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.util.stream.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BeijingTaxiTraceParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(BeijingTaxiTraceParser.class);
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

    /**
     * 33556这个司机行驶的轨迹路线图:
     * 1. 15:07 ~ 18:07 一直停在某个位置[40.0423,116.61]
     * 2. 20:07 位置:[40.0427,116.606], 速度为18.41km/h, 方向是291
     *
     4186585,33556,2009-01-06T05:15:07,40.0423,116.61,0,0
     4186586,33556,2009-01-06T05:16:07,40.0422,116.61,0,0
     4186587,33556,2009-01-06T05:17:07,40.0422,116.61,0,0
     4186588,33556,2009-01-06T05:18:07,40.0422,116.61,0,0
     4186589,33556,2009-01-06T05:19:07,40.0426,116.609,0,0
     4186590,33556,2009-01-06T05:20:07,40.0427,116.606,18.41,291
     4186591,33556,2009-01-06T05:21:07,40.0442,116.602,3.44488,297
     4186592,33556,2009-01-06T05:22:07,40.0438,116.602,1.14084,159
     4186593,33556,2009-01-06T05:23:15,40.0437,116.602,0,0
     4186594,33556,2009-01-06T05:24:15,40.0438,116.602,0,324
     */
    private static final int ID_FIELD    = 0;   //唯一编号
    private static final int NID_FIELD   = 1;   //
    private static final int DATE_FIELD  = 2;
    private static final int LAT_FIELD   = 3;   //纬度40.0427
    private static final int LON_FIELD   = 4;   //经度116.606
    private static final int SPEED_FIELD = 5;   //速度
    private static final int DIR_FIELD   = 6;   //方向
    
    @Override
    public List<StreamValues> parse(String input) {
        String[] fields = input.split(",");
        
        if (fields.length != 7)
            return null;
        
        try {
            String carId  = fields[ID_FIELD];
            DateTime date = formatter.parseDateTime(fields[DATE_FIELD]);
            boolean occ   = true;
            double lat    = Double.parseDouble(fields[LAT_FIELD]);
            double lon    = Double.parseDouble(fields[LON_FIELD]);
            int speed     = ((Double)Double.parseDouble(fields[SPEED_FIELD])).intValue();
            int bearing   = Integer.parseInt(fields[DIR_FIELD]);
            
            int msgId = String.format("%s:%s", carId, date.toString()).hashCode();
            
            StreamValues values = new StreamValues(carId, date, occ, speed, bearing, lat, lon);
            values.setMessageId(msgId);
            
            return ImmutableList.of(values);
        } catch (NumberFormatException ex) {
            LOG.warn("Error parsing numeric value", ex);
        } catch (IllegalArgumentException ex) {
            LOG.warn("Error parsing date/time value", ex);
        }
        
        return null;
    }
    
}
