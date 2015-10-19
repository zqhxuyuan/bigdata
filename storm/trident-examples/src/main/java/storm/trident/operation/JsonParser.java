package storm.trident.operation;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;

import java.util.Map;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonParser extends BaseFunction {
    private static final Logger LOG = LoggerFactory.getLogger(JsonParser.class);
    private JSONParser parser;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        
        parser = new JSONParser();
    }
    
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String json = tuple.getString(0);
        
        try {
            Object obj = parser.parse(json);
            collector.emit(new Values(obj));
        } catch (ParseException ex) {
            LOG.error("Error parsing JSON string", ex);
            LOG.debug("JSON error '{}' caused by '{}'", ex.getMessage(), json);
        }
    }
}