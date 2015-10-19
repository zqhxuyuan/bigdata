package siddhi.debs;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

@SiddhiExtension(namespace = "debs", function = "getSequenceCount")
public class SequenceCountCalculatorExtension extends FunctionExecutor
{
    Logger log = Logger.getLogger(TwoDimDistanceCalculatorExtension.class);

    @Override
    public Type getReturnType() {
        return Attribute.Type.INT;
    }

    @Override
    public void destroy() {

    }

    @Override
    public void init(Attribute.Type[] types, SiddhiContext siddhiContext)
    {

    }

    @Override
    protected Object process(Object obj)
    {
        if (obj instanceof Object[])
        {
            return ((Object[]) obj).length;
        }
        else
        {
            return 1;
        }
    }

}