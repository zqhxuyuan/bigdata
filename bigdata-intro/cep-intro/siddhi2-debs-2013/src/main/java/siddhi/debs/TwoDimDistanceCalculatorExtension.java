package siddhi.debs;

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;


@SiddhiExtension(namespace = "debs", function = "getTwoDimDistance")
public class TwoDimDistanceCalculatorExtension extends FunctionExecutor
{
    Logger log = Logger.getLogger(TwoDimDistanceCalculatorExtension.class);
    Attribute.Type returnType;

    @Override
    public Attribute.Type getReturnType()
    {
        return returnType;
    }

    @Override
    public void destroy()
    {

    }

    @Override
    public void init(Attribute.Type[] types, SiddhiContext siddhiContext)
    {
        for (Attribute.Type attributeType : types)
        {
            if (attributeType == Attribute.Type.INT)
            {
                returnType = attributeType;
                break;
            }
            else
            {
                throw new QueryCreationException("Position attributes should be type of INT");
            }
        }

    }

    @Override
    protected Object process(Object obj)
    {
        int ax, ay, bx, by;
        double dist = 0;
        if ((obj instanceof Object[]) && ((Object[]) obj).length == 4)
        {
            ax = Integer.parseInt(String.valueOf(((Object[]) obj)[0]));
            ay = Integer.parseInt(String.valueOf(((Object[]) obj)[1]));
            bx = Integer.parseInt(String.valueOf(((Object[]) obj)[2]));
            by = Integer.parseInt(String.valueOf(((Object[]) obj)[3]));

            dist = Math.sqrt( Math.pow((ax - bx), 2) + Math.pow((ay - by), 2) );
        }

        return dist;
    }

}