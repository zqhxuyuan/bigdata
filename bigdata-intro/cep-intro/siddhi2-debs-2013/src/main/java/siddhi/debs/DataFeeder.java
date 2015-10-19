package siddhi.debs;

import org.wso2.siddhi.core.stream.input.InputHandler;

public abstract class DataFeeder implements Runnable
{
    protected InputHandler m_SiddhiInputHandler;
    public long l_StartTime;
    public long l_EndTime;
    protected long l_EventCounter;

    public DataFeeder()
    {
    }

    public void SetInputHandler(InputHandler _inputHandler)
    {
        this.m_SiddhiInputHandler = _inputHandler;
    }

    public long GetEventCounter()
    {
        return l_EventCounter;
    }

    public void run()
    {
        StartProcess();
    }

    protected abstract void StartProcess();
}