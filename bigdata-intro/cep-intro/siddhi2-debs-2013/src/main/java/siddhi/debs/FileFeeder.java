package siddhi.debs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class FileFeeder extends DataFeeder
{

    private String s_DataFilePath;
    //	private Object [] ap_TupleBuffer;
//	private long [] al_TimestampBuffer;
    private int i_RateLimitSize;
    private int i_Timeout;
    private int i_CurrentOffset;
    private long l_LastEventTimestamp;
    private long l_ThisEventTimestamp;
    private long l_LastStatTimestamp;
    private long l_LastEventCount;

    public FileFeeder(String _dataFilePath, int _iTimeout, int _iRateLimitSize)
    {
        this.s_DataFilePath = _dataFilePath;
        this.i_RateLimitSize = _iRateLimitSize;
        this.i_Timeout = _iTimeout;
//		this.ap_TupleBuffer = new Object[i_RateLimitSize];
//		this.al_TimestampBuffer = new long[i_RateLimitSize];
        this.i_CurrentOffset = 0;
        this.l_LastEventTimestamp = System.currentTimeMillis();
        this.l_ThisEventTimestamp = 0;
        this.l_LastStatTimestamp = 0;
        this.l_LastEventCount = 0;
    }

    protected void StartProcess()
    {
        try {
            BufferedReader br = new BufferedReader(new FileReader(s_DataFilePath));
            String line;
            l_StartTime = System.currentTimeMillis();
            while ((line = br.readLine()) != null) {

                //System.out.println("Line " + line);
                ProcessLine(line);
            }
            l_EndTime = System.currentTimeMillis();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * Input file line format (CSV)
     * sid, ts, x, y, z, |v|, |a|, vx, vy, vz, ax, ay, az
     */
    private void ProcessLine(String _line) throws IOException
    {

        String [] tokens = _line.split(",");
        if (tokens.length != 13)
        {
            throw new IOException("Invalid record received");
        }

        Object [] tuple = new Object[13];

        try
        {
            tuple[TokenIds.SID] = Integer.parseInt(tokens[0].trim());
            tuple[TokenIds.TS] = Long.parseLong(tokens[1].trim());
            tuple[TokenIds.X] = Integer.parseInt(tokens[2].trim());
            tuple[TokenIds.Y] = Integer.parseInt(tokens[3].trim());
            tuple[TokenIds.Z] = Integer.parseInt(tokens[4].trim());
            tuple[TokenIds.VEL] = Integer.parseInt(tokens[5].trim());
            tuple[TokenIds.ACC] = Integer.parseInt(tokens[6].trim());
            tuple[TokenIds.VX] = Integer.parseInt(tokens[7].trim());
            tuple[TokenIds.VY] = Integer.parseInt(tokens[8].trim());
            tuple[TokenIds.VZ] = Integer.parseInt(tokens[9].trim());
            tuple[TokenIds.AX] = Integer.parseInt(tokens[10].trim());
            tuple[TokenIds.AY] = Integer.parseInt(tokens[11].trim());
            tuple[TokenIds.AZ] = Integer.parseInt(tokens[12].trim());
        }
        catch (NumberFormatException nfe)
        {
            throw new IOException("Error parsing floating point value in record");
        }

//	    Send(tuple);
        RateLimitSend(tuple);
    }

    private void RateLimitSend(Object [] tuple)
    {
        try
        {
            if(i_CurrentOffset > i_RateLimitSize)
            {
                l_ThisEventTimestamp = System.currentTimeMillis();
                if (l_LastEventTimestamp + i_Timeout >= l_ThisEventTimestamp)
                {
                    Thread.sleep(l_LastEventTimestamp + i_Timeout - l_ThisEventTimestamp);
                }
                l_LastEventTimestamp = l_ThisEventTimestamp;
                i_CurrentOffset = 0;
            }

            m_SiddhiInputHandler.send(tuple);
            l_EventCounter++;
            i_CurrentOffset++;

//	    	l_ThisEventTimestamp = System.currentTimeMillis();
//	    	al_TimestampBuffer[i_CurrentOffset] = l_ThisEventTimestamp;
//	    	ap_TupleBuffer[i_CurrentOffset++] = tuple;
//
//	    	if(i_CurrentOffset == i_RateLimitSize) // if events buffer is full
//	    	{
//	    		l_LastEventTimestamp = l_ThisEventTimestamp;
//	    		for(int i=0; i<i_RateLimitSize; i++)
//	    		{
//	    			m_SiddhiInputHandler.send(al_TimestampBuffer[i], (Object[])ap_TupleBuffer[i]);
//	    		}
//	    		l_EventCounter += i_RateLimitSize;
//	    		i_CurrentOffset = 0;
//	    		Thread.sleep(10);
//	    	}
//	    	else if (l_ThisEventTimestamp - l_LastEventTimestamp >= i_Timeout) // else if max timeout reached
//	    	{
//	    		l_LastEventTimestamp = l_ThisEventTimestamp;
//	    		for(int i=0; i<i_CurrentOffset; i++)
//	    		{
//	    			m_SiddhiInputHandler.send(al_TimestampBuffer[i], (Object[])ap_TupleBuffer[i]);
//	    		}
//	    		l_EventCounter += i_CurrentOffset;
//	    		i_CurrentOffset = 0;
//	    		Thread.sleep(10);
//	    	}
        }
        catch (InterruptedException ex)
        {
            ex.printStackTrace();
        }

//	    long l = l_EventCounter / 100000;
//	    if(l > l_LastCount)
//	    {
//	    	l_LastCount = l;
//	    	System.out.println("Input count : " + l_EventCounter);
//	    }

        if(l_EventCounter % 100000 == 0)
        {
            long now = System.currentTimeMillis();
            long timediff =  now - l_LastStatTimestamp;
            long eventcount = l_EventCounter - l_LastEventCount;
            float throughput = (eventcount / timediff) * 1000;
            l_LastStatTimestamp = now;
            l_LastEventCount = l_EventCounter;
            System.out.println("Input Stream [Count=" + l_EventCounter + " TimeDiff=" + timediff + " Throughput=" + throughput);
        }
    }

    private void Send(Object [] tuple)
    {
        try
        {
            m_SiddhiInputHandler.send(tuple);
            l_EventCounter++;
        }
        catch (InterruptedException ex)
        {
            ex.printStackTrace();
        }

        if(l_EventCounter % 100000 == 0)
        {
            System.out.println("Input count : " + l_EventCounter);
        }
    }
}