package siddhi.debs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class SocketReader extends DataFeeder
{
    private String s_Ip;
    private int i_Port;
    private Socket m_Socket;

    public SocketReader(String _sIp, int _iPort)
    {
        this.s_Ip = _sIp;
        this.i_Port = _iPort;
    }

    public void run()
    {
        StartProcess();
    }

    protected void StartProcess()
    {
        try {
            m_Socket = new Socket(s_Ip, i_Port);
            ReadData();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void ReadData()
    {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(m_Socket.getInputStream()));
            String line;
            l_StartTime = System.currentTimeMillis();
            while ((line = br.readLine()) != null) {

                //System.out.println("Line " + line);
                ProcessLine(line);
            }
            l_EndTime = System.currentTimeMillis();
            br.close();
            m_Socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void ProcessBuffer(byte [] _buff)
    {
        Object [] tuple = new Object[13];
        ByteBuffer buf = ByteBuffer.wrap(_buff);
        tuple[TokenIds.SID] = buf.getInt();
        tuple[TokenIds.TS] = buf.getLong();
        tuple[TokenIds.X] = buf.getInt();
        tuple[TokenIds.Y] = buf.getInt();
        tuple[TokenIds.Z] = buf.getInt();
        tuple[TokenIds.VEL] = buf.getFloat();
        tuple[TokenIds.ACC] = buf.getFloat();
        tuple[TokenIds.VX] = buf.getInt();
        tuple[TokenIds.VY] = buf.getInt();
        tuple[TokenIds.VZ] = buf.getInt();
        tuple[TokenIds.AX] = buf.getInt();
        tuple[TokenIds.AY] = buf.getInt();
        tuple[TokenIds.AZ] = buf.getInt();

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
            tuple[TokenIds.VEL] = Float.parseFloat(tokens[5].trim());
            tuple[TokenIds.ACC] = Float.parseFloat(tokens[6].trim());
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