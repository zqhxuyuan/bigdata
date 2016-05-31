package kafka.etl;

public class RequestMQ {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
        KafkaETLRequest request =
                new KafkaETLRequest("SimpleTestEvent", "tcp://localhost" + ":" + "9092", 0);
        
        byte[] bytes = request.toString().getBytes("UTF-8");
        
        System.out.println(new String(bytes,"utf-8"));
	}

}
