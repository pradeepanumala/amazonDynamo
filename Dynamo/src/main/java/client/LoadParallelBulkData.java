package client;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.protobuf.InvalidProtocolBufferException;

import loadBalancer.ILoadBalancer;
import protobuf.DynamoProto.GetResponse;
import protobuf.DynamoProto.PutRequest;



public class LoadParallelBulkData {
	HashMap<String , GetResponse> hm = new HashMap<String,GetResponse>();
	String Ip="10.0.0.1";
	String Port="1099";
	static final int MAX_THREADS = 10;

	public static void main(String args[]){
		String Key;
		String Value;
		LoadParallelBulkData client = new LoadParallelBulkData();
		while(true){
			System.out.println("Please enter an option 1 [for bulk get] and 2 [for Bulk Load] 3[to quit] ");
			
			Scanner reader = new Scanner(System.in);  
			int n = reader.nextInt(); 
			if(n==1){
				int startKey = 600000;
				int endKey   = 500000;
				int keyRange = startKey - endKey;
				long startTime = System.currentTimeMillis();
				for(Integer i = startKey;i>=endKey;i--){
					if(i%20000 == 0)
						System.out.println("Getting " + i);
					client.get(i.toString());
				}
				long endTime = System.currentTimeMillis();
				long timeTaken = ((endTime - startTime)/1000);
				System.out.println("Time Taken in sec : " + timeTaken);
				System.out.println("Number of keys : " +  keyRange);
				
				System.out.println("Number of gets per second" + (keyRange / timeTaken));
				System.out.println("Average Bytes/Sec for Get " + (39+23)*keyRange/timeTaken);
				
			}else if (n==2){
				
				ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_THREADS);
			
				try {
					int startKey = 600000;
					int endKey   = 500000;
					int keyRange = startKey - endKey;
					long startTime = System.currentTimeMillis();	
					for(Integer i = startKey;i>=endKey;i--){
						if(i%20000 == 0)
							System.out.println("Loading " + i);
						
						executor.execute((new LoadParallelBulkData()).new SubmitReq(i+"",i+""));
						
					}
					
					long endTime = System.currentTimeMillis();
					long timeTaken = ((endTime - startTime)/1000);
					System.out.println("Time Taken in sec : " + timeTaken);
					System.out.println("Number of keys : " +  keyRange);
					
					System.out.println("Number of puts per second" + (keyRange / timeTaken));
					System.out.println("Average Bytes/Sec for put " + (39+23)*keyRange/timeTaken);
					//t2.join();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				//	client.put(i.toString(), i.toString());
				
				
		

			}else if (n==3){
				System.out.println("Bye");break;
			}else{
				System.out.println("Enter a correct value");
			}
		}
	}

	 class SubmitReq implements Runnable{
		 String Key;
		 String Value;
      public  SubmitReq(String Key, String Value){
    	  this.Key=Key;
    	  this.Value=Value;
      }
		@Override
		public void run() {
			LoadParallelBulkData client = new LoadParallelBulkData();
			 client.put(Key.toString(), Key.toString());
		}
		 
	 }
	private void get(String Key){
		ILoadBalancer iLoadBalancer;
		try {
			iLoadBalancer = (ILoadBalancer) Naming.lookup("rmi://10.0.0.1:1099/loadBalancer");
			GetResponse gr = GetResponse.parseFrom(iLoadBalancer.get(Key));
		//	System.out.println("Values Fetched :");
			if( gr.getGetResultsList().size()==0)
				System.out.println("Value not found for Key "+Key);
		//	for(GetResult getRes : gr.getGetResultsList()){
				//System.out.println(getRes.getValue());
			//}
		/*	System.out.println("Context Fetched :");
			for(Vector v : gr.getContextList()){
				System.out.println(v.getNodeId()+".."+v.getCounter()+".."+v.getTimeStamp());
			}*/
		} catch (MalformedURLException | RemoteException | NotBoundException | InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
	}
	
	private void put(String Key,String Value){
		ILoadBalancer iLoadBalancer;
		
		try {
			iLoadBalancer = (ILoadBalancer) Naming.lookup("rmi://10.0.0.1:1099/loadBalancer");
			PutRequest.Builder prb = PutRequest.newBuilder();
			prb.setKey(Key);
			prb.setValue(Value);
			prb.setIsReqFromLB(true);
			//prb.addAllContext(hm.get(Key).getContextList());
			
			String output = iLoadBalancer.put(prb.build().toByteArray());
			//System.out.println("Status : "+output);
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}
		
	}
}
