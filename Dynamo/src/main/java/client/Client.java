package client;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import com.google.protobuf.InvalidProtocolBufferException;

import loadBalancer.ILoadBalancer;
import protobuf.DynamoProto.GetResponse;
import protobuf.DynamoProto.GetResult;
import protobuf.DynamoProto.PutRequest;
import protobuf.DynamoProto.Vector;

//104, 103, 102, 106, 105, 101
/*72712922306684615368203398412760380840:103
104024752245405710190753707988200232859:104
113178293013889906417492048522395791925:102
167725070164187415741040350671081442450:105
206073825402269071318543232163873447762:101
318334748128727752481564733794903952270:106*/
  
public class Client {
	HashMap<String , GetResponse> hm = new HashMap<String,GetResponse>();
	String Ip="10.0.0.1";
	String Port="1099";
	public static void main(String args[]){
		String Key;
		String Value;
		Client client = new Client();
		while(true){
			System.out.println("Please enter an option 1 [for get] and 2 [for put] 3[to quit] ");
			Scanner reader = new Scanner(System.in);  
			int n =-1;
			try{
			n= reader.nextInt(); 
			}catch(Exception e){
				;
			}
			if(n==1){
				System.out.println("Enter Key");
				Scanner KeyScanner = new Scanner(System.in); 
				Key=KeyScanner.next();
				client.get(Key);
			}else if (n==2){
				
				System.out.println("Enter Key");
				Scanner KeyScanner = new Scanner(System.in); 
				Key=KeyScanner.next();
				
				System.out.println("Enter Value");
				Scanner ValScanner = new Scanner(System.in); 
				Value = ValScanner.next();
				
				for(int i = 10000000;i>=2000;i--){
					
				}
				
				client.put(Key,Value);
				//System.out.println("Key "+Key+"..value "+Value);
			}else if (n==3){
				System.out.println("Bye");break;
			}else{
				System.out.println("Enter a correct value");
			}
		}
	}

	private void get(String Key){
		ILoadBalancer iLoadBalancer;
		try {
			iLoadBalancer = (ILoadBalancer) Naming.lookup("rmi://10.0.0.1:1099/loadBalancer");
			byte[] resp = iLoadBalancer.get(Key);

			GetResponse gr = GetResponse.parseFrom(resp);
			
			if(gr==null){
				System.out.println("Value is not found");
				return;
			}
			if(gr.getGetResultsList().size()>0)
				hm.put(Key, gr);
			System.out.println("Values Fetched :");
			for(GetResult getRes : gr.getGetResultsList()){
				System.out.println(getRes.getValue());
			}
			System.out.println("Context Fetched :");
			for(Vector v : gr.getContextList()){
				System.out.println(v.getNodeId()+".."+v.getCounter()+".."+v.getTimeStamp());
			}
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
			if(hm.get(Key)!=null)
			prb.addAllContext(hm.get(Key).getContextList());
			
			String output = iLoadBalancer.put(prb.build().toByteArray());
			System.out.println("Status : "+output);
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}
		
	}
}
