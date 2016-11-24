package test;
import org.iq80.leveldb.*;

import com.google.protobuf.ByteString;

import protobuf.DynamoProto.ConcValue;
import protobuf.DynamoProto.Vector;
import protobuf.DynamoProto.VersionValue;
import protobuf.LoadBalancerProto.BInteger;
import utility.VectorComparator;

import static org.fusesource.leveldbjni.JniDBFactory.*;
import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class Test {
	private DB db1;
	//private DB db2;
	private DB db3;
	
	public Test() throws IOException{
	    Options options = new Options();
	    //options.createIfMissing(true);
	  //  new BigInteger("111111111111111111",16);
	
	   db1 = factory.open(new File("/opt/data/instance_1104/DB_105"), options);
	//    db3 = factory.open(new File("/opt/data/myDb1"), options);
	   // factory.repair(new File("/opt/data/myDb1"), options);
	   db1.close();
	//    db3.close();
		/*
		System.out.println("get Property output : " + db1.getProperty("leveldb.sstables"));
		String value = asString(db1.get(bytes(md5("Akshay").toString())));
		ConcValue cv = ConcValue.parseFrom(db1.get(bytes(md5("Akshay").toString())));
		for(VersionValue vv : cv.getVersionValueList()){
			for(Vector v : vv.getContextList()){
				System.out.println(".."+v.getCounter()+".."+v.getNodeId());
			}
		}
        System.out.println(" value is "+ value);
		db1.close();*/
		//db3 = factory.open(new File("newjaffaa"), options);
		
		//db2 = factory.open(new File("jaffaa"), options);
	    
	    //BigInteger lower = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",16);
	  //BigInteger lower = new BigInteger("40000000000000000000000000000000",16);
	   /* System.out.println("Value is " + lower);
	    System.out.println("340282366920938463463374607431768211455".length());
	    BInteger.Builder builder = BInteger.newBuilder();
		ByteString bytes=ByteString.copyFrom(lower.toByteArray());
		builder.setValue(bytes);
		
		System.out.println(new BigInteger(builder.build().getValue().toByteArray()));*/
	}
	
	public void closeConn() throws IOException{
		db1.close();
		//db2.close();
		//db3.close();
	}
	
   
	public static void main2(String args[]){
		
		BigInteger KeyHash = md5("2001");
		String KeyHashString = KeyHash.toString();
		while (KeyHashString.length() < 39)
			KeyHashString = "0" + KeyHashString;
		DB db;
		try {
			db = factory.open(new File("/opt/data/instance_" + 1103 + "/myDb1"), new Options());
			byte[] valInDb = db.get(bytes(KeyHashString));
			ConcValue dbConcValue = ConcValue.parseFrom(valInDb);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static void main(String args[]) throws IOException{
		Options option=new Options();
  int myPort=1101;
  int replacedNodeId=105;
	//DB db = factory.open(new File("/opt/data/instance_1104/DB_101"), option);
	DB db = factory.open(new File("/opt/data/instance_" + 1103 + "/myDb1"), new Options());
		
	String key="2001";
	BigInteger keyHash= md5(key);
	String khs = keyHash.toString();
	
	
	/*ConcValue curConcVal = ConcValue.parseFrom(db.get(bytes(khs)));
	List<VersionValue> curVerValList= curConcVal.getVersionValueList();
	
	//loop through each version eg v1
	for(int k=0;k<curVerValList.size();k++){
		VersionValue v = curVerValList.get(k);
		 List<Vector> curContextList = new ArrayList<Vector>(v.getContextList());
         for(int j=0;j<curContextList.size();j++){
        	 System.out.println(""+curContextList.get(j).getNodeId()+"-"+curContextList.get(j).getCounter());
         }

	}*/
		
	while(khs.length()<39){
			khs = "0"+khs;
		}
	    System.out.println(khs);
	    
	    Vector.Builder vectorBuilder = Vector.newBuilder();
		vectorBuilder.setCounter(1);
		vectorBuilder.setNodeId(103);
		vectorBuilder.setTimeStamp(System.currentTimeMillis());
		
		VersionValue.Builder verValueBuilder = VersionValue.newBuilder();
		verValueBuilder.setValue("V1");
		verValueBuilder.addContext(vectorBuilder);
		
		ConcValue.Builder valueBuilder = ConcValue.newBuilder();
		valueBuilder.addVersionValue(verValueBuilder);
		
		db.put(bytes(khs), valueBuilder.build().toByteArray());
		byte[] val =db.get(bytes(khs));
		System.out.println("val is "+asString(val));
		db.close();
		factory.repair(new File("/opt/data/instance_" + 1103 + "/myDb1"), option);
	//	BigInteger lower = new BigInteger("00003e3b9e5336685200ae85d21b4f5e",16);
		
	/*	Thread t1 = new Thread(new Runnable() {
		      public void run() {
			        // task to run goes here
			    	  System.out.println("Kuch to hua ha");
			    	  test.load3();
			    	  //System.out.println("heart beat report"+System.currentTimeMillis());
			      }
			    });*/
		/*Thread t2 = new Thread(new Runnable() {
		      public void run() {
			        // task to run goes here
			    	  
			    	  test.load2();
			    	  //System.out.println("heart beat report"+System.currentTimeMillis());
			      }
			    });*/
	//	t1.start();
		//t2.start();
	/*	try {
			t1.join();
			//t2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	     test.closeConn();*/

	}
	
	public void load1(){
		 try {
	         // Use the db in here....
			 		System.out.println("Tampa1");
	               db1.put(bytes("Tampa1"), bytes("rocks"));
	               System.out.println("Tampa2");
	               db1.put(bytes("Tampa2"), bytes("popo"));
	               System.out.println("Tampa3");
	               db1.put(bytes("Tampa3"), bytes("rocks"));
	               System.out.println("Tampa4");
	               db1.put(bytes("Tampa4"), bytes("popo"));
	               System.out.println("Tampa5");
	               db1.put(bytes("Tampa5"), bytes("rocks"));
	               db1.put(bytes("Tampa6"), bytes("popo"));
	               db1.put(bytes("Tampa7"), bytes("rocks"));
	               db1.put(bytes("Tampa8"), bytes("popo"));
	               db1.put(bytes("Tampa9"), bytes("rocks"));
	               
	                             

	               String value = asString(db1.get(bytes("Tampa")));
	               //db.delete(bytes("Tampa"));
	               //System.out.println(" value is "+ value);
       
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
	}
	
	public void load2(){
		
		 try {
	         // Use the db in here....
			/*       System.out.println("Tampa11");
	               db2.put(bytes("Tampa11"), bytes("rocks"));
	               System.out.println("Tampa12");
	               db2.put(bytes("Tampa12"), bytes("popo"));
	               System.out.println("Tampa13");
	               db2.put(bytes("Tampa13"), bytes("rocks"));
	               System.out.println("Tampa14");
	               db2.put(bytes("Tampa14"), bytes("popo"));
	               System.out.println("Tampa15");
	               db2.put(bytes("Tampa15"), bytes("rocks"));
	               db2.put(bytes("Tampa16"), bytes("popo"));
	               db2.put(bytes("Tampa17"), bytes("rocks"));
	               db2.put(bytes("Tampa18"), bytes("popo"));
	               db2.put(bytes("Tampa19"), bytes("rocks"));*/

	               //String value = asString(db.get(bytes("Tampa")));
	               //db.delete(bytes("Tampa"));
	               //System.out.println(" value is "+ value);
      
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
		
	}
	
	
	public void load3(){
		 try {
	         // Use the db in here....
			 Integer i;
			 
			 for(i=500000;i>=2000;i--){
				 if(i%50000 == 0)
				   System.out.println("Thread 1 Loading " + i);
				 String str = i + " Chinna";
	          //     db1.put(bytes(md5(i.toString())), bytes(str));
			 }
			
     
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
	}
	
	public void load4(){
		 try {
	         // Use the db in here....
		// String value = asString(db1.get(bytes(md5("Hell10"))));
		//	 System.out.println("VALUE IS: " + value);
			 
			 //db1.put(bytes("1"), bytes("Akshay1"));
			 
			 System.out.println("get Property output : " + db1.getProperty("leveldb.sstables"));
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
	}
	
	

	public static BigInteger md5(String input) {
		
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(input.getBytes());
            BigInteger number = new BigInteger(1, messageDigest);
           // String hashtext = number.toString(16);
            // Now we need to zero pad it if you actually want the full 32 chars.
           
            return number;
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        
    }
	
	
	public void load5(){
		 try {
	         // Use the db in here....
			 		//System.out.println("Tampa1");
	               db3.put(bytes("PradeepAnumala"), bytes("rocks"));


	               String value = asString(db3.get(bytes("PradeepAnumala")));
	               //db.delete(bytes("Tampa"));
	               System.out.println(" value is "+ value);
      
	       } finally {
	         // Make sure you close the db to shutdown the 
	         // database and avoid resource leaks
	       }	
	}

	
	
	
	
	
}