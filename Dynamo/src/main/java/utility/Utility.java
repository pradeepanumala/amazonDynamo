package utility;

import java.io.File;
import java.io.FileFilter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import node.MembershipInfoNode;
import node.NodeImpl;

public class Utility {
public BigInteger md5(String input) {
		
		//String md5 = null;
		BigInteger b1 = null;
		//TODO: Exit the code if input is null
		if(null == input) return null;
		
		try {
			
		//Create MessageDigest object for MD5
		MessageDigest digest = MessageDigest.getInstance("MD5");
		
		//Update input string in message digest
		digest.update(input.getBytes(), 0, input.length());

		//Converts message digest value in base 16 (hex) 
		
		b1 = new BigInteger(1, digest.digest());
		//System.out.println(b1);


		} catch (NoSuchAlgorithmException e) {

			e.printStackTrace();
		}
		return b1;
	}

public HashMap<Integer,Integer> convertStringToHashMap(String input){
	HashMap<Integer,Integer> op=new HashMap<Integer,Integer>();
	String[] data= input.split(",");
	//System.out.println(".."+data.length);
	for(int i=0;i< data.length;i++){
		String[] pair = data[i].split("-");
		op.put(Integer.parseInt(pair[0]),Integer.parseInt(pair[1]));
	}
	return op;
}

public  String convertHashMapToString(HashMap<Integer,Integer> input){
	String output="";
	for (Map.Entry<Integer, Integer> entry : input.entrySet()) {
		output= output+entry.getKey()+"-"+entry.getValue()+",";
	}
    output.substring(0, output.length()-1);
	return output;
}

public static String getMD5(String input) {
    try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] messageDigest = md.digest(input.getBytes());
        BigInteger number = new BigInteger(1, messageDigest);
        String hashtext = number.toString(16);
        // Now we need to zero pad it if you actually want the full 32 chars.
        while (hashtext.length() < 32) {
            hashtext = "0" + hashtext;
        }
        return hashtext;
    }
    catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
    }
}

public ArrayList<String> getFileNames(String Dir,String Pattern){
	
	      ArrayList<String> fileNames = new ArrayList<String>();
		   File dir = new File(Dir);
		   File[] files = dir.listFiles(new FileFilter() {
			    @Override
			    public boolean accept(File pathname) {
			        String name = pathname.getName().toLowerCase();
			        return name.endsWith(Pattern) && pathname.isFile();
			    }
			});
		   
		   for(File file:files)
	       {fileNames.add(file.getName());
	       }
	return fileNames;
}

public static void main(String a[]){
	Utility u = new Utility();
	System.out.println("bla");
	HashMap<Integer,Integer> op=u.convertStringToHashMap("101-12,102-2,103-4,,");
	for (Map.Entry<Integer, Integer> entry : op.entrySet()) {
		System.out.println(".."+entry.getValue());
	}
}

public static void trace(String debugMessage){
	if(NodeImpl.debug)
	System.out.println(debugMessage);
}
 public boolean deleteDirectory(File path) {
    if( path.exists() ) {
      File[] files = path.listFiles();
      for(int i=0; i<files.length; i++) {
         if(files[i].isDirectory()) {
           deleteDirectory(files[i]);
         }
         else {
           files[i].delete();
         }
      }
    }
    return( path.delete() );
  }
}
