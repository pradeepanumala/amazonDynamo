package test;

import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;


public class Test2 {
public static void main(String args[]) throws IOException{
	Options option=new Options();
	int myPort=1101;
	DB db = factory.open(new File("/opt/data/instance_" + 1103 + "/myDb1"), new Options());
	
	String key="350000";
	BigInteger keyHash= md5(key);
	String khs = keyHash.toString();
	while (khs.length() < 39)
		khs = "0" + khs;
	
	byte[] value = db.get(bytes(khs));
	System.out.println("size of key "+bytes(khs).length);
	System.out.println("size of value "+value.length);
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

}
