package node;

import java.math.BigInteger;

public class FileInfo {
String fileName;
BigInteger lower;
BigInteger upper;

public FileInfo(String fileName,BigInteger lower,BigInteger upper){
	this.fileName = fileName;
	this.lower = lower;
	this.upper = upper;
}

}
