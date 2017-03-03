package com.acadgild.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Task5 {

	public static void main(String[] args) {		
		if(args.length!=1){
			System.out.println("please pass argument");
			System.exit(1);	
		}
		String uri=args[0];
		FSDataInputStream in=null;
		try{
			Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(uri),conf);
			
			in=fs.open(new Path(uri));
			IOUtils.copyBytes(in,System.out,4096,false);
		}catch(IOException e){
			e.printStackTrace();
		}
		finally{
			IOUtils.closeStream(in);
		}
	}

}
