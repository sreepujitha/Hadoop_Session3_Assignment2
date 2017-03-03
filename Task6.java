package com.acadgild.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Task6 {

	public static void main(String[] args) {
		if(args.length!=2){
			System.out.println("please pass two arguments");
			System.exit(1);			
		}
		try{
			String src=args[0];
			String dest=args[0];
			InputStream in=new BufferedInputStream(new FileInputStream(src));
				
			Configuration conf=new Configuration();
			FileSystem fs=FileSystem.get(URI.create(dest),conf);
			OutputStream out=fs.create(new Path(dest));
			IOUtils.copyBytes(in,out,4096,true);
		}catch(IOException e){
			e.printStackTrace();
		}
	}

}
