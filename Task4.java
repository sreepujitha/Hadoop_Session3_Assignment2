package com.acadgild.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Task4 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if(args.length!=3){
			System.out.println("please pass 3 arguments");
			System.exit(1);			
		}
		Path path=new Path(args[0]);
		long start_ts=Long.parseLong(args[1]);
		long end_ts=Long.parseLong(args[2]);
			Configuration conf=new Configuration();
			iterateDirectory(path,conf,start_ts,end_ts);	
		
	}
static void iterateDirectory(Path dirpath,Configuration config,long sts,long ets){
	try{
FileSystem fileSystem=FileSystem.get(dirpath.toUri(),config);
FileStatus[] fileStatus=fileSystem.listStatus(dirpath);
for(FileStatus fstat:fileStatus){
	if(fstat.isDirectory()){
		Path dirPath=fstat.getPath();				
		iterateDirectory(dirPath,config,sts,ets);				
	}else if(fstat.isFile()){
		long modifiedTime=fstat.getModificationTime();
		if(modifiedTime>sts && modifiedTime<ets){
			System.out.println("File" + fstat.getPath()+" "+fstat.getModificationTime());
			}
		}
	}
	}catch(IOException e){
		e.printStackTrace();
	}	
	}


}
