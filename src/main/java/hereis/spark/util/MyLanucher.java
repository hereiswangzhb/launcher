package hereis.spark.util;

import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.io.InputStream;

public class MyLanucher {
	private static void dumpInput(InputStream input) throws IOException{
		byte[] buff = new byte[1023];
		while(true){
			int len=input.read(buff);
			
			if(len<0){
				break;
			}
			
			System.out.println(new String(buff,0,len));
		}
	}
	
	public static void main(String [] args) throws IOException,InterruptedException{
		SparkLauncher launcher = new SparkLauncher();
		//		launcher.setSparkHome("/home/hadoop/app/spark");
		launcher.setAppResource("demo.jar");
		launcher.setMainClass("hereis.spark.demo.JavaWordCount");

		launcher.setMaster("local[*]");   // launcher.setMaster("yarn-cluster"); launcher.setMaster("local");
		launcher.setConf(SparkLauncher.DRIVER_MEMORY, "512m");
		launcher.setConf(SparkLauncher.EXECUTOR_MEMORY, "512m");
		launcher.setConf(SparkLauncher.EXECUTOR_CORES, "1");
		
		Process process = launcher.launch();
		InputStream input = process.getInputStream();
		InputStream err   = process.getErrorStream();

		int exitValue = process.waitFor();

		System.out.println("---------------- read msg -----------------");
		dumpInput(input);

		System.out.println("---------------- read err msg -----------------");
		dumpInput(err);

		System.out.println("Launcher over :"+exitValue);
	}
	
}
