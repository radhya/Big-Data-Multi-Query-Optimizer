/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package SharedHive;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 *
 * @author hadoop
 */
public class commandline {
    
       public static void main(String args[]) throws InterruptedException, IOException {

    String cmd = "hadoop job -list all" ;
    cmd="hadoop job -counter job_201504271208_0001  FileSystemCounters HDFS_BYTES_READ";
	Runtime run = Runtime.getRuntime() ;
	Process pr = run.exec(cmd) ;
	pr.waitFor() ;
	BufferedReader buf = new BufferedReader( new InputStreamReader( pr.getInputStream() ) ) ;
	String line="";
	while ( (  line = buf.readLine() ) != null )
	{
	System.out.println(line) ;
	}
	
       }
}
