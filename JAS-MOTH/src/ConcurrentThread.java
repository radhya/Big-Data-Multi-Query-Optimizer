package SharedHive;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author hadoop
 */ 


import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

  
public  class ConcurrentThread extends Thread {
   public Thread t;
   public String threadName;
  // public ConcurrentListThread ConcurrentThread_Arr[]= new ConcurrentListThread [50];
   //public double start_time=0;
   public double run_time=0;
   public String threadquery="";
   
    ConcurrentThread( ){
       
       threadName="";
       //System.out.println("Creating " +  threadName );
      
   }
    //@Override
   public  void run()  {
       
        try {
            
          
           
            try {
               
               
                {
                           
                        try {
                            try {
                                 
                                 String connection_name=this.threadName;
                                 
                                String sql=this.threadquery;
                                 //start_time=System.currentTimeMillis()/1000;
                                //this.run_time+=connection_db(sql,connection_name);
                                this.run_time+=Connection_to_DB.Connection_DB_No_Write_ResultFile(sql,connection_name);
                               // end_time=System.currentTimeMillis()/1000;
                                 
                                //this.end=true;
                //  System.out.println("Thread: " + threadName +" End .."+end+ " Time: " + System.currentTimeMillis()/1000 );
                            } catch (SQLException ex) {
                                Logger.getLogger(ConcurrentListThread.class.getName()).log(Level.SEVERE, null, ex);
                            } catch (ClassNotFoundException ex) {
                                Logger.getLogger(ConcurrentListThread.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        } catch (IOException ex) {
                            Logger.getLogger(ConcurrentListThread.class.getName()).log(Level.SEVERE, null, ex);
                        }
                                             Thread.sleep(0);
                
                
                }
           } catch (InterruptedException e) {
               System.out.println("Thread " +  threadName + " interrupted.");
           }
            //System.out.println();
           //System.out.println("Thread " +  threadName + " exiting. "+" Thread time.."+(end_time-start_time));
           // long time = ManagementFactory.getThreadMXBean().getThreadCpuTime(this.getId());
           // System.out.println("My thread " + this.getId() + " execution time: " + time);
        } 
        
        finally {
            
        }
   }
   
    @Override
   public void start ()
   {
       
       
      //System.out.println("Starting " +  threadName );
      if (t == null)
      {
         t = new Thread (this, threadName);
         t.start ();
      }
   }
 
   public double   connection_db  (String sql, String connection_name) throws SQLException, ClassNotFoundException, IOException
      {
    
  Connection con ;
  String driverName = "org.apache.hive.jdbc.HiveDriver";
  java.sql.Statement stmt;
   
   
 Class.forName(driverName);
     con  = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
      //con.setAutoCommit(true);
     stmt = con.createStatement();
     //parallel(stmt);
    double start_time=System.currentTimeMillis()/1000;
     System.out.println(connection_name+" start at "+start_time+" In  Running:" +sql);
    stmt.execute(sql);
     double end_time=System.currentTimeMillis()/1000;
     double run_time=end_time-start_time;
   System.out.println(connection_name+" time ="+run_time);
   con.close();
    return run_time;
    
      } //end connection db
  
  
  public static void parallel( java.sql.Statement  stmt  ) throws SQLException , IOException 
   {
      stmt.execute (" SET hive.execution.engine = tez");
    stmt.execute ("SET mapreduce.framework.name=yarn-tez");
    stmt.execute ("SET tez.queue.name=SIU");
    stmt.execute ("SET hive.vectorized.execution.enabled=true");
    stmt.execute ("SET hive.auto.convert.join=true");
    stmt.execute ("SET hive.compute.query.using.stats = [true, **false**]");
    stmt.execute ("SET hive.stats.fetch.column.stats = [true, **false**]");
    stmt.execute ("SET hive.stats.fetch.partition.stats = [true, **false**]");
    stmt.execute ("SET hive.cbo.enable = [true, **false**]");
    stmt.execute ("SET hive.exec.dynamic.partition = true");
    stmt.execute ("SET hive.exec.dynamic.partition.mode=nonstrict");
    stmt.execute ("SET hive.exec.parallel=true");
  //  stmt.execute ("SET hive.exec.mode.local.auto=true");
 /**/   stmt.execute ("SET hive.exec.reducers.bytes.per.reducer=1000000000");
    stmt.execute ("SET hive.mapjoin.smalltable.filesize=1000000000");
/* */   stmt.execute ("SET hive.auto.convert.join.noconditionaltask.size=25000000");/*/
  //*/  stmt.execute ("SET hive.hadoop.supports.splittable.combineinputformat=true");
    stmt.execute ("SET hive.mapjoin.optimized.keys=true");
    stmt.execute ("SET hive.mapjoin.lazy.hashtable=true");
   /**/ stmt.execute ("SET hive.exec.parallel.thread.number=16");
  /**/  stmt.execute ("SET hive.merge.mapfiles=true");
    stmt.execute ("SET hive.merge.mapredfiles=true");/**/
   /// stmt.execute ("SET hive.optimize.skewjoin=true");
    stmt.execute ("SET hive.optimize.bucketmapjoin=true");
    stmt.execute ("SET hive.mapred.supports.subdirectories=true");
    stmt.execute ("SET mapred.input.dir.recursive=true");
    stmt.execute ("SET mapreduce.job.reduces=-1");
    stmt.execute ("SET hive.exec.compress.intermediate=true");
    stmt.execute ("SET hive.exec.compress.output=true");
    stmt.execute ("SET tez.runtime.intermediate-input.is-compressed=true");
    stmt.execute ("SET tez.runtime.intermediate-output.should-compress=true");
    stmt.execute("SET total.order.partitioner.natural.order=false");



    } // end parallel



  public  void recivethread( List<List<String>> Allquerylist)
  {
        List<ConcurrentListThread>  ThreadArr= new ArrayList<ConcurrentListThread>();
      
       //System.out.println("Allquerylist.size"+Allquerylist.size());
       //querieslist.add("select * from lineitem_t2  where  l_discount> 0.01 and l_discount< 0.03");
       // querieslist.add("select * from lineitem_t2  where  l_discount> 0.04 and l_discount< 0.08");
       for (int i = 0; i <Allquerylist.size(); i++)
       { 
          List<String>  querylist= new ArrayList<String>();
          
           String threadname="Con-Thread- list"+(i+1);
           querylist=Allquerylist.get(i);
           String threadquery="";
          // System.out.println("querylist.size "+querylist.size());
           System.out.println(""+querylist);
           //ThreadArr.add( new ConcurrentThread( threadname,threadquery));
          ThreadArr.get(i).start();
       }   
  }
   
  public static void main(String args[]) 
  {
     
      List<String>  querylist= new ArrayList<String>();
        querylist.add(" create table t1 as select * from lineitem_100t  where  l_discount> 0.01 and l_discount< 0.03");
        querylist.add("select * from lineitem_100t  where  l_discount between 0.04 and  0.08");
      
       ConcurrentListThread mythread= new ConcurrentListThread();
       mythread.querylist=querylist;
       mythread.threadName="First";
       mythread.start();
  }
  
   
}// end class ThreadDemo2