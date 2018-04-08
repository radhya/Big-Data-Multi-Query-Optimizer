/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package SharedHive;

/**
 *
 * @author hadoop
 */
//import AllTheads.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
public class Excutor {
    
  

    public static void  drop_tables(int table_num)
    {
        String sql="";
        for(int i=1; i<=table_num;i++)
        {
             
       
        
        try {
            sql="drop table t"+String.valueOf(i); connection_db(sql);
            sql="drop table t"+String.valueOf(i)+"1"; connection_db(sql);
            sql="drop table t"+String.valueOf(i)+"2"; connection_db(sql);
        } catch (SQLException ex) {
            Logger.getLogger(Excutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Excutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Excutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }// end for i
    }// end drop_tables
    
     public  static void  drop_table(String table_name)
    {
        String sql="";
        
        sql="drop table "+table_name.trim();
        try {
            connection_db(sql);
        } catch (SQLException ex) {
            Logger.getLogger(Excutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Excutor.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Excutor.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public static void  connection_db  (String sql) throws SQLException, ClassNotFoundException, IOException
      {
     
   
   Connection con ;
  String driverName = "org.apache.hive.jdbc.HiveDriver";
  java.sql.Statement stmt;
   
   
 Class.forName(driverName);
     con  = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
      stmt = con.createStatement();
        //parallel(stmt);
      

     //System.out.println("In  Running:" +sql);
    
     
    stmt.execute(sql);
    con.close();
      } // end connection db
   
  public static double   connection_db_time  (String sql) throws SQLException, ClassNotFoundException, IOException
      {
     
    double start_time=0;
    double end_time=0;
    double run_time=0;
   
   Connection con ;
  String driverName = "org.apache.hive.jdbc.HiveDriver";
  java.sql.Statement stmt;
   
   
 Class.forName(driverName);
     con  = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
      stmt = con.createStatement();
       
   System.out.println("In  Running:" +sql);
    // to cashed data
   // stmt.execute(get_subquery(sql));
   if(sql.contains("create"))
   {
       int indexofas=sql.indexOf("as");
       String s=sql.substring(13,indexofas);
        //System.out.println(s);
       drop_table(s.trim());
   }
   start_time=System.currentTimeMillis()/1000;
    
   stmt.execute(sql);
   
   end_time=System.currentTimeMillis()/1000;
    //stmt.execute(sql);
    con.close();
    /*
    if(sql.contains("UNION"))
    run_time=0;
    else
     * *
     */
    run_time=end_time-start_time;
    
    return run_time;
      } // end connection db
   
  
  public static String  get_subquery(String sql)
    {
      String s="";
      int indexofselect=sql.indexOf("select");
      int indexofwhere=sql.indexOf("where");
      s=sql.substring(indexofselect, indexofwhere);
     System.out.print(s);
      return s;
    }
     public  static double runSequtailQueryList(List <String> querylist) throws SQLException, IOException, ClassNotFoundException
   {
        // drop_tables(10);
     List <Double>  querytimelist =new ArrayList <Double> ();  
     //System.out.println("---------------runSequtailQueryListt--------------------");
     double time=0;
     for(int i=0; i<querylist.size();i++)
     {
        String sql=querylist.get(i); 
        //connection_db(sql);
        querytimelist.add(connection_db_time(sql));
        System.out.println("time...."+ querytimelist.get(i));
        time+=querytimelist.get(i);
     }
   
    //System.out.println("----Sequtail time="+time);
     return time;
   }     //runSequtailQuery
     
    
      public  static List <Double> runcashedSequtailQueryList(List <String> querylist) throws SQLException, IOException, ClassNotFoundException
   {
       //drop_tables(10);
          List <Double>  querytimelist =new ArrayList <Double> ();  
      System.out.println("---------------runSequtailQueryListt--------------------");
      double time=0;
     for(int i=0; i<querylist.size();i++)
     {
        String sql=querylist.get(i); 
        //connection_db(sql);
        querytimelist.add(connection_db_time(sql));
        time+=querytimelist.get(i);
     }
   
    System.out.println("----cashed time="+time);
     return querytimelist;
   }
     //runcashedSequtailQuery
   
      
       public  static void runConcurrentList(List<List<String>> Allquerylist) throws SQLException, IOException, ClassNotFoundException, InterruptedException
   {
       //drop_tables(10);
       //List<List<Double>> array = new ArrayList<List<Double>>();
       List<ConcurrentListThread>  ArrayofThreads= new ArrayList<ConcurrentListThread>();
       //ConcurrentThread temp=new ConcurrentThread();
       //List <Double>  querytimelist =new ArrayList <Double> ();  
       List <String>  querylist =new ArrayList <String> ();  
           System.out.println("---------------runconcurrentQueryList--------------------");
         for(int i=0; i<Allquerylist.size();i++)
     {
        querylist=Allquerylist.get(i);
         
        ConcurrentListThread temp=new ConcurrentListThread();
        temp.querylist=querylist;
        temp.threadName=String.valueOf(i+1);
        ArrayofThreads.add(temp);
     }
       
       
       double run_time=0;
       // start thread
          for(int i=0; i<Allquerylist.size();i++)
          {
               ArrayofThreads.get(i).start();
               ArrayofThreads.get(i).join();
               run_time+=ArrayofThreads.get(i).run_time;
                //ArrayofThreads.get(i).s
             // ManagementFactory.getThreadMXBean().setThreadCpuTimeEnabled(true);//.getThreadCpuTime(ArrayofThreads.get(i).getId());
               
              
          }
      //long time = ManagementFactory.getThreadMXBean().getThreadCpuTime(ArrayofThreads.get(0).getId());
      //long total_time=//getUserTime(ArrayofThreads) ;
      //System.out.println("threadtime= "+run_time);
    return ;//querytimelist;
   }
     //runConcurrentList
       
       
       public  static void runConcurrent(List<String> Allquerylist) throws SQLException, IOException, ClassNotFoundException, InterruptedException
   {
       //drop_tables(10);
       //List<List<Double>> array = new ArrayList<List<Double>>();
       List<ConcurrentThread>  ArrayofThreads= new ArrayList<ConcurrentThread>();
       //ConcurrentThread temp=new ConcurrentThread();
       //List <Double>  querytimelist =new ArrayList <Double> ();  
        String sql="";
           System.out.println("---------------runconcurrentQueryList--------------------");
         for(int i=0; i<Allquerylist.size();i++)
     {
        sql=Allquerylist.get(i);
         
        ConcurrentThread temp=new ConcurrentThread();
        temp.threadquery=sql;
        temp.threadName=String.valueOf(i+1);
        ArrayofThreads.add(temp);
     }
       
       
       
       // start thread
          for(int i=0; i<Allquerylist.size();i++)
          {
               ArrayofThreads.get(i).start();
               ArrayofThreads.get(i).join();            
          }
      
    return ;//querytimelist;
   }
     
        public static void runConcurrent_2( List<String> Allquerylist) throws SQLException, IOException, ClassNotFoundException, InterruptedException
        {
            //pool thread
            int pool_size=3;
        ExecutorService executor = Executors.newFixedThreadPool(pool_size);
        String sql="";
           System.out.println("---------------runconcurrentQueryList--------------------");
           
           double start_time=0;
          double end_time=0;
          start_time=System.currentTimeMillis()/1000;
         for(int i=0; i<Allquerylist.size();i++)
     {
        sql=Allquerylist.get(i);
         
       
            ConcurrentThread  worker = new ConcurrentThread();
            worker.threadquery=sql;
            worker.threadName=String.valueOf(i+1);
            executor.execute(worker);
           
          }
        
        executor.shutdown();
       
        while (!executor.isTerminated()) {
        }
         end_time=System.currentTimeMillis()/1000;
        System.out.println("Finished all threads with time:"+(end_time-start_time));
    }
       public static void main(String args[]) throws SQLException, IOException, InterruptedException, ClassNotFoundException 
       {
       List <String>  querylist =new ArrayList <String> ();  
      
      //drop_tables(10);
       /*
        * here read list of  concurrent list and drop tables if are found 
        */
     // querylist=ReadWriteFiles.read_to_List();
       querylist.add(" create table t1 as select * from lineitem_10m  where   l_discount=0.01");
       querylist.add(" create table t2 as select * from lineitem_10m  where   l_discount=0.02");
       querylist.add(" create table t3 as select * from lineitem_10m  where   l_discount=0.03");
       querylist.add(" create table t4 as select * from lineitem_10m  where   l_discount=0.04");
       runConcurrent(querylist);
      
     
  }
       
       
      public  static long getUserTime(  List<ConcurrentListThread> conthreads)// ,long[] ids )
      {
    ThreadMXBean bean = ManagementFactory.getThreadMXBean( );
    if ( ! bean.isThreadCpuTimeSupported( ) )
        return 0L;
    long time = 0L;
     for (  int i=0; i<conthreads.size();i++ ) {
        long t = bean.getThreadUserTime( conthreads.get(i).getId() );
        if ( t != -1 )
            time += t;
    }
    return time;
      }// getUserTime
      
} // end class excutor
