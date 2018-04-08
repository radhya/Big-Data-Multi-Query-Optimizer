package SharedHive;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author hadoop
 */ 


import java.beans.Statement;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

  

  
  

public class Run {
   public static void main(String args[]) throws InterruptedException {
       
       double start_time=0;
       double end_time=0;
      start_time=System.currentTimeMillis()/1000;
   
   
      //send();
      //sendbylevel(); 
      sendbymanual();
   //   loop();
  
}// end main
   
   public static void send () throws InterruptedException
   {
        List<ConcurrentListThread>  ArrayofThreads= new ArrayList<ConcurrentListThread>();
       for(int i=0; i<2;i++)
       {
       List<String>  querylist= new ArrayList<String>();
        String q1="";
       //int t1=i+7;
       if(i==0)
       {
       q1="create table s"+(i+1)+ " as select * from lineitem_1  where  l_discount> 0.01 and l_discount< 0.05";
        querylist.add(q1);//"create table t5 as select * from lineitem_1  where  l_discount> 0.01 and l_discount< 0.05");
        q1= "create table s"+(i+2)+ " as select from s"+ (i+1)+ " where l_discount between 0.02 and  0.03";
        querylist.add(q1);
       }
       
       if(i==1)
       {
       q1="create table r"+(i+1)+ " as select * from lineitem_1  where  l_discount> 0.01 and l_discount< 0.05";
        querylist.add(q1);//"create table t5 as select * from lineitem_1  where  l_discount> 0.01 and l_discount< 0.05");
        q1= "create table r"+(i+2)+ " as select from r"+ (i+1)+ " where l_discount between 0.02 and  0.03";
        querylist.add(q1);
       }
       
      
       ConcurrentListThread mythread= new ConcurrentListThread();
       mythread.querylist=querylist;
       mythread.threadName=String.valueOf(i+1);
       ArrayofThreads.add(mythread);
      
       }
        for(int i=0; i<2;i++)
       {
          ArrayofThreads.get(i).start();  
       }
        
         for(int i=0; i<2;i++)
       {
          ArrayofThreads.get(i).join();  
       }
      /*
      System.out.println(T1.getState());
      if ((T1.getState().equals("TERMINATED")) && (T2.getState().equals("TERMINATED")) && (T3.getState().equals("TERMINATED")))
      {
       end_time=System.currentTimeMillis()/1000;
       System.out.println("total Time using threads "+(end_time-start_time));
      }  
   */
   } // end send 
   
    public  static void sendbymanual () throws InterruptedException
    {
      ConcurrentListThread  t1 = new ConcurrentListThread();
      ConcurrentListThread  t2 = new ConcurrentListThread();
      ConcurrentListThread  t3 = new ConcurrentListThread();
      ConcurrentListThread  t4 = new ConcurrentListThread();
       List<ConcurrentListThread>  ArrayofThreads= new ArrayList<ConcurrentListThread>();
       List<String>  querylist1= new ArrayList<String>();
       List<String>  querylist2= new ArrayList<String>();
       String q1="";String q2="";
       q1="create table s1 as select * from lineitem_1  where l_discount between 0.01 and 0.05";
       querylist1.add(q1);
       q2="create table s2 as select * from s1  where  l_discount between 0.01 and 0.03";
        querylist1.add(q2);
         q2="create table s3 as select * from s2  where  l_discount=0.02";
        querylist1.add(q2);
       
        t1.querylist=querylist1;
       t1.threadName="S";
       ArrayofThreads.add(t1);
       
       
       //querylist2.clear();
       q2="create table r1 as select * from lineitem_1  where l_discount=0.06";
       querylist2.add(q2);//"create table t5 as select * from lineitem_1  where  l_discount> 0.01 and l_discount< 0.05");
       q2=";";
       querylist2.add(q2);
       t2.querylist=querylist2;
       t2.threadName="R";
       ArrayofThreads.add(t2);
       //boolean isrun=true;
       for(int i=0; i<2;i++)
       {
         ArrayofThreads.get(i).start();
         ArrayofThreads.get(i).join();
       }
       
      
      
    }
   public  static void sendbylevel () throws InterruptedException
   {
        List<ConcurrentListThread>  ArrayofThreads= new ArrayList<ConcurrentListThread>();
       for(int i=0; i<2;i++)
       {
       List<String>  querylist= new ArrayList<String>();
       //List<ConcurrentThread>  ArrayofThreads= new ArrayList<ConcurrentThread>();
       ConcurrentListThread tempthread;//= new ConcurrentThread();
        String q1="";
       //int t1=i+7;
       if(i==0)
       {
       q1="create table s"+(i+1)+ " as select * from lineitem_1  where l_discount between 0.01 and 0.05";
        querylist.add(q1);//"create table t5 as select * from lineitem_1  where  l_discount> 0.01 and l_discount< 0.05");
       tempthread=new ConcurrentListThread();
        tempthread.querylist=querylist;
       tempthread.threadName="s1";//String.valueOf(i+1);
       //tempthread.start();
        ArrayofThreads.add(tempthread);
        
        q1="create table r"+(i+1)+ " as select * from lineitem_1  where l_discount between 0.01 and 0.05";
       
         querylist.add(q1);
         tempthread=new ConcurrentListThread();
        tempthread.querylist=querylist;
      tempthread.threadName="r1";
      // tempthread.start();
       ArrayofThreads.add(tempthread);
       }
       
       if(i==1)
       {
        q1= "create table s"+(i+2)+ " as select from s"+ i+ " where l_discount between 0.02 and  0.03";
        querylist.add(q1);//"create table t5 as select * from lineitem_1  where  l_discount> 0.01 and l_discount< 0.05");
         
          tempthread=new ConcurrentListThread();
        tempthread.querylist=querylist;
        tempthread.threadName="s2";
       //tempthread.start();
        ArrayofThreads.add(tempthread);
        q1= "create table r"+(i+2)+ " as select from r"+  i+ " where l_discount between 0.02 and  0.03";
        querylist.add(q1);
        
          tempthread=new ConcurrentListThread();
        tempthread.querylist=querylist;
      
        tempthread.threadName="r2";
       //tempthread.start();
        ArrayofThreads.add(tempthread);
       }
       
      boolean isrun=true;
      
     
      
    
       }// end for
         
       for(int i=0; i<2;i++)
       {
          ArrayofThreads.get(i).start(); 
           ArrayofThreads.get(i).join(); 
       }
    
     
     
   }
    public   static void loop( )
  {
        for (int i = 0; i <100; i++) 
       System.out.println("i="+i);
  }
   
  public  void recivethread( List<List<String>> Allquerylist)
  {
        List<ConcurrentListThread>  ThreadArr= new ArrayList<ConcurrentListThread>();
      
       //System.out.println("Allquerylist.size"+Allquerylist.size());
       // querieslist.add("select * from lineitem_t2  where  l_discount> 0.01 and l_discount< 0.03");
       // querieslist.add("select * from lineitem_t2  where  l_discount> 0.04 and l_discount< 0.08");
       for (int i = 0; i <Allquerylist.size(); i++)
       { 
          List<String>  querylist= new ArrayList<String>();
          
           String threadname="Con-Thread- list"+(i+1);
           querylist=Allquerylist.get(i);
           String threadquery="";
          // System.out.println("querylist.size "+querylist.size());
           System.out.println(""+querylist);
          // ThreadArr.add( new ConcurrentThread( threadname,threadquery));
          ThreadArr.get(i).start();
       }   
  }
   
} //end class Hive_Thread