package SharedHive;


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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author hadoop
 */
public class Test_Sequntail {
   
    
    
    
 public static void main(String args[]) throws SQLException, IOException 
 {
    double start_time=0;
    double end_time=0;
     String sql="";
       start_time=System.currentTimeMillis()/1000;
        try {
            sql="select * from part";
            String filter1="l_discount> 0.01 and l_discount< 0.08";
            String filter2="l_discount> 0.01 and l_discount< 0.03";
            String filter3="l_discount> 0.04 and l_discount< 0.08";
             String filter4="l_discount=0.04";
           
            //sql="select l_discount, sum(l_tax) as sum_tax from lineitem_1 where "+ filter2+ " group by l_discount ";
              //sql="show tables";connection_db_to_list(sql,"tables_list");
              //sql=" create table lineitem_10mC as select L_ORDERKEY, L_PARTKEY , L_DISCOUNT from  lineitem_10m where L_DISCOUNT between 0.01 and 0.03 ";connection_db(sql);
              //sql=" create table Temp_10m as select L_ORDERKEY, L_PARTKEY , L_DISCOUNT from  lineitem_10m where L_DISCOUNT between 0.04 and 0.07";connection_db(sql);
              // sql=" create table t_10m as select L_ORDERKEY, L_PARTKEY , L_DISCOUNT from  lineitem_10m where L_DISCOUNT between 0.02 and 0.05";connection_db(sql);
                test_scan(); 
              
             
               //sql="select * from My_Histogram where table_name='lineitem_1'";connection_db_to_list(sql);
             //  sql="load data local inpath '/home/hadoop/Desktop/Radhya/Benchmark.part.csv' into table part1";
           // sql="drop table  tt4";connection_db(sql); sql="drop table  tt5";connection_db(sql); sql="drop table  s3";connection_db(sql);
             //Excutor.drop_table("t1") ; sql="show tables";connection_db_to_list(sql);
              //test_join();
               //test_scan();
               //sql=union();connection_db(sql);
             /* 
             sql="select L_DISCOUNT,count(*) from t_10m group by L_DISCOUNT order by L_DISCOUNT ";
                connection_db_to_list(sql,"t_10m");
                sql="select L_DISCOUNT,count(*) from t_10m4 group by L_DISCOUNT order by L_DISCOUNT";
                connection_db_to_list(sql,"t_10m4");
              * */
              
                // test_rewite_join();
              //test_compute();
            //sql="drop table  r1";connection_db(sql); sql="drop table  r2";connection_db(sql);
        
         //sql="select * from My_Histogram";connection_db_to_list(sql);
          //test_scan();
          // sql="create table t4 as select L_ORDERKEY,L_PARTKEY,l_discount from lineitem_1m where l_discount>= 0.01 and l_discount<=0.04";
           //connection_db(sql);
           // sql="select l_discount from s2";connection_db_to_list(sql);
           // sql="select L_ORDERKEY,l_discount from t6  where  l_discount=0.03";connection_db_to_list(sql);
         
            
         
           // sql="create table t10 as "+sql;
            //sql="select * from lineitem_t2  where  "+ filter2;
            //sql="select * from lineitem_t2  where "+ filter3;
           // sql="select * from lineitem_t2  where "+ filter4;
            // connection_db(sql);
            
            //sql="select lo_commitdate , count(lo_commitdate) from lineorder group by lo_commitdate";

           
           /* System.out.println("--------------------------------------");
            sql="select name from customer";
            sql="select custkey  from lineorder";
            connection_db(sql);
            System.out.println("--------------------------------------");
           
            sql="select lo_discount from lineorder1 where lo_discount =8";
            sql="select custkey  from lineorder";
            connection_db(sql);*/
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Test_Sequntail.class.getName()).log(Level.SEVERE, null, ex);
        }
       end_time=System.currentTimeMillis()/1000;
       System.out.println("total Time   "+(end_time-start_time));     
 }// end main
 
 public  static String  union() throws SQLException, ClassNotFoundException, IOException
 {
     String s1="";
     s1="CREATE TABLE t1t2b AS SELECT log.L_ORDERKEY, log.L_DISCOUNT";
s1+=" FROM ( SELECT l1.L_ORDERKEY, l1.L_DISCOUNT FROM t1 l1 where l1.L_DISCOUNT between 0.03 and 0.06";
s1+=" UNION log SELECT l2.L_ORDERKEY, l2.L_DISCOUNT FROM t2 l2";
s1+=" ) log";
//s1="CREATE TABLE  t1 AS Select log.l_discount , log.L_ORDERKEY From ( SELECT qt1.l_discount , qt1.L_ORDERKEY FROM t2 qt1 where qt1.l_discount between 0.03 and 0.05 UNION log SELECT qt2.l_discount , qt2.L_ORDERKEY FROM lineitem_1 qt2  where qt2.l_discount between 0.01 and 0.02 ) log";
s1="CREATE TABLE  t1b AS Select log.L_DISCOUNT, log.L_ORDERKEY From ( SELECT qt1.L_DISCOUNT , qt1.L_ORDERKEY FROM t11  qt1 UNION All SELECT qt2.L_DISCOUNT , qt2.L_ORDERKEY FROM t12  qt2 ) log";
 

//String sql=" create table lineitem_10mC as select L_ORDERKEY, L_PARTKEY , L_DISCOUNT from  lineitem_10m where L_DISCOUNT between 0.01 and 0.03 ";connection_db(sql);
   //sql=" create table Temp_10m as select L_ORDERKEY, L_PARTKEY , L_DISCOUNT from  lineitem_10m where L_DISCOUNT between 0.04 and 0.07";connection_db(sql);
    //sql=" create table t_10m as select L_ORDERKEY, L_PARTKEY , L_DISCOUNT from  lineitem_10m where L_DISCOUNT between 0.02 and 0.05";connection_db(sql);
             
//s1=" create table t8 as select from t1";
String table1="lineitem_10mC";
String table2="Temp_10m";
s1="CREATE TABLE  t_10m4 AS SELECT log.L_ORDERKEY, log.L_PARTKEY, log.l_discount ";
s1+="FROM ( SELECT l1.L_ORDERKEY, l1.L_PARTKEY , l1.L_DISCOUNT FROM "+ table1+ " l1 where l1.L_DISCOUNT between 0.02 and 0.03 ";
s1+="UNION ALL SELECT l2.L_ORDERKEY, l2.L_PARTKEY, l2.L_DISCOUNT FROM "+ table2+" l2 where l2.L_DISCOUNT between 0.04 and 0.05";
s1+=") log";
//s1="create table line100 as select *from lineitem_100t ";
//s1="create table union3 as  SELECT L_ORDERKEY, L_PARTKEY, l_discount from lineitem_100t where  l_discount between 0.01 and 0.02 ";
//s1="";
System.out.println(s1);
return s1;

 }
 public static void  test_scan() throws SQLException, ClassNotFoundException, IOException
 {
     String sql="";
     
     /*sql="create table t1 as select L_ORDERKEY, L_PARTKEY , L_SHIPDATE, l_discount   from lineitem_100t  where l_discount between 0.02 and 0.04 ";
      connection_db(sql);
      
      sql="create table t2 as select L_ORDERKEY,  l_discount   from lineitem_100t  where l_discount between 0.02 and 0.04 ";
      connection_db(sql);
      */
     sql=" select  count(*) from lineitem_100t where l_discount between 0.01 and 0.03 or l_discount between 0.05 and 0.06  ";
     //connection_db_to_list(sql,"betwwen");
      
    
                
              
 }
 
 public static void  test_rewite_join() throws SQLException, ClassNotFoundException, IOException
 {
     String sql="";
      Excutor.drop_table("j1");
    sql=" create table  j1 as SELECT  c.c_custkey, c.c_city ,lo.lo_discount FROM customer c  Join lineorder25 lo on (lo.lo_custkey = c.c_custkey) Join lineitem_u100t lt on (lt.l_orderkey=lo.lo_orderkey)  where lo.lo_discount=1.0 order by c.c_custkey";  
     sql=" create table  j1 as SELECT  c.c_custkey, c.c_name ,lo.lo_discount FROM customer c  Join lineorder25 lo on (lo.lo_custkey = c.c_custkey) Join lineitem_u100t lt on (lt.l_orderkey=lo.lo_orderkey)  where lo.lo_discount=1.0 order by c.c_custkey";  
    //sql= " create table  j1 as SELECT  c.c_custkey, c.c_name ,lo.lo_discount FROM customer c  Join lineorder25 lo on (lo.lo_custkey = c.c_custkey)";
    //connection_db(sql);
    //sql="RadSa ya";
    
    System.out.println(sql.lastIndexOf("R"));
    
    
     sql=sql.replace("customer c  Join lineorder25 lo on (lo.lo_custkey = c.c_custkey)", "....");
      System.out.println(sql);
 }
 
 public static void  test_compute() throws SQLException, ClassNotFoundException, IOException
 {
     String sql="";
     Excutor.drop_table("c1");
     //(l_discount*5)/(L_TAX*10)   ((l_discount*5)*(L_QUANTITY*10))/(L_TAX*10)
     String operation="l_discount*5";
     operation="((l_discount*5)*(L_QUANTITY*10))/(L_TAX*10)";
     String tabe_name="lineitem_50m";
     String where=" where l_discount between 0.02 and 0.04";
     sql=" create table  c1  as SELECT l_discount,L_ORDERKEY," +operation+" as ld from "+ tabe_name+ " where l_discount between 0.01 and 0.05";
     connection_db(sql);
     //sql="select * from c1";
     //connection_db_to_list(sql, "c1");
     
     // for cashe
      sql="SELECT l_discount,L_ORDERKEY, ld from c1";
     connection_db(sql);
     
     Excutor.drop_table("c2");
     sql=" create table  c2  as SELECT l_discount,L_ORDERKEY, ld from c1 "+ where;
     connection_db(sql);
     //sql="select * from c2";
     //connection_db_to_list(sql, "c2");
     
     Excutor.drop_table("c3");
     sql=" create table  c3  as SELECT l_discount,L_ORDERKEY, "+operation+"  as ld from "+tabe_name + where;
     connection_db(sql);
    // sql="select * from c3";
    // connection_db_to_list(sql, "c3");
     
 }
 public static void  test_join() throws SQLException, ClassNotFoundException, IOException
 {
     String sql="";
     Excutor.drop_table("j1");
     sql=" create table  j1 as SELECT c.c_custkey, c.c_city ,lo.lo_orderkey, lo.lo_discount FROM  lineorder25 lo join lineitem_u100t lt on (lt.l_orderkey=lo.lo_orderkey) join customer c on (lo.lo_custkey = c.c_custkey) where lo.lo_discount=1.0 order by c.c_custkey";
     connection_db(sql);
     sql="select * from j1";
     connection_db_to_list(sql, "j1");

    
     Excutor.drop_table("j2");
     sql=" create table  j2 as SELECT  c.c_custkey, c.c_city ,lo.lo_orderkey, lo.lo_discount FROM customer c  Join lineorder25 lo on (lo.lo_custkey = c.c_custkey) Join lineitem_u100t lt on (lt.l_orderkey=lo.lo_orderkey)  where lo.lo_discount=1.0 order by c.c_custkey";  
     connection_db(sql);
     sql="select * from j2";
     connection_db_to_list(sql, "j2");
     
     
     Excutor.drop_table("temp");
     sql=" create table  temp as SELECT  c.c_custkey, c.c_city ,lo.lo_orderkey, lo.lo_discount FROM customer c  Join lineorder25 lo on (lo.lo_custkey = c.c_custkey) where lo.lo_discount=1.0 order by c.c_custkey";  
     connection_db(sql);
     
     Excutor.drop_table("j3");
     sql=" create table  j3 as SELECT  t.c_custkey, t.c_city ,t.lo_orderkey, t.lo_discount FROM temp t Join lineitem_u100t lt on (lt.l_orderkey=t.lo_orderkey) order by t.c_custkey";//  where t.lo_discount=1.0 order by t.c_custkey";  
     connection_db(sql);
     sql="select * from j3";
     connection_db_to_list(sql, "j3");
     
     
     
       /*
      Excutor.drop_table("j1");
     sql=" create table j1 as SELECT  c.c_custkey, c.c_city ,lo.lo_discount, lo_orderkey  FROM customer c  Join lineorder25 lo on (lo.lo_custkey = c.c_custkey) ";
     connection_db(sql);  
     Excutor.drop_table("j2");
      sql=" create table j2 as SELECT  j.c_custkey, j.c_city ,j.lo_discount, lt.l_discount From j1 j Join lineitem_10m lt on (lt.l_orderkey=j.lo_orderkey)  where j.lo_discount=1.0 order by j.c_custkey";
     connection_db(sql);*/
       
     /*
  SELECT  c.custkey, c.city ,lo.lo_discount, lt.l_discount, s.suppkey  FROM customer c  Join lineorder25 lo on (lo.lo_custkey = c.custkey) Join lineitem_u100T lt on (lt.l_orderkey=lo.lo_orderkey) Join supplier s on (s.suppkey=lo.lo_suppkey)  where lo.lo_discount=1.0 order by c.custkey;  
  */
 }
 public static void  test_groupby() throws SQLException, ClassNotFoundException, IOException
 {
     String sql="";
      sql="create table t1 as select l_discount, sum(l_discount) as sd from lineitem_1m where l_discount between 0.01 and 0.05 group by l_discount";
             connection_db(sql);
              sql="create table t2 as select l_discount, sum(l_discount) as sd from lineitem_1m where l_discount between 0.02 and 0.03 group by l_discount";
             connection_db(sql);
              sql="create table t3 as select l_discount, sd from t1 where l_discount between 0.02 and 0.03";
             connection_db(sql);
             
              sql="create table t4 as select l_discount, sum(l_discount) from lineitem_1m where l_discount=0.02 group by l_discount";
             connection_db(sql);
             sql="create table t5 as select l_discount, sd from t1 where l_discount=0.02";
             connection_db(sql);
             sql="create table t6 as select l_discount, sd from t2 where l_discount=0.02";
             connection_db(sql);
              sql="create table t7 as select l_discount, sd from t3 where l_discount=0.02";
             connection_db(sql);
 }
 
 
 public static void  connection_db  ( String sql) throws SQLException, ClassNotFoundException, IOException
      {
      //replace "hive" here with the name of the user the queries should run as
   
    
   
   Connection con ;
  String driverName = "org.apache.hive.jdbc.HiveDriver";
  java.sql.Statement stmt;
   
   
 Class.forName(driverName);
     con  = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
      stmt = con.createStatement();
      //parlogel(stmt);
      
     System.out.println(" Running:" +sql);
    
      double start_time=0;
    double end_time=0;
if(sql.contains("create"))
   {
       int indexofas=sql.indexOf("as");
       String s=sql.substring(13,indexofas);
      // s="t1t22";
        //System.out.println(s);
       stmt.execute(" drop table "+s.trim());
   }
if( sql.contains("CREATE"))
   {
       int indexofas=sql.indexOf("AS");
       String s=sql.substring(13,indexofas);
      // s="t1t22";
        //System.out.println(s);
       stmt.execute(" drop table "+s.trim());
   }
    //stmt.execute(Excutor.get_subquery(sql));
       start_time=System.currentTimeMillis()/1000;
    stmt.execute(sql);
     end_time=System.currentTimeMillis()/1000;
       System.out.println("total Time   "+(end_time-start_time));  
    //ResultSet res = stmt.executeQuery(sql);
    //ResultSetMetaData rsmd;
    /*List<String> records = new ArrayList<String>();
    int row=0;
    
    while (res.next()) 
    {
        String line="";
        rsmd=res.getMetaData();
         int col_size=rsmd.getColumnCount();
         int i;
        // System.out.print((row+1)+"\t");
         for( i=1;i<=col_size;i++) 
         {
    // System.out.println(res.getString(i)+"     ["+(System.currentTimeMillis())+"]");
      //System.out.println(res.getString(i)+",");//   ["+(System.currentTimeMillis())+"]");
      line+=res.getString(i)+",";
   
         }
         line+="\n";
         records.add(line);
     //System.out.println();
     row++;
    }
    int buf_size=1048576;
    String file_name="lineorder_result";
      Test.FileWritingBuffersize.writeBuffered(file_name, records, buf_size);*/
    con.close();
    
      } //end connection db
public static void  connection_db_to_list  ( String sql, String file_name) throws SQLException, ClassNotFoundException, IOException
      {
      //replace "hive" here with the name of the user the queries should run as
   
    
   
   Connection con ;
  String driverName = "org.apache.hive.jdbc.HiveDriver";
  java.sql.Statement stmt;
   
   
 Class.forName(driverName);
     con  = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
      stmt = con.createStatement();
      //parlogel(stmt);
    
      if(sql.contains("create") || sql.contains("CREATE") )
   {
       int indexofas=sql.indexOf("as");
       String s=sql.substring(13,indexofas);
      // s="t1t22";
        //System.out.println(s);
       stmt.execute(" drop table "+s.trim());
   }
     System.out.println(" Running:" +sql);
    
    
  //con.setAutoCommit(false);
   // System.out.println("AutoCommit "+con.getAutoCommit());
    ResultSet res = stmt.executeQuery(sql);
    
    ResultSetMetaData rsmd;
    List<String> records = new ArrayList<String>();
    int row=0;
    
    while (res.next()) 
    {
        String line="";
        rsmd=res.getMetaData();
         int col_size=rsmd.getColumnCount();
         int i;
        // System.out.print((row+1)+"\t");
         for( i=1;i<=col_size;i++) 
         {
    // System.out.println(res.getString(i)+"     ["+(System.currentTimeMillis())+"]");
      //System.out.println(res.getString(i)+",");//   ["+(System.currentTimeMillis())+"]");
      line+=res.getString(i)+",";
   
         }
        // System.out.println(line);
         line+="\n";
         records.add(line);
    
     row++;
    }
    int buf_size=1048576;
   
      ReadWriteFiles.writeBuffered(file_name, records, buf_size);
    con.close();
    
      } //end connection db

 public static void parlogel( java.sql.Statement  stmt  ) throws SQLException , IOException 
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
    stmt.execute ("SET hive.exec.parlogel=true");
  //  stmt.execute ("SET hive.exec.mode.local.auto=true");
 /**/   stmt.execute ("SET hive.exec.reducers.bytes.per.reducer=1000000000");
    stmt.execute ("SET hive.mapjoin.smlogtable.filesize=1000000000");
/* */   stmt.execute ("SET hive.auto.convert.join.noconditionaltask.size=25000000");/*/
  //*/  stmt.execute ("SET hive.hadoop.supports.splittable.combineinputformat=true");
    stmt.execute ("SET hive.mapjoin.optimized.keys=true");
    stmt.execute ("SET hive.mapjoin.lazy.hashtable=true");
   /**/ stmt.execute ("SET hive.exec.parlogel.thread.number=16");
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



    } // end parlogel
}
