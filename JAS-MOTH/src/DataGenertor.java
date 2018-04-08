/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package SharedHive;

import java.io.IOException;
import java.sql.SQLException;

/**
 *
 * @author hadoop
 */
public class DataGenertor {
    
    public static void Histogram_Generator(String table_name) throws SQLException, IOException, InterruptedException, ClassNotFoundException 
    {
       System.out.println("************Histogram_Generator of "+table_name+"******************");

        String sql="";
        // String table_name="";
        
         // load Histogram
         //table_name="My_Histogram_10m";
         sql="drop table "+table_name;
         Connection_DB.Connection_DB_No_Write_ResultFile(sql,"Drop table");
         
         // create  My_Histogram_10m
         sql= "CREATE TABLE "+ table_name+" (table_name     STRING,"
                             +" column_name    STRING,"
                             +" distinct_Value     INT,"
                             +" frequncy  INT"
                             +" )row format delimited"
                             +" fields terminated by ','" ;
                            // +" stored as textfile" +"\n";
         
      // sql="CREATE TABLE My_Histogram_10m (table_name     STRING, column_name    STRING, distinct_Value     INT, frequncy  INT ) row format delimited fields terminated by ',' stored as textfile";

         Connection_DB.Connection_DB_No_Write_ResultFile(sql,"create table");
         
         
           // create  My_Histogram_10m
         sql= "load data local inpath '/home/hadoop/Desktop/Radhya/Benchmark/Histogram/"+table_name+".csv' into table " +table_name;
         Connection_DB.Connection_DB_No_Write_ResultFile(sql,"load data into "+ table_name);
        System.out.println();

    }
    
     public static void lineitem_Generator(String table_name) throws SQLException, IOException, InterruptedException, ClassNotFoundException 
    {
      System.out.println("************lineitem_Generator of "+table_name+"******************");
      String sql="";
     
         sql="drop table "+table_name;
         Connection_DB.Connection_DB_No_Write_ResultFile(sql,"Drop table");
         
         // create  My_Histogram_10m
         sql= "CREATE TABLE "+ table_name
                            +"( L_ORDERKEY    INT, "
                            +" L_PARTKEY     INT,"
                            +" L_SUPPKEY     INT,"
                            +" L_LINENUMBER  INT,"
                            +" L_QUANTITY    double,"
                            +" L_EXTENDEDPRICE  double ,"
                            +" L_DISCOUNT    double,"
                            +" L_TAX         double,"
                            +" L_RETURNFLAG  STRING,"
                            +" L_LINESTATUS  STRING,"
                            +" L_SHIPDATE    STRING,"
                            +" L_COMMITDATE  STRING,"
                            +" L_RECEIPTDATE STRING,"
                            +" L_SHIPINSTRUCT STRING,"
                            +" L_SHIPMODE     STRING,"
                            +" L_COMMENT      STRING)row format delimited "
                            +" fields terminated by ','";
                 
               
         Connection_DB.Connection_DB_No_Write_ResultFile(sql,"create table");
         
         
           // create  lineitem
         sql= "load data local inpath'/home/hadoop/Desktop/Radhya/Benchmark/data_Lineitem/Test_Benchmark/"+table_name+".csv' into table "+table_name;
         Connection_DB.Connection_DB_No_Write_ResultFile(sql,"load data into "+ table_name);
          System.out.println();
         
           
    }
     
     public static void lineitem_Load_Data(String table_name, int size_in_millions) throws SQLException, IOException, InterruptedException, ClassNotFoundException 
    {
      System.out.println("************lineitem_Generator of "+table_name+"******************");
      String sql="";
       int load_times=size_in_millions/10;
       for(int i=1; i<=load_times;i++)
      {    
         //String path= "/home/hadoop/Radhya/ForCloudLab/LaB_Benchmark/lineitem_10m.csv";
         String path="/home/hadoop/ForCloudLab/TestBenchmark/lineitem_10m.csv";//
          sql= "load data local inpath'"+path+"' into table "+table_name;
            
          Connection_DB.Connection_DB_No_Write_ResultFile(sql,"load data into "+ table_name);
          System.out.println("sucessful copy "+(i*10) + " millions");
      }   
     }
      public static void lineitem_Load_Ordered_Data(String table_name, int size_in_millions) throws SQLException, IOException, InterruptedException, ClassNotFoundException 
    {
      System.out.println("************lineitem_Generator of "+table_name+"******************");
      String sql="";
       int load_times=size_in_millions/10;
       for(int i=1; i<=load_times;i++)
      {    
         //String path= "/home/hadoop/Radhya/ForCloudLab/LaB_Benchmark/lineitem_10m.csv";
         String path="/home/hadoop/ForCloudLab/TestBenchmark/lineitem_10m_OR.csv";//
          sql= "load data local inpath'"+path+"' into table "+table_name;
            
          Connection_DB.Connection_DB_No_Write_ResultFile(sql,"load data into "+ table_name);
          System.out.println("sucessful copy "+(i*10) + " millions");
      }   
     }
     
       public static void lineitem_sechema(String table_name) throws SQLException, IOException, InterruptedException, ClassNotFoundException 
    {
      System.out.println("************lineitem_Generator of "+table_name+"******************");
      String sql="";
     
         sql="drop table "+table_name;
         Connection_DB.Connection_DB_No_Write_ResultFile(sql,"Drop table");
         
         // create  My_Histogram_10m
         sql= "CREATE TABLE "+ table_name
                            +"( L_ORDERKEY    INT, "
                            +" L_PARTKEY     INT,"
                            +" L_SUPPKEY     INT,"
                            +" L_LINENUMBER  INT,"
                            +" L_QUANTITY    double,"
                            +" L_EXTENDEDPRICE  double ,"
                            +" L_DISCOUNT    double,"
                            +" L_TAX         double,"
                            +" L_RETURNFLAG  STRING,"
                            +" L_LINESTATUS  STRING,"
                            +" L_SHIPDATE    STRING,"
                            +" L_COMMITDATE  STRING,"
                            +" L_RECEIPTDATE STRING,"
                            +" L_SHIPINSTRUCT STRING,"
                            +" L_SHIPMODE     STRING,"
                            +" L_COMMENT      STRING)row format delimited "
                            +" fields terminated by ','";
                 
               
         Connection_DB.Connection_DB_No_Write_ResultFile(sql,"create table");
    }
     public static void main(String args[]) throws SQLException, IOException, InterruptedException, ClassNotFoundException 
       {
        
         String table_name="";
         System.out.println(" Check Paths of *.csv files(lineitem_10m.csv)  maybe need to change");
       /* 
         // load Histogram
         table_name="My_Histogram_100T";
         Histogram_Generator(table_name);
         table_name="My_Histogram_10m";
         Histogram_Generator(table_name);
         
         table_name="lineitem_100T";
         lineitem_Generator(table_name); 
         
         table_name="lineitem_10m";
         lineitem_Generator(table_name); 
         
         table_name="lineitem_20m";
         lineitem_Generator(table_name);
         
         table_name="lineitem_30m";
         lineitem_Generator(table_name); 
         
         table_name="lineitem_40m";
         lineitem_Generator(table_name); 
         
         table_name="lineitem_50m";
         lineitem_Generator(table_name); 
*/
         lineitem_sechema("lineitem_1B");
         lineitem_Load_Data("lineitem_1B", 1000);
         // 
         //lineitem_sechema("lineitem_100m_OR");
         //lineitem_Load_Ordered_Data("lineitem_100m_OR", 100);
       }
    
}