
package SharedHive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author hadoop
 */
public class Connection_to_DB {
  public static double  Connection_DB_No_Write_ResultFile  ( String sql, String connection_name) throws SQLException, ClassNotFoundException, IOException
      {
      //replace "hive" here with the name of the user the queries should run as
   
    
   
   Connection con ;
  String driverName = "org.apache.hive.jdbc.HiveDriver";
  java.sql.Statement stmt;
   
   
 Class.forName(driverName);
     con  = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
     
     stmt = con.createStatement();
    
      //parlogel(stmt);
      System.out.println(" In Connection_DB_No_Write_ResultFile in "+  connection_name);
     System.out.println(" Running:" +sql);
    
      double start_time=0;
      double end_time=0;
      double query_run_time=0;

      
      if(sql.contains("create"))
   {
       int indexof_ASor_openbracket=sql.indexOf("as");
       if(indexof_ASor_openbracket==-1) indexof_ASor_openbracket=sql.indexOf("(");
       String s=sql.substring(13,indexof_ASor_openbracket);
      // s="t1t22";
        //System.out.println(s);
       stmt.execute(" drop table "+s.trim());
   }
if( sql.contains("CREATE"))
   {
       int indexof_ASor_openbracket=sql.indexOf("AS");
       if(indexof_ASor_openbracket==-1) indexof_ASor_openbracket=sql.indexOf('(');
       String s=sql.substring(13,indexof_ASor_openbracket);
      
      // s="t1t22";
        //
       
      // System.out.println("indexof_ASor_openbracket"+indexof_ASor_openbracket+ s);
       stmt.execute(" drop table "+s.trim());
   }
    //stmt.execute(Excutor.get_subquery(sql));
    start_time=System.currentTimeMillis()/1000;
    stmt.execute(sql);
    end_time=System.currentTimeMillis()/1000;
    query_run_time=end_time-start_time;
    System.out.println("query_run_time of "+ connection_name +" = "+query_run_time+ "..."+sql);  
   
    
   // for( int i=1;i<=2;i++) 
  /* 
    sql="drop table t1";
    start_time=System.currentTimeMillis()/1000;
    stmt.execute(sql);
    sql="create table t1 as select l_discount,L_ORDERKEY,  L_SHIPDATE  from lineitem_100t_old where l_discount between 0.01 and 0.02";
    stmt.execute(sql);
    end_time=System.currentTimeMillis()/1000;
    query_run_time=end_time-start_time;
    System.out.println("query_run_time of "+ connection_name +" = "+query_run_time+ "..."+sql);  
   
    sql="drop table t2";
    start_time=System.currentTimeMillis()/1000;
    stmt.execute(sql);
    sql="create table t2 as select l_discount,L_ORDERKEY,  L_SHIPDATE  from lineitem_100t_old where l_discount between 0.01 and 0.04";
    stmt.execute(sql);
    end_time=System.currentTimeMillis()/1000;
    query_run_time=end_time-start_time;
    System.out.println("query_run_time of "+ connection_name +" = "+query_run_time+ "..."+sql);  

    */
    con.close();
     return query_run_time;
      } //end Connection_DB_No_Write_ResultFile
  
  
  
  public static double  Connection_DB_Write_ResultFile  ( String sql, String file_name) throws SQLException, ClassNotFoundException, IOException
      {
      //replace "hive" here with the name of the user the queries should run as
   
    
   
   Connection con ;
  String driverName = "org.apache.hive.jdbc.HiveDriver";
  java.sql.Statement stmt;
  double start_time=0;
  double end_time=0;
  double query_run_time=0; 
  
  System.out.println(" In Connection_DB_Write_ResultFile");

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
    
    start_time=System.currentTimeMillis()/1000;
    
    
  //con.setAutoCommit(false);
   // System.out.println("AutoCommit "+con.getAutoCommit());
    ResultSet res = stmt.executeQuery(sql);
    end_time=System.currentTimeMillis()/1000;
    query_run_time=end_time-start_time;
    System.out.println("query_run_time   "+ query_run_time); 
    
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
    return query_run_time;
      } //Connection_DB_Write_ResultFile
  
  
  
   public static void main(String args[]) throws SQLException, IOException, ClassNotFoundException 
 {
     String sql="";
     sql="create table tt1 as select l_discount, L_ORDERKEY from t6 where l_discount = 0.01;\n";
     sql+= " create table tt2 as select l_discount, L_ORDERKEY from t6 where l_discount = 0.02;";
    sql="create table li4_5 as select *from lineitem_100t where l_discount between 0.04 and 0.05";
    
    sql= "CREATE TABLE test2 AS SELECT log.L_ORDERKEY, log.L_DISCOUNT FROM "+
     // " ( SELECT l1.L_ORDERKEY, l1.L_DISCOUNT FROM   lineitem_10m  l1  where l1.L_DISCOUNT  between 0.01 and 0.03 "+
     " ( SELECT l1.L_ORDERKEY, l1.L_DISCOUNT FROM   li1_3  l1 "+
   
            " UNION ALL"+
            " SELECT l2.L_ORDERKEY, l2.L_DISCOUNT FROM  li4_5 l2 ) log";
  // sql="create table NDQ4 as select l_discount, L_SHIPDATE, L_LINESTATUS, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE from NDQ where l_discount between 0.08 and 0.08";Connection_DB_No_Write_ResultFile(sql, "con-2");
  // sql="create table t12 as select l_discount, L_SHIPDATE, L_LINESTATUS from t3 where l_discount between 0.03 and 0.05";Connection_DB_No_Write_ResultFile(sql, "con-2");
  sql="Select  l.l_orderkey,  sum(l.l_extendedprice  *  (1  -  l.l_discount)) as "+  
 " revenue,  o.o_orderdate, o.o_shippriority "+
" From customer c, orders o, lineitem_100t l "+
 " Where c.c_mktsegment = 'BUILDING' "+
 " and c.c_custkey = o.o_custkey "+
" and l.l_orderkey = o.o_orderkey "+
" and o.o_orderdate < ' 1995-03-15' "+
 " and l.l_shipdate > '1995-03-15' ";
   Connection_DB_No_Write_ResultFile(sql, "con-2");
 
     sql="select l_discount, count(l_discount) from lineitem_10m group by l_discount order by l_discount";
    // sql="show tables";
   // Connection_DB_Write_ResultFile(sql,"test_lineitem_10m.csv");
     
    
 }
}
