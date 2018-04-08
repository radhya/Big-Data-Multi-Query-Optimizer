/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package SharedHive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author hadoop
 */
public class Meta_Data{

    public Map<String, Double> columns_MetaData = new HashMap<String, Double>();

    public void Init_MetaData() {
         
        columns_MetaData.put("L_PARTKEY", 6.44653);      //INT
        columns_MetaData.put("L_SUPPKEY", 6.44653);      //INT
      
        columns_MetaData.put("L_SUPPKEY", 6.44653);      //INT
        columns_MetaData.put("L_ORDERKEY", 5.88901);      //INT
        columns_MetaData.put("L_DISCOUNT", 4.86949);     //double
        //columns_MetaData.put("L_DISCOUNT", 0.16949);
        columns_MetaData.put("L_TAX", 4.88964);           //double
        columns_MetaData.put("L_SHIPDATE", 9.93926);      //STRING
        columns_MetaData.put("L_COMMITDATE", 9.93779);    //STRING
        columns_MetaData.put("L_COMMENT", 9.93779);       //STRING
        columns_MetaData.put("L_LINENUMBER", 2.0);        //INT
       
        columns_MetaData.put(" L_QUANTITY", 4.82003);      //double
        columns_MetaData.put(" L_EXTENDEDPRICE", 8.60192); //double
        columns_MetaData.put(" L_RETURNFLAG", 2.0);    //STRING
        columns_MetaData.put(" L_LINESTATUS", 2.0);    //STRING
        columns_MetaData.put(" L_RECEIPTDATE", 9.94238);   //STRING
        columns_MetaData.put(" L_SHIPINSTRUCT", 13.01082);  //STRING
        columns_MetaData.put(" L_SHIPMODE", 5.2876);      //STRING
     
        //L_RETURNFLAG ,L_LINESTATUS,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE
/*
 *                           L_ORDERKEY    INT,
                             L_PARTKEY     INT,
                             L_SUPPKEY     INT,
                             L_LINENUMBER  INT,
                             L_QUANTITY    double,
                             L_EXTENDEDPRICE  double ,
                             L_DISCOUNT    double,
                             L_TAX         double,
                             L_RETURNFLAG  STRING,
                             L_LINESTATUS  STRING,
                             L_SHIPDATE    STRING,
                             L_COMMITDATE  STRING,
                             L_RECEIPTDATE STRING,
                             L_SHIPINSTRUCT STRING,
                             L_SHIPMODE     STRING,
                             L_COMMENT      STRING
 */
    }
    
     public double calculate_TupleSize(List <String>  columns_list) {
         double TupleSize=0; 
         for(int i=0; i<columns_list.size();i++)
          {
              System.out.print(columns_list.get(i)+", ");
              String coloum_name=columns_list.get(i).toUpperCase();
              if(columns_MetaData.get(coloum_name)!=null)
              TupleSize+=columns_MetaData.get(coloum_name);
          }
         System.out.println();
          return TupleSize;
     }
      public static void main(String[] args )
      {
        List <String>  columns_list=new ArrayList <String> ();  
        columns_list.add("L_PARTKEY");
         columns_list.add("L_TAX");
        //System.out.println(columns_list.get(0));

        Meta_Data MD= new Meta_Data();
        MD.Init_MetaData();
        double TupleSize=MD.calculate_TupleSize(columns_list);
        System.out.println(TupleSize);
      }

     
}