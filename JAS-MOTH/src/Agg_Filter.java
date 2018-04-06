/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.flink.examples.java.relational;

/**
 *
 * @author hadoop
 */


 

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;



import java.util.*;
import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.nodes.IExpressionVisitor;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TParseTreeNode;
import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.nodes.TTable;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import java.io.File;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.core.fs.FileSystem.WriteMode;

@SuppressWarnings("serial")
public class Agg_Filter {

	
public static LinkedList columnssql=new LinkedList<String>();    
public static LinkedList columnswhere=new LinkedList<String>(); 
public static LinkedList operation=new LinkedList<String>(); 
public static final ArrayList<String> values = new ArrayList<String>(); ; 
public static  Map<String, Integer> map = new HashMap<String, Integer>();
public static  Map<String, Integer> maporder = new HashMap<String, Integer>();   
 
public static ArrayList<String> values1 = new ArrayList<String>();
public static  void main(String[] args) throws Exception {
      
    
       LinkedList columnspartsupp = new LinkedList<String>();
       LinkedList columnslineitem = new LinkedList<String>();
       final  Map<String, Integer> map = new HashMap<String, Integer>();
       final  Map<String, Integer> maporder = new HashMap<String, Integer>();  
       
     //// columns of table order
        columnslineitem.add("l_orderkey");
        columnslineitem.add("l_partkey");
        columnslineitem.add("l_suppkey");
        columnslineitem.add("l_linenumber");
        columnslineitem.add("l_quantity");
        columnslineitem.add("l_extendedprice");
        columnslineitem.add("l_discount");
        columnslineitem.add("l_tax");
        columnslineitem.add("l_returnflag");
        columnslineitem.add("l_linestatus");
        columnslineitem.add("l_shipdate");
        columnslineitem.add("l_commitdate");
        columnslineitem.add("l_receiptdate");
        columnslineitem.add("l_shipinstruct");
        columnslineitem.add("l_shipmode");
        columnslineitem.add("l_comment ");
        
        
        //file query
        args = new String[]{"/home/hadoop/Desktop/Flink/Dataset/query.sql"};
        System.out.println("--------------------------------------------------------------------");
        System.out.println (args.toString());
        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvmssql);

        // get file
        if (args.length != 1) {
            System.out.println("Usage: java checksyntax test.sql");
            return;
        }
        // this file found or not
        File file = new File(args[0]);
        if (!file.exists()) {
            System.out.println("File not exists:" + args[0]);
            return;
        }
        //extract from sql statment
        sqlparser.sqlfilename = args[0];
        if (sqlparser.parse() != 0) {
            System.out.println("Syntax Error:");
            System.out.println(sqlparser.getErrormessage());
            return;
        }
        TCustomSqlStatement TCsql = sqlparser.sqlstatements.get(0);
        TTable table;
        TResultColumnList columns;
        
        if (TCsql instanceof TSelectSqlStatement) {
            TSelectSqlStatement select = (TSelectSqlStatement) TCsql;
            select.getStatements();
            columns = select.getResultColumnList();
            for (int i = 0; i < columns.size(); i++) {
               // System.out.println("columns :" + columns.elementAt(i).toString());
                columnssql.add(columns.elementAt(i).toString());
            }
            int size = 0;
            size = select.tables.size();
            for (int i = 0; i < size; i++) {
                table = select.tables.getTable(i);
                //System.out.println("Table (" + i + ") =" + table);
            }
            if (select.getWhereClause() != null) {

                WhereCondition w = new WhereCondition(sqlparser.sqlstatements.get(0).getWhereClause().getCondition());
                w.printColumn();
            }
            if (select.getOrderbyClause() != null) {
                System.out.println("order by");
                for (int i = 0; i < select.getOrderbyClause().getItems().size(); i++) {

                    System.out.println(select.getOrderbyClause().getItems().getOrderByItem(i).getSortKey());
                    columnssql.add(select.getOrderbyClause().getItems().getOrderByItem(i).getSortKey());
                }

            }


     }
        for(int i=0;i<columnssql.size();i++)
            System.out.println(columnssql.get(i));
// get mask to Partsupp
  for (int j = 0; j < columnssql.size(); j++) 
        System.out.println(columnssql.get(j).toString());

        String mask = "";
        int count = 0;
        int flag = 0;
        String c1 = null;
        String c2 = null;
        final LinkedList l = new LinkedList<String>();
        for (int i = 0; i < columnspartsupp.size(); i++) {

            for (int j = 0; j < columnssql.size(); j++) {

                c1 = columnspartsupp.get(i).toString();
                
                c2 = columnssql.get(j).toString();
                if (c2.matches(c1)) {

                    flag = 1;
                    count++;
                    l.add(c1);
                    break;


                } else {
                    flag = 0;
                 
                }

            }
            if (flag == 1) {
                mask = mask + "1";

            } else {
                mask = mask + "0";
            }

        }
     System.out.println(mask);

        for (int i = 0; i < l.size(); i++) {
            map.put(l.get(i).toString(), i);
        }
        String c3;
        for (int i = 0; i < l.size(); i++) {
            c3 = l.get(i).toString();
            
            System.out.println(c3 + " postion=" + map.get(c3));
        }

    // get mask to order
        String maskorder = "";
        int count1 = 0;
        int flagorder = 0;
        String order1 = null;
        String order2 = null;
        final  LinkedList l1 = new LinkedList<String>();
        for (int i = 0; i < columnslineitem.size(); i++) {

            for (int j = 0; j < columnssql.size(); j++) {

                order1 = columnslineitem.get(i).toString();
           
                order2= columnssql.get(j).toString();
                 //order1 = order1.toLowerCase();
                //order2 = order2.toLowerCase();
               
                if (order2.equalsIgnoreCase( order1)) {

                    flagorder = 1;
                    count1++;
                    l1.add(order1);
                    break;


                } else {
                    flagorder = 0;
                   
                }

            }
            if (flagorder == 1) {
                maskorder = maskorder + "1";

            } else {
                maskorder = maskorder + "0";
            }

        }
      System.out.println(maskorder);

        for (int i = 0; i < l1.size(); i++) {
            maporder.put(l1.get(i).toString(), i);
        }    
        
  String c4;
        for (int i = 0; i < l1.size(); i++) {
            c4 = l1.get(i).toString();
            System.out.println(c4 + " postion=" + maporder.get(c4));
    }
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


    

    DataSet<Lineitem> Lineitem1= getLineitemDataSet(env,maskorder);  
   // Lineitem= Lineitem.aggregate(Aggregations.MAX,0).first(1);
    
       
      
       
       Lineitem1 = Lineitem1.filter(
            new FilterFunction<Lineitem>() {
                    @Override
                    public boolean filter(Lineitem l) {
                    
                   
                        return  Double.parseDouble( l.getField(0).toString())>=0.01 && Double.parseDouble( l.getField(0).toString())<=0.06  ;
                         
                    }
            });//.groupBy(0).aggregate(Aggregations.SUM, 1); // group by
       //select l_discount,l_tax, sum(l_tax) from lineitem where l_discount between 0.01 and 0.06 group by l_discount order by l_discount

       //.groupBy(0);//aggregate(Aggregations.SUM, 1).groupBy(0);
        Lineitem1=Lineitem1.sortPartition(0,Order.ASCENDING).setParallelism(1); // single-level order by
        //Lineitem1=Lineitem1.sortPartition(1,Order.ASCENDING).setParallelism(1); // multi-level order by

        Lineitem1.writeAsCsv("/home/hadoop/Desktop/Flink/Dataset/PlainResults/t1.csv", "\n", ",",WriteMode.OVERWRITE).setParallelism(1);
       env.execute();
  
 
    
}



public static  Tuple  get_Partsupp()
        {
            
                return new Partsupp();
          
        }

        public static interface Partsupp1
        {}

        public static class Partsupp extends Tuple2<Double,Double> implements Partsupp1{
		 
	}
             
        private static DataSet<Partsupp> getPartsuppDataSet(ExecutionEnvironment env,String mask) {
		DataSet<Partsupp> Partsupp=(DataSet<Partsupp>)env.readCsvFile("/home/hadoop/Desktop/Flink/Dataset/partsupp.csv")
					.fieldDelimiter('|')
                                        .includeFields(mask).ignoreFirstLine()
                                        .tupleType(get_Partsupp().getClass());
          
                return  Partsupp;
	}
        
       public static  Tuple  get_Lineitem()
        {
            
                return new Lineitem();
          
        }
        public static interface lineitem
        {}
        
        
        public static class Lineitem extends Tuple2<Double,Double> implements lineitem {

        private static Object sortPartition(int i, Order order) {
            throw new UnsupportedOperationException("Not yet implemented");
        }
            
		
	}
    
       private static DataSet<Lineitem> getLineitemDataSet(ExecutionEnvironment env,String mask) {
       DataSet<Lineitem> Lineitem=(DataSet<Lineitem>) env.readCsvFile("/home/hadoop/Desktop/Flink/Dataset/PlainResults/LINEITEM_NUD_6m.csv")

       //DataSet<Lineitem> Lineitem=(DataSet<Lineitem>) env.readCsvFile("/home/hadoop/Desktop/Flink/Dataset/PlainResults/t1.csv")
      .fieldDelimiter(',')
      .includeFields(mask).ignoreFirstLine()
      .tupleType(get_Lineitem().getClass());
       
      
      return Lineitem;
      }
     public static class Result extends Tuple5<Double,Double,Double,Double,Double>  {
		
             public Result(){};
             public Result(Double c1,Double c2,Double o1,Double o2,Double o3)
             {
                 this.f0=c1;
                 this.f1=c2;
                 this.f2=o1;
                 this.f3=o2;
                 this.f4=o3;
           
             };
	}
        public static class WhereCondition implements IExpressionVisitor {

        private TExpression condition;

        public WhereCondition(TExpression expr) {
            this.condition = expr;
        }

        public void printColumn() {
            this.condition.inOrderTraverse(this);
        }

        boolean is_compare_condition(EExpressionType t) {
            return ((t == EExpressionType.simple_comparison_t)
                    || (t == EExpressionType.group_comparison_t) || (t == EExpressionType.in_t));
        }

        public boolean exprVisit(TParseTreeNode pnode, boolean pIsLeafNode) {
            // > < >= <= == != operation
            TExpression lcexpr = (TExpression) pnode;
            if (is_compare_condition(lcexpr.getExpressionType())) {
                TExpression leftExpr = (TExpression) lcexpr.getLeftOperand();

                System.out.println("column: " + leftExpr.toString());
                columnssql.add(leftExpr.toString().toString());
                columnswhere.add(leftExpr.toString().toString());
                if (lcexpr.getComparisonOperator() != null) {
                    System.out.println("Operator: "
                            + lcexpr.getComparisonOperator().astext);
                    operation.add(lcexpr.getComparisonOperator().astext);
                }
                System.out.println("value: "
                        + lcexpr.getRightOperand().toString());
                values.add(lcexpr.getRightOperand().toString());
             //   columnssql.add(lcexpr.getRightOperand().toString());
                System.out.println("");

            }
     
           
            // and operation
            if (lcexpr.getExpressionType().toString() == "logical_and_t") {
               // System.out.println("and operation : " + lcexpr.getOperatorToken().toString());
                 //columnssql.add(lcexpr.getOperatorToken().toString());

            }
            // or operation 
            if (lcexpr.getExpressionType().toString() == "logical_or_t") {
             //   System.out.println("OR operation : " + lcexpr.getOperatorToken().toString());
                 //columnssql.add(lcexpr.getOperatorToken().toString());
            }

            // like operation
            if (lcexpr.getExpressionType().toString() == "pattern_matching_t") {
                System.out.println("Like operation : " + lcexpr.getOperatorToken().toString());

                TExpression leftExpr = (TExpression) lcexpr.getLeftOperand();

                System.out.println("column value: " + leftExpr.toString());
                columnssql.add(leftExpr.toString());

                System.out.println("value: "
                        + lcexpr.getRightOperand().toString());
                System.out.println("");

            }
            return true;
        }
    }
}
