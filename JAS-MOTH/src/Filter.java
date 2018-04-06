package org.apache.flink.examples.java.relational;

import com.google.common.io.Files;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple;

import org.apache.flink.api.java.tuple.Tuple1;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

import org.apache.flink.api.java.tuple.Tuple9;



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
import java.io.BufferedWriter;
import java.io.File;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.String;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.FilterFunction;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;

@SuppressWarnings("serial")
public class Filter {

    public static LinkedList columnssql = new LinkedList<String>();
    public static LinkedList columnswhere = new LinkedList<String>();
    public static LinkedList operation = new LinkedList<String>();
    public static final ArrayList<String> values = new ArrayList<String>();
    ; 
public static Map<String, Integer> map = new HashMap<String, Integer>();
    public static Map<String, Integer> maporder = new HashMap<String, Integer>();
    public static FileWriter fstream;
    public static BufferedWriter out;
    public static BufferedWriter outtempsql;
    public static String DataPath = "/home/hadoop/Desktop/Flink/Dataset/";
    public static ArrayList<String> values1 = new ArrayList<String>();
    public static Query multi_query[] = new Query[50];
    public static int total_query_num = 2;
    public static int start_query_num = 0;
    public static double total_multi_query_run_time = 0;
//public static  double quey_run_time []= new double [50]; 

    public static void main(String[] args) throws Exception {

        Read_multi_query_plan(DataPath + "multi_query_plans/multi_query_plan.sql");

        for (int i = start_query_num; i < total_query_num; i++) {
            // System.out.println(multi_query[i].sql);
            //multi_query[i].degree_of_parallisim=1;
            Perdicate_Query_Flink_Translator(i, multi_query[i], multi_query[i].filter1, multi_query[i].filter2, DataPath);
            /* for(int i=0; i<2; i++`)
            {
            multi_query[i]= new Query();
            multi_query[i].columns.add("l_orderkey");
            multi_query[i].columns.add("l_discount");
            multi_query[i].columns.add("l_shipdate");
            multi_query[i].columns.add("l_commitdate");
            multi_query[i].columns.add("l_receiptdate");
            multi_query[i].input_file_name=DataPath+"q0.csv";
            //multi_query[i].output_file_name=DataPath+"q2.csv";
            multi_query[i].output_file_name=DataPath+"q"+(i+1)+".csv";
            
            multi_query[i].sql="select l_discount,l_orderkey, l_commitdate from q1 where l_discount between 0.02 and 0.04";
            multi_query[i].filter1=0.02;
            multi_query[i].filter2=0.02;
            multi_query[i].fieldDelimiter=",";
            multi_query[i].degree_of_parallisim=1; 
             */
            ;
        }

        Print_multi_query_plan_Report();

    }

// end q1
    public static void Perdicate_Query_Flink_Translator(int query_num, final Query Q, final double filter1, final double filter2, String DataPath) throws Exception {



        String Query_Filename = DataPath + "multi_query_plans/one_query_at_time.sql";
        write_single_query(Q.sql, Query_Filename);
        String args[] = new String[]{Query_Filename};

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
        //parsing
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
            System.out.println("Q" + query_num + ": " + select);
            columnssql.clear();
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


        // start Flink code

        // get mask to order
        String maskorder = "";
        int count1 = 0;
        int flagorder = 0;
        String order1 = null;
        String order2 = null;
        final LinkedList l1 = new LinkedList<String>();
        int columnslineitemsize = Q.columns.size();
        int columnssqlsize = columnssql.size();
        System.out.println("query_num" + query_num + " columnslineitemsize " + columnslineitemsize + " columnssqlsize" + columnssqlsize);
        for (int i = 0; i < columnslineitemsize; i++) {

            for (int j = 0; j < columnssql.size(); j++) {

                order1 = Q.columns.get(i).toString();

                order2 = columnssql.get(j).toString();
                // to case sensative
                order1 = order1.toLowerCase();
                order2 = order2.toLowerCase();
                System.out.println("order1 " + order1 + " order2 " + order2);


                if (order2.matches(order1)) {

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
        System.out.println("maskorder: " + maskorder);

        for (int i = 0; i < l1.size(); i++) {
            maporder.put(l1.get(i).toString(), i);
        }

        String c4;
        for (int i = 0; i < l1.size(); i++) {
            c4 = l1.get(i).toString();
            System.out.println(c4 + " postion=" + maporder.get(c4));
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<Lineitem> Lineitem1 = getLineitemDataSet(env, maskorder, Q.input_file_name, Q.fieldDelimiter);

        Lineitem1 = Lineitem1.filter(new FilterFunction<Lineitem>() {

            @Override
            public boolean filter(Lineitem l) {


                //return  l.f1>=Q.filter1 && l.f1<= Q.filter2;
                return l.f1 >= filter1 && l.f1 <= filter2;

            }
        });
        
       // Lineitem1=Lineitem1.sortPartition(6, Order.ASCENDING);
        
        DataSink<Lineitem> setParallelism = Lineitem1.writeAsCsv(Q.output_file_name, "\n", Q.fieldDelimiter, WriteMode.OVERWRITE).setParallelism(Q.degree_of_parallisim);


        double start_time = 0;
        double end_time = 0;
        //double quey_run_time=0;
        start_time = System.currentTimeMillis() / 1000;
        System.out.println("Submit Jobs to Flink 9");
        env.setParallelism(Q.degree_of_parallisim);
        env.execute();
        end_time = System.currentTimeMillis() / 1000;
        Q.quey_run_time = end_time - start_time;
        System.out.println("run_time " + Q.quey_run_time + " seconds");
        total_multi_query_run_time += Q.quey_run_time;
    }

// end q1
    public static Tuple get_lineitem() {

        return new Lineitem();

    }

    public static interface lineitem {
    }//<editor-fold defaultstate="collapsed" desc="comment">

    //</editor-fold>
    public static class Lineitem extends Tuple9<Double, Double, String, String, String, String, String, String, String> implements lineitem {
    }

    private static DataSet<Lineitem> getLineitemDataSet(ExecutionEnvironment env, String mask, String input_file_name, String fieldDelimiter) {
        DataSet<Lineitem> Lineitem = null;

        Lineitem = (DataSet<Lineitem>) env.readCsvFile(input_file_name).fieldDelimiter(fieldDelimiter).includeFields(mask).ignoreFirstLine().tupleType(get_lineitem().getClass());





        return Lineitem;

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

    public static void Print_multi_query_plan_Report() {
        System.out.println("---------Final Report-----------");
        for (int i = start_query_num; i < total_query_num; i++) {
            String s = "Runtime of Q" + (i + 1) + " " + multi_query[i].quey_run_time;
            s += " input " + multi_query[i].input_file_name + " output " + multi_query[i].output_file_name;
            System.out.println(s);
        }
        System.out.println("Runtime of_multi_query_plan " + total_multi_query_run_time + " seconds");

    }

    public static void Read_multi_query_plan(String Query_Filename) throws IOException, SQLException {
       // String fn=getAbsolutePath()
        
        //String multi_query_plan_content = new String(Files.readAllBytes(Paths.get(Query_Filename)));
       FileInputStream fisTargetFile = new FileInputStream(new File(Query_Filename));

String multi_query_plan_content = IOUtils.toString(fisTargetFile, "UTF-8");
        //System.out.println(multi_query_plan_content);
        String Query[] = multi_query_plan_content.split("----");
        String Query_line_content[] = new String[1000];
        //System.out.println("Q1\n"+Query[0]);
// System.out.println("Q2\n"+Query[1]);

//fill multi-query 
        total_query_num = Query.length - 1;
        System.out.println("Query number" + total_query_num);
        for (int i = 0; i < Query.length - 1; i++) {
            System.out.println("Q" + (i + 1) + "\n" + Query[i + 1]);
            multi_query[i] = new Query();
            //quey_run_time[i]=0;
            multi_query[i].Query();
            multi_query[i].Query_id = 0;
            String Query_content[] = Query[i + 1].split("\n");
            //System.out.println("ParentQuery"+Query_content[0]);

            Query_line_content = Query_content[0].split("=");
            multi_query[i].input_file_name = DataPath + "PlainResults/" + Query_line_content[1].trim() + ".csv";
            Query_line_content = Query_content[1].split("=");
            multi_query[i].output_file_name = DataPath + "PlainResults/" + Query_line_content[1].trim() + ".csv";
            Query_line_content = Query_content[2].split("=");
            multi_query[i].fieldDelimiter = Query_line_content[1].trim();
            Query_line_content = Query_content[3].split("=");
            multi_query[i].filter1 = Double.parseDouble(Query_line_content[1].trim());
            Query_line_content = Query_content[4].split("=");
            multi_query[i].filter2 = Double.parseDouble(Query_line_content[1].trim());
            Query_line_content = Query_content[5].split("=");
            multi_query[i].degree_of_parallisim = Integer.parseInt(Query_line_content[1].trim());
            Query_line_content = Query_content[6].split("=");
            multi_query[i].sql = Query_line_content[1].trim();
            String columnslist[] = Query_content[7].split(",");
            System.out.println("columnslist.length " + columnslist.length);
            //fill columnslist
            for (int j = 1; j < columnslist.length; j++) //System.out.println("Q1_Parent_name.."+Query_line_content[1]);
            {
                multi_query[i].columns.add(columnslist[j].trim());
            }
            //System.out.println(multi_query[i].columns); 
        } // end for i
        //System.out.println(multi_query[1].sql);

    }  // end function      

    public static void write_single_query(String sql, String Query_Filename) throws IOException, SQLException {
        System.out.println("write_single_query");
        fstream = new FileWriter(Query_Filename, false);
        outtempsql = new BufferedWriter(fstream);
        outtempsql.write(sql);
        // outtempsql.write("\n");
        outtempsql.flush();
        outtempsql.close();


    }
}