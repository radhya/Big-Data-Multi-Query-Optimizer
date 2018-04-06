package PIMQO_System;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TBaseType;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.nodes.*;
import gudusoft.gsqlparser.stmt.*;

import java.io.*;
import java.sql.*;

import gudusoft.gsqlparser.nodes.TResultColumnList;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author hadoop
 */
public class query_tokens {

	public  String query="";
	public  String table_name ="";
	public int columns_num=0;
	public  int rows=0;
	//public TResultColumnList columns = null;
	public  String order_clauses ="";
	public  String groupby_clauses ="";
	public  String Having_clauses ="";
	public  String join_clauses ="";
	public  List <String>  join_tables_list =new ArrayList <String> ();  
	public  List <String>  columns_list =new ArrayList <String> ();  
	public   double TupleSize=0; 
	public   double datasize_in_MB=0;
	public   double Hive_estimated_cost=0; // this for sequetail cost
	public   double estimated_cost=0; // this to estimate it as parent
	public   double HiveReused_estimated_cost=0; // this after upadte its parent
	public   double LeftOperand=0;
	public   double RightOperand=0;

	//  public  List <String>  where_clauses =new ArrayList <String> ();  
	//public  List<String> columns_list=new List<String>();
	public  String  where_clauses="";//; =new String [50];
	//public  LinkedList<String> where_clauses=new LinkedList<String>();
	//public  LinkedList<String> join_clauses=new LinkedList<String>();

	//public  List<List<String>> tempresult = new ArrayList<List<String>>();
	public  void query_tokens()
	{
		this.table_name ="";
		this.query="";
		this.rows=0;
		this.columns_num=0;
		this.groupby_clauses ="";
		this.Having_clauses ="";
		this.order_clauses="";
		this.where_clauses="";
		this.join_clauses="";
		 
	}
	public  void print_query_tokens()
	{
		System.out.println("print_query_tokens-----------------");
		System.out.println("Query: "+this.query);
		System.out.println("table_name: "+this.table_name);
		System.out.println("columns_num:"+this.columns_num);
		System.out.println("columns_list \t:");
		for(int i=0; i<this.columns_list.size();i++)
			System.out.print(this.columns_list.get(i) +",\t");  

		System.out.println();
		//  if(this.where_clauses.size()!=0)
		System.out.println("where_clauses: "+this.where_clauses);//.get(0));
		// else
		// System.out.println("where_clauses: No where_clauses");

		System.out.println("groupby_clauses: "+this.groupby_clauses);
		System.out.println("order_clauses: "+this.order_clauses);
		System.out.println("Having_clauses: "+this.Having_clauses);
		System.out.println("End query_tokens -----------------");


	}

	public  void get_TupleSize()
	{
		Meta_Data MD= new Meta_Data();
		MD.Init_MetaData();
		this.TupleSize=MD.calculate_TupleSize(this.columns_list);
		//return this.TupleSize;
	}

	public  int  get_column_num(String column_name)
	{
		return this.columns_list.indexOf(column_name); 
	}

	public  boolean  compare_groupby(query_tokens qt)
	{
		return (this.groupby_clauses.equalsIgnoreCase(qt.groupby_clauses));
	}



	public  void  parsing(String sql, File sqlfile) throws SQLException, IOException 
	{


		/*************        parsing            ***************/   
		//  if(!found_before) 
		// {
		String tableName;

		String d = null;String ordercol = null; String groupcol = null; String joincond = null;

		TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvmssql);
		sqlparser.sqlfilename=sqlfile.getName();
		if(sqlparser.parse()!=0)
		{
			System.out.println("Syntax Error:");                
			System.out.println(sqlparser.getErrormessage());
			return;
		}
		TCustomSqlStatement TCsql= sqlparser.sqlstatements.get(0);
		// System.out.println("TCsql"+TCsql);

		TTable table;
		TResultColumnList columns = null;
		LinkedList<String> cols=new LinkedList<String>();
		LinkedList<String> tabs=new LinkedList<String>();

		TObjectName x,y = null;
		String fun = null;//,tabl;
		if(TCsql instanceof TSelectSqlStatement)
		{
			TSelectSqlStatement select = (TSelectSqlStatement)TCsql; 
			select.getStatements();
			columns =select.getResultColumnList();
			for(int i=0;i<columns.size();i++)
			{
				if (!sql.contains("join")){
					cols.add(columns.elementAt(i).toString());
					//System.out.println("columns :"+columns.elementAt(i).toString());
					//here take columns
				}
				else{ cols.add(columns.getResultColumn(i).getEndToken().toString());

			 

				System.out.println("columnccccs :"+columns.getResultColumn(i).getEndToken().toString());}
				//  coll=columns.elementAt(i).toString();
			}          
			 	for(int j=0;j<select.tables.size();j++)            
			{
				table = select.tables.getTable(j);
				tabs.add(table.getName());
				// System.out.println("Table Name:"+table.getName());
				this.table_name=table.getName();
				 
			}
			// here Where Clause
			if (select.getWhereClause()!=null )
			{

				//System.out.println("getHierarchicalClause"+select.getHierarchicalClause());

				x=select.getWhereClause().getCondition().getLeftOperand().getObjectOperand();
 
				y=select.getWhereClause().getCondition().getRightOperand().getObjectOperand();
			 
				d=select.getWhereClause().getCondition().toString(); 
				this.LeftOperand= Double.parseDouble((select.getWhereClause().getCondition().getLeftOperand().toString()));
				this.RightOperand= Double.parseDouble((select.getWhereClause().getCondition().getRightOperand().toString()));

				this.where_clauses=d;
				 



			}
			// order by
			if (select.getOrderbyClause()!=null)
			{
				int s=0;
				s=select.getOrderbyClause().getItems().size();
				for (int i=0;i<s;i++)
				{
					//cols.add(select.getOrderbyClause().getItems().elementAt(i).toString());
					System.out.println("columns:"+select.getOrderbyClause().getItems().elementAt(i).toString());
					ordercol=select.getOrderbyClause().getItems().elementAt(i).toString();
					System.out.println(" order by " + ordercol);
				}
				this.order_clauses=select.getOrderbyClause().toString();
			}

			// Group by clauses
			if (sql.contains("group by")){
				if (select.getGroupByClause() != null) 
				{
					System.out.println("Tables:"+select.getGroupByClause().getItems());
					this.groupby_clauses=select.getGroupByClause().toString();

					int s=0;
					s=select.getGroupByClause().getItems().size();
					if(s==0)
					{
						//  tabs.add(select.getGroupByClause().getItems().elementAt(s).toString());
						groupcol=select.getGroupByClause().getItems().elementAt(s).toString();
					}
					else
					{
						for(int k=0;k<s;k++)
						{
							// tabs.add(select.getGroupByClause().getItems().elementAt(k).toString());
							System.out.println("Tables:"+select.getGroupByClause().getItems().elementAt(k).toString());
							groupcol=select.getGroupByClause().getItems().elementAt(k).toString();
						}
					}

				}
			}
			// Having

			if (sql.contains("having")){
				if (select.getGroupByClause().getHavingClause()!=null)
				{
					if(select.getGroupByClause().getHavingClause().getComparisonOperator()==null )
					{
						fun=select.getGroupByClause().getHavingClause().getFunctionCall().toString();
						System.out.println( "function is :"+fun.toString());

					}
					else
					{

						fun=select.getGroupByClause().getHavingClause().getLeftOperand().getFunctionCall().toString();
						fun=fun.concat(select.getGroupByClause().getHavingClause().getComparisonOperator().toString());
						fun=fun.concat(select.getGroupByClause().getHavingClause().getRightOperand().toString());
						System.out.println("function is :"+fun);
						// System.out.println( "function is :"+fun.toString());
					}
				}

				//    System.out.println( "function is :"+select.getGroupByClause().getHavingClause().getExprList().toString());
			}

			/***    join parser   ***/
			if(sql.contains("join")){
				System.out.println(select.joins.toString()); 
				this.join_clauses=select.joins.toString();
				for(int i=0;i<select.joins.size();i++)
				{
					TJoin join = select.joins.getJoin(i);
					switch (join.getKind()){
					case TBaseType.join_source_fake:
						System.out.printf("join_source_fake >>table: %s, alias: %s\n",join.getTable().toString(),(join.getTable().getAliasClause() !=null)?join.getTable().getAliasClause().toString():"");

						/*1*/      //          alias1=(join.getTable().getAliasClause() !=null)?join.getTable().getAliasClause().toString():"";System.out.println("ana=  "+alias1);
						break;
					case TBaseType.join_source_table:
						System.out.printf("join_source_table\n>>table: %s, alias: %s\n",join.getTable().toString(),(join.getTable().getAliasClause() !=null)?join.getTable().getAliasClause().toString():"");
						/*2*/          //        alias2=(join.getTable().getAliasClause() !=null)?join.getTable().getAliasClause().toString():"";System.out.println("aka=  "+alias2);
						join_tables_list.add(join.getTable().toString());
						System.out.println("source join_table..."+this.join_tables_list.get(0));
						for(int j=0;j<join.getJoinItems().size();j++){
							TJoinItem joinItem = join.getJoinItems().getJoinItem(j);
							System.out.printf("Join type: %s\n",joinItem.getJoinType().toString());
							System.out.printf("table: %s, alias: %s\n",joinItem.getTable().toString(),(joinItem.getTable().getAliasClause() !=null)?joinItem.getTable().getAliasClause().toString():"");
							join_tables_list.add(joinItem.getTable().toString());
							System.out.println("join_tables_list..."+this.join_tables_list.get(j+1));
							/*3*/              //          alias3=(joinItem.getTable().getAliasClause() !=null)?joinItem.getTable().getAliasClause().toString():"";System.out.println("ala=  "+alias3);

							if (joinItem.getOnCondition() != null){
								System.out.printf("On: %s\n",joinItem.getOnCondition().toString());

								joincond=joinItem.getOnCondition().toString();

							}else  if (joinItem.getUsingColumns() != null){
								System.out.printf("using: %s\n",joinItem.getUsingColumns().toString());
							}
						}
						break;
					case TBaseType.join_source_join:
						TJoin source_join = join.getJoin();
						System.out.printf("table: %s, alias: %s\n",source_join.getTable().toString(),(source_join.getTable().getAliasClause() !=null)?source_join.getTable().getAliasClause().toString():"");
						/*4*/ //alias4=(source_join.getTable().getAliasClause() !=null)?source_join.getTable().getAliasClause().toString():"";System.out.println("amma=  "+alias4);
						for(int j=0;j<source_join.getJoinItems().size();j++){
							TJoinItem joinItem = source_join.getJoinItems().getJoinItem(j);
							System.out.printf("source_join type: %s\n",joinItem.getJoinType().toString());
							System.out.printf("table: %s, alias: %s\n",joinItem.getTable().toString(),(joinItem.getTable().getAliasClause() !=null)?joinItem.getTable().getAliasClause().toString():"");
							/*5*/                  //   alias5=(joinItem.getTable().getAliasClause() !=null)?joinItem.getTable().getAliasClause().toString():"";System.out.println("ammmnaa=  "+alias5);   
							if (joinItem.getOnCondition() != null){
								System.out.printf("On: %s\n",joinItem.getOnCondition().toString());

								joincond=joinItem.getOnCondition().toString();

							}else  if (joinItem.getUsingColumns() != null){
								System.out.printf("using: %s\n",joinItem.getUsingColumns().toString());
							}
						}

						for(int j=0;j<join.getJoinItems().size();j++){
							TJoinItem joinItem = join.getJoinItems().getJoinItem(j);
							System.out.printf("Join type: %s\n",joinItem.getJoinType().toString());
							System.out.printf("table: %s, alias: %s\n",joinItem.getTable().toString(),(joinItem.getTable().getAliasClause() !=null)?joinItem.getTable().getAliasClause().toString():"");

							/*6*/            //      alias6=(join.getTable().getAliasClause() !=null)?join.getTable().getAliasClause().toString():"";System.out.println("aaa=  "+alias6);
							if (joinItem.getOnCondition() != null){
								System.out.printf("On: %s\n",joinItem.getOnCondition().toString());

								joincond=joinItem.getOnCondition().toString();

							}else  if (joinItem.getUsingColumns() != null){
								System.out.printf("using: %s\n",joinItem.getUsingColumns().toString());
							}
						}

						break;
					default:
						System.out.println("unknown type in join!");
						break;
					}
				}
			}
		}


		// get all columns_list into querytoken.columslist
		for(String s : cols )
		{
			//system.out.println("final columns :"+s);  
			this.columns_list.add(s);
		}
		this.columns_num=this.columns_list.size();
	}
	/*  End  of parser*/




	public  boolean issubset( query_tokens qt) 
	{
		boolean subset=false;
		boolean check_table_name=false;
		boolean check_columns=false;

		if(this.table_name.equals(qt.table_name))   
			check_table_name=true;

		//int min=Math.min(this.columns_num, qt.columns_num);

		for(int i=0; i<qt.columns_num;i++)

		{
			if(this.columns_list.toString().contains(qt.columns_list.get(i)))

			{
				check_columns=true;
				System.out.println("true same column  \t"+qt.columns_list.get(i));
			} 
			else
			{
				check_columns=false;
				System.out.println("flase diffrent column \t"+qt.columns_list.get(i));

			}
			if(!check_columns)  break;
		} //end for  
		  
		subset=check_table_name &&check_columns;
		return subset;
	}// end issubset

	// public   List<String> get_distinct_values() 




}// end class


