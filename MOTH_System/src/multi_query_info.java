package PIMQO_System;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author hadoop
 */



public  class multi_query_info
{
	public  int query_num=0;
	public  int rows=0;
	public  int columns=0;
	public  String query_type ="";//{shared, non-shared}
	public  String orginal_query ="";
	public  String new_query ="";
	public  String matarlized_view ="";// {yes,no}

	public  double estimated_time=0;

	public  void multi_query_info()
	{
		this.query_num=0;  
		this.rows=0;
		this.columns=0;
		this.query_type ="";
		this.orginal_query ="";
		this.new_query ="";
		this.matarlized_view ="";
		this.estimated_time=0;
		//this.tempresult=new String[1000000][10];
	}
	public  void print_multi_query_info()
	{
		System.out.println("query Info-----------------");
		System.out.println("query_num: "+(this.query_num+1));
		System.out.println("orginal_query:"+this.orginal_query);
		System.out.println("new_query: "+this.new_query);
		System.out.println("rows: "+this.rows);
		System.out.println("columns:"+this.columns);
		System.out.println("query_type: "+this.query_type);
		System.out.println("temp_stored: "+this.matarlized_view);
		System.out.println("estimated_time: "+this.estimated_time);
		System.out.println("End query Info-----------------");


	}

}


