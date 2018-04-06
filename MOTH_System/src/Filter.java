/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package MOTH_System;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.TGSqlParser;

/**
 *
 * @author hadoop
 */
public class Filter {

	public  String column1="";
	public  String column2="";
	public int LeftOperand=0;
	public int RightOperand=0;
	public String query="";
	public String tablename="";
	public String Sharing="";
	public int estimated_sharing=0;

	public  void Filter()
	{
		this.LeftOperand=0;
		this.RightOperand=0;
		this.column1="";
		this.column2="";
		this.query="";
		this.tablename="";
		this.Sharing="";
		this.estimated_sharing=0;
	}

	public  Range issubset(Filter F2)
	{
		Range<Integer> Q1 = new Range<Integer>(this.LeftOperand, this.RightOperand);

		Range<Integer> Q2 = new Range<Integer>(F2.LeftOperand, F2.RightOperand); 

		Range<Integer> overlap=new Range <Integer>(0,0);
		
		Q1.intersect(Q2);
		//if(Q1.intrsect_higherBound==0&& Q1.intrsect_lowerBound==0 )
		if(Q1.overlp_count<0)

		{
			overlap.intrsect_lowerBound=0;
			overlap.intrsect_higherBound=0;
			overlap.overlapp_type="N";

			return overlap;
		}

		else if(this.LeftOperand==F2.LeftOperand && this.RightOperand==F2.RightOperand)
		{
			
			overlap.intrsect_lowerBound=Q1.intrsect_lowerBound;
			overlap.intrsect_higherBound=Q1.intrsect_higherBound;
			overlap.overlapp_type="E";
			return overlap;
		}

		else if(this.LeftOperand>=F2.LeftOperand && this.RightOperand<=F2.RightOperand)
		{
			
			overlap.intrsect_lowerBound=Q1.intrsect_lowerBound;
			overlap.intrsect_higherBound=Q1.intrsect_higherBound;
			overlap.overlapp_type="F";
			return overlap;
		}


		//partial F1=[3-7] F2 is F2=[4-10] OR [1-5]  
		else if(this.LeftOperand<=F2.LeftOperand || this.RightOperand>=F2.RightOperand)
		{
			
			overlap.intrsect_lowerBound=Q1.intrsect_lowerBound;
			overlap.intrsect_higherBound=Q1.intrsect_higherBound;
			overlap.overlapp_type="P";

			return overlap;
		}

				return overlap; 

	}



	public  void printFilter()
	{ String space="  ";
	System.out.println(this.tablename+space+this.column1+space+" is ["+this.LeftOperand+","+this.RightOperand+" ]");
	}



	public   void getWhereClauseToken( String query)
	{
		TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvmssql);
		sqlparser.sqltext=query;
 
		int i = sqlparser.parse( );
		String temp="";

		temp=sqlparser.sqlstatements.get( 0 ).getWhereClause( ).toString();
		temp=sqlparser.sqlstatements.get( 0 ).getWhereClause( ).toString();

		for(int j=0;j<sqlparser.sqlstatements.get( 0 ).tables.size();j++)            
		{
			this.tablename= sqlparser.sqlstatements.get( 0 ).tables.getTable(j).getName();
 

		}
 		temp=sqlparser.sqlstatements.get( 0 ).getWhereClause( ).getCondition().getOperatorToken().toString();
 		temp=sqlparser.sqlstatements.get( 0 ).getWhereClause( ).getCondition().getLeftOperand().toString();
		if(temp.contains(".")) 
			this.LeftOperand=(int)(Double.parseDouble(temp)*100);
		else
			this.LeftOperand=Integer.parseInt(temp);

		temp=sqlparser.sqlstatements.get( 0 ).getWhereClause( ).getCondition().getBetweenOperand().toString();
		this.column1=temp;
		temp=sqlparser.sqlstatements.get( 0 ).getWhereClause( ).getCondition().getRightOperand().toString();
		if(temp.contains(".")) 
			this.RightOperand=(int)(Double.parseDouble(temp)*100);  
		else
			this.RightOperand=Integer.parseInt(temp);
		//swap if left is larger than right

		if(this.LeftOperand>this.RightOperand)
		{

			      
			int t=this.LeftOperand;
			this.LeftOperand=this.RightOperand;
			this.RightOperand=t;
			 
		}

	}


	public static void main(String[] args )
	{
		String query="select lo_discount from lineorder1 where lo_discount between 2 and 7 group by lo_discount";

		Filter f1=new Filter();
		f1.query=query;
		System.out.println("query:"+query);
		f1.getWhereClauseToken(query); 
		f1.printFilter();

		query="select lo_discount from lineorder1 where lo_discount between 8 and 9 group by lo_discount";

		Filter f2=new Filter();
		f2.query=query;
		System.out.println("query:"+query);
		f2.getWhereClauseToken(query); 
		f2.printFilter();
		Range<Integer> overlap=new Range <Integer>(0,0);
		overlap=f1.issubset(f2);
		int overlaplen=(overlap.intrsect_higherBound-overlap.intrsect_lowerBound)+1;
		System.out.println("intrsect_lowerBound= "+overlap.intrsect_lowerBound);
		System.out.println("intrsect_higherBound= "+overlap.intrsect_higherBound);
		System.out.println("ovelapping length="+ overlaplen);

	}  
}// end class

