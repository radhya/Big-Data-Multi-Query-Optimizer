package MOTH_System;
//XX:Maxpermsize=500M

/*
 * First Radhya
 * the last vesion of our work dual HiveReused 
 */
/**
 *
 * @author hadoop
 */



import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MOTH{

	//  Declaration of data connection 
	public static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static Connection con ;
	public static Statement stmt ;  


	//Declaration of algorithm variables;

	public static int queryno=100;
	public static double total_time=0;
	public static File multi_queryfile;
	public static File sqlfile;
	public static String sql , new_sql;
	public static int query_num=0;
	public static String [] temp;
	public static  multi_query_info  multi_query_info_arr []= new multi_query_info[queryno];  
	public static  query_tokens  query_tokens_arr []= new query_tokens[queryno];  
	public static  Histogram  Histogram_arr []= new Histogram[50]; 
	public static int Histogram_len=0;
	public static int original_table_len=0;;
	public static  Sharing  Sharing_arr [][]= new Sharing[queryno][queryno]; 
	public static  SharingGraph  SharingGraph_arr []= new SharingGraph[queryno]; 
	public static double start_time=0, end_time=0, run_time=0;
	public static int SharingGraph_query_num=0;
	public  static List <String>  concurrentlist =new ArrayList <String> ();  
	public  static List <String>  nonconcurrentlist =new ArrayList <String> ();  
	public  static double   estimated_cost_nonconcurrent =0;
	public  static double   estimated_cost_concurrent =0;  
	public  static double total_datasize_WHDFS=0;
	public  static double HiveReused_Read_Total_Estimated_Cost=0;

	public static String table_name="My_Histogram_100m";

	public static int expermint_type=1; // 1-Shared Data
	public static  double Hive_total_estimated_cost= 0.0;
	public static boolean MT_Overlp_Tech=false;
	public static boolean Histogram_Tech= false;
	public static  boolean CostModel_Tech= false;
	public static boolean MOTH=true; 
	public static int MT_HistT_CostT=2; 
	public static int run_all=3; 
	public static double rows_num_in_million=250;
	public static int test_run=0; //0-false test only 1- true test & run
	public static int second_run=0;  // 0-false 1-true
	public static int not_rerun_query_no=1;
	public static int processors_num=2;

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args ) throws SQLException, FileNotFoundException, IOException, InterruptedException {

		System.out.println("***********Relaxed MRShare***************");

		try {
			Class.forName(driverName);
			switch (MT_HistT_CostT)
			{
			case 1: {MT_Overlp_Tech=true; System.out.println("**********  Uniform Distribution Materlization UDT**************"); }break; // Naive Materlaization Techinque
			case 2: {Histogram_Tech=true; System.out.println("********** Histogram HT**************");}break; // Histogram Techinque
			case 3: {CostModel_Tech=true; System.out.println("********** Histogram MetaData-Cost model  HMT**************");}break; // Histogram Techinque
			}
			load_Histogram();
			print_Histogram();
			parse_run_multi_query();
			Initi_Estimate_Results(0,query_num);
			// build sharing matrix
			Sharing_Classifier(0,query_num);
			CreateTemp(0);
			Print_Sharing();
			Reused_Parent_Enumerator(0);
			//Final_report();
			/*
      //Find_Nonderiviedquery(); Find_Sharing(0,query_num); Print_Sharing(); Find_Parent_Sharing(0);



    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }
  }
  public static void Find_MRSharing(int start, int end ) throws SQLException, IOException
    {
          Filter F=new Filter();

    }

  public static void Initi_Estimate_Results(int start, int end ) throws SQLException, IOException
  {

     for(int i=start; i<query_num;i++)
     {
        SharingGraph_arr[i]=new SharingGraph();


          SharingGraph_arr[i].queryno=i;
          SharingGraph_arr[i].level=0;
          SharingGraph_arr[i].parentno=-1;
          //SharingGraph_arr[i].query.equals(multi_query_info_arr[i].orginal_query);


        Filter F=new Filter(); 
        F.query=query_tokens_arr[i].query;
        //System.out.println("queryi:"+queryi);
        F.getWhereClauseToken(F.query);
        double result=get_range_stat_Histogram(F.LeftOperand,F.RightOperand); 
        SharingGraph_arr[i].actulresult=result;
        String s=  "["+F.LeftOperand+","+F.RightOperand+"] ...";
        s+=" actulresult of Q"+ (i+1)+" = "+SharingGraph_arr[i].actulresult;
        query_tokens_arr[i].datasize_in_MB=((result*query_tokens_arr[i].TupleSize)/(1024*1024));
        query_tokens_arr[i].estimated_cost=Estimate_Cost(query_tokens_arr[i].TupleSize,result,expermint_type, i);
        s+=" datasize of Q in MB "+ (i+1)+" = "+query_tokens_arr[i].datasize_in_MB;
        s+=" cost of Q"+ (i+1)+" = "+ query_tokens_arr[i].estimated_cost;
        System.out.println(s);
        total_datasize_WHDFS+=query_tokens_arr[i].datasize_in_MB;
        Hive_total_estimated_cost+=query_tokens_arr[i].estimated_cost;
       //Fi.printFilter();

     }

  }

   public static void Final_report()
   {
       // here to estimate time for concurrent and non concurrent queries
       int concurrent_query_num=0;
       for(int i=0; i<query_num;i++)

       { // if concurrent take the longest one
           if(SharingGraph_arr[i].isconcurrent)
           {
               //if (estimated_cost_concurrent<=query_tokens_arr[i].HiveReused_estimated_cost)

               estimated_cost_concurrent+=query_tokens_arr[i].HiveReused_estimated_cost;
               concurrent_query_num++;
               System.out.println( "concurrent Q"+(i+1)+" HiveReused_estimated_cost"+query_tokens_arr[i].HiveReused_estimated_cost);
           }
           // if nonconcurrent accumalte cost
            if(!SharingGraph_arr[i].isconcurrent)
            {
            estimated_cost_nonconcurrent+=query_tokens_arr[i].HiveReused_estimated_cost;
            System.out.println( "nonconcurrent Q"+(i+1)+" HiveReused_estimated_cost"+query_tokens_arr[i].HiveReused_estimated_cost);

            }

       }

     System.out.println("estimated_cost_concurrent  "+estimated_cost_concurrent);  
     System.out.println("estimated_cost_nonconcurrent  "+estimated_cost_nonconcurrent);  
     estimated_cost_concurrent/=processors_num;
     HiveReused_Read_Total_Estimated_Cost=estimated_cost_nonconcurrent+estimated_cost_concurrent;

     System.out.println("****************Final Reports*****************************");
     System.out.println("processors_num=" +processors_num);
     System.out.println("*********************************************");

     total_datasize_WHDFS=total_datasize_WHDFS/1024;
     System.out.println("total_datasize_WHDFS in GB="+total_datasize_WHDFS);

     System.out.println("*********************************************");
     System.out.println("total_datasize_WHDFS in GB="+total_datasize_WHDFS);

     System.out.println("Hive_total_estimated_cost min="+(Hive_total_estimated_cost));
     System.out.println("*********************************************");

     System.out.println("HiveReused_total_estimated_cost min="+(HiveReused_Read_Total_Estimated_Cost));
     System.out.println("*********************************************");

   }
   public static void Sharing_Classifier(int start, int end ) throws SQLException, IOException
    {
        /*
			 * this function implements 
			 * 1-sharing classifier into sharing and non sharing query lists (fully , partail, nonsharing) 
			 * according relation (table name) and coloums list, data ranges
			 * 2-sharing estimator 1- similar/ 2-overlapped /3-histogram /4-costmodel

			 */
			// act as flag 01 /10




			// Find_Sharing type
			for(int i=start; i<query_num;i++)

				for(int j=0; j<query_num;j++)
				{
					// dont comapre i,i
					if (i!=j)   

					{
						Sharing_arr[i][j]= new Sharing();

						//String queryi="select * from lineitem_t2 where l_discount between 6 and 10";
						// String queryj="select * from lineitem_t2 where l_discount between 15 and 3";
						String queryi=query_tokens_arr[i].query;
						String queryj=query_tokens_arr[j].query;
						double TuplesNo_j=0;


						// 1- if coli is subset or equal colj
						boolean subcolumns=query_tokens_arr[j].columns_list.containsAll(query_tokens_arr[i].columns_list);// ||
						// boolean subcolumns1=query_tokens_arr[j].columns_list.contains(query_tokens_arr[i].columns_list);
						boolean equalcolumns=query_tokens_arr[j].columns_list.equals(query_tokens_arr[i].columns_list);
						boolean condtion=query_tokens_arr[j].where_clauses.equals(query_tokens_arr[i].where_clauses);

						//MRshare
						//boolean subset= equalcolumns &&condtion; 
						//SHOW
						boolean subset=subcolumns || equalcolumns;
						//if(i==5)
						//System.out.println("subcolumns "+subcolumns+" equalcolumns "+equalcolumns);
						if (subset==false )//&& subcolumns1==false)
						{
							Sharing_arr[i][j].sharingtype="N";
							Sharing_arr[i][j].sharinginfo="not subcolumns OR equalcolumns";
							Sharing_arr[i][j].sharedresult=0;
						}
						if(subset)
						{
							//System.out.println("start compare");
							Filter Fi=new Filter();
							Fi.query=queryi;
							//System.out.println("queryi:"+queryi);
							Fi.getWhereClauseToken(queryi); 
							//Fi.printFilter();


							Filter Fj=new Filter();
							Fj.query=queryj;
							//System.out.println("queryj:"+queryj);
							Fj.getWhereClauseToken(queryj); 
							//Fj.printFilter();

							// 2- condtion if Fi has the same column as Fj  the test ranges
							if(Fi.column1.equals(Fj.column1))
							{
								// return the overlapping of queries
								Range<Integer> overlap=new Range <Integer>(0,0);
								overlap=Fi.issubset(Fj);
								int overlaplen=(overlap.intrsect_higherBound-overlap.intrsect_lowerBound)+1;
								//System.out.println("intrsect_lowerBound= "+overlap.intrsect_lowerBound);
								//System.out.println("intrsect_higherBound= "+overlap.intrsect_higherBound);
								//System.out.println("ovelapping length="+ overlaplen);
								//System.out.println("ovelapping type="+ overlap.overlapp_type);


								// assign sharing type 
								String overlapstr=overlap.intrsect_lowerBound+" and "+overlap.intrsect_higherBound;

								// to handle fully reused
								if(overlap.overlapp_type.equals("E"))
									overlap.overlapp_type="F";

								Sharing_arr[i][j].sharingtype= overlap.overlapp_type; 
								Sharing_arr[i][j].overlapping=overlapstr;
								String s=" Fi=["+Fi.LeftOperand+","+Fi.RightOperand+"]";
								s=s+"  Fj=["+Fj.LeftOperand+","+Fj.RightOperand+"]";
								s=s+ "  ["+ overlapstr+"]";
								Sharing_arr[i][j].sharinginfo=s;

								// Reused Estimator 
								// if full sharing get the Qj reused result for comparing later Fnnnn
								if(overlap.overlapp_type.equals("F")) 
								{

									if(MT_Overlp_Tech)
									{// for prvoius work
										TuplesNo_j=Fj.RightOperand-Fj.LeftOperand; 
										Sharing_arr[i][j].sharedresult=TuplesNo_j;
										Sharing_arr[i][j].sharedresultsize=TuplesNo_j;
										Sharing_arr[i][j].cost=TuplesNo_j;
									}
									else if (Histogram_Tech)
									{
										TuplesNo_j=get_range_stat_Histogram(Fj.LeftOperand,Fj.RightOperand);   
										//System.out.println(Fj.LeftOperand+""+Fj.RightOperand);
										Sharing_arr[i][j].sharedresult=TuplesNo_j;
										Sharing_arr[i][j].sharedresultsize=TuplesNo_j;
										//Sharing_arr[i][j].cost=TuplesNo_j;
										Sharing_arr[i][j].cost=Estimate_Cost(1,TuplesNo_j,expermint_type,j);
									}

									else if (CostModel_Tech)
									{
										TuplesNo_j=get_range_stat_Histogram(Fj.LeftOperand,Fj.RightOperand); 

										Sharing_arr[i][j].sharedresult=TuplesNo_j;
										Sharing_arr[i][j].sharedresultsize=TuplesNo_j*query_tokens_arr[j].TupleSize;
										Sharing_arr[i][j].cost=Estimate_Cost(query_tokens_arr[j].TupleSize,TuplesNo_j,expermint_type,j);

									}

									//rewite() select from view  assign to non concurrent list according to its parent
								}

								// if partail sharing get the overlapping results comparing later Pnnnn
								if(overlap.overlapp_type.equals("P")) 
								{
									// // for prvoius work (Wang work)  
									if(MT_Overlp_Tech) 
									{
										TuplesNo_j=overlap.intrsect_higherBound-overlap.intrsect_lowerBound;
										Sharing_arr[i][j].sharedresult=TuplesNo_j;
										Sharing_arr[i][j].sharedresultsize=TuplesNo_j;
										Sharing_arr[i][j].cost=TuplesNo_j;
									}
									else if (Histogram_Tech)
									{
										TuplesNo_j=get_range_stat_Histogram(overlap.intrsect_lowerBound,overlap.intrsect_higherBound);
										Sharing_arr[i][j].sharedresult=TuplesNo_j;
										Sharing_arr[i][j].sharedresultsize=TuplesNo_j;
										Sharing_arr[i][j].cost=TuplesNo_j;

									}
									else if (CostModel_Tech)
									{
										// evalute Temp
										TuplesNo_j=get_range_stat_Histogram(Fj.LeftOperand,Fj.RightOperand); 
										double TempCost=0;
										Sharing_arr[i][j].sharedresult=TuplesNo_j;
										Sharing_arr[i][j].sharedresultsize=(TuplesNo_j*query_tokens_arr[j].TupleSize)+TempCost;
										Sharing_arr[i][j].cost=Estimate_Cost(query_tokens_arr[j].TupleSize,TuplesNo_j,expermint_type,j);

									}

									// //rewite() select from view and sub query assign to concurrent list
								}

								if(overlap.overlapp_type.equals("N"))
								{
									Sharing_arr[i][j].sharedresult=0;
									Sharing_arr[i][j].sharedresult=0;
									Sharing_arr[i][j].sharedresultsize=0;//(TuplesNo_j*query_tokens_arr[j].TupleSize)+TempCost;
									Sharing_arr[i][j].cost=0;//Estimate_Cost(query_tokens_arr[j].TupleSize,TuplesNo_j,expermint_type);

									// assign to concurrent if it has not sharing with other queries
								}
								// System.out.println("sharingtype  of ["+i+","+j+"]= "+Sharing_arr[i][j].sharingtype);
								// System.out.println("sharedresult of ["+i+","+j+"]= "+Sharing_arr[i][j].sharedresult);

							}   // 1- if coli is subset or equal colj
						}  //if(Fi.column1.equals(Fj.column1))

					}// only for i<>j digonal is not comparing
				}// end for j loop

		} //  end Find_Sharing function


		public static double  Estimate_Cost(double TupleSize, double TuplesNo, int expermint_type, int queryno)
		{

			double CPUC=5; // nanosecond
			double NET=300*CPUC;
			double cost_LocalDisk_Read=4*NET;
			double cost_LocalDisk_Write=4*NET;
			double cost_HDFS_Read=1.5*cost_LocalDisk_Read;
			double cost_HDFS_Write=10*cost_LocalDisk_Write;

			double IO_Usage=0;
			double CPU_Usage=0;

			/*   if( Histogram_Tech)  
        {
         IO_Usage=cost_HDFS_Read*TuplesNo*1;
         CPU_Usage=cost_HDFS_Read*TuplesNo;
        }
        else if(CostModel_Tech)
        {
             {
         IO_Usage=cost_HDFS_Read*TuplesNo*TupleSize;
         CPU_Usage=cost_HDFS_Read*TuplesNo;
        }
        }   
			 * */
			IO_Usage=cost_HDFS_Read*TuplesNo*TupleSize;
			CPU_Usage=cost_HDFS_Read*TuplesNo;


			double Total_cost=0;

			switch (expermint_type)
			{
			case 1: Total_cost=IO_Usage+CPU_Usage; break; // sharing data
			}

			// convert cost in terms of seconds according of processors number
			// double temp_Total_cost=Total_cost;
			double IO_ratio=(IO_Usage/Total_cost);
			double CPU_ratio=(CPU_Usage/Total_cost);
			double processors_num_ratio=(processors_num/Total_cost);
			// double rows_num_in_million_ratio=(rows_num_in_million/Total_cost);
			System.out.println("Query "+(queryno+1)+"------------------------------------------");
			System.out.println("TuplesNo= "+TuplesNo +"..TupleSize="+ TupleSize);
			System.out.println("IO_ratio="+IO_ratio+" CPU_ratio="+CPU_ratio);

			System.out.println("before Total_cost="+Total_cost);  

			//Total_cost*= CPU_ratio;//+processors_num_ratio;//+ rows_num_in_million_ratio;//+(CPU_Usage/Total_cost);
			//Total_cost*= (CPU_Usage/Total_cost);
			// double diff_Total_cost=temp_Total_cost-Total_cost;

			System.out.println("IO_Usage= "+IO_Usage +"..CPU_Usage="+ CPU_Usage);
			System.out.println("after Total_cost="+Total_cost);  

			Total_cost=Total_cost /1000000000; // convert from nano second to second
			Total_cost/=processors_num;  
			Total_cost/=60; // convert to min

			// System.out.println("TuplesNo= "+TuplesNo +"..TupleSize="+ TupleSize);
			//System.out.println("IO_Usage= "+IO_Usage +"..CPU_Usage="+ CPU_Usage);
			//System.out.println("temp_Total_cost="+temp_Total_cost+" Total_cost in min= "+Total_cost+ " diff="+diff_Total_cost);  
			System.out.println(" Total_cost in min= "+Total_cost);  

			return Total_cost;  
		}

		public static void  CreateTemp(int parent_no)
		{
			/*
			 * this fuction to store non dervied data into Temp
			 */
			// find LeftOperand for temp as minum =min(LeftOperands)
			double MinLeftOperand=query_tokens_arr[0].LeftOperand; 
			for(int i=1; i<query_num;i++)
			{
				if(query_tokens_arr[i].LeftOperand<=MinLeftOperand)
					MinLeftOperand=query_tokens_arr[i].LeftOperand;
			}

			// find LeftOperand for temp as minum =min(LeftOperands)
			double MaxRightOperand=query_tokens_arr[0].RightOperand; 
			for(int i=1; i<query_num;i++)
			{
				if(query_tokens_arr[i].RightOperand>=MaxRightOperand)
					MaxRightOperand=query_tokens_arr[i].RightOperand;
			}
			//MinLeftOperand=MinLeftOperand/100;
			//MaxRightOperand=MaxRightOperand/100;
			System.out.println("MinLeftOperand= "+ MinLeftOperand+ " MaxRightOperand= "+ MaxRightOperand);

			// write Temp Table Query
			//sql=select  count(*) from lineitem_10m where l_discount between 0.01 and 0.03 or l_discount between 0.05 and 0.06 
			String between1= " between "+Double.toString(MinLeftOperand/100)+" and "+Double.toString((query_tokens_arr[parent_no].LeftOperand-1)/100);
			String between2= " between "+Double.toString((query_tokens_arr[parent_no].RightOperand+1)/100)+" and "+Double.toString(MaxRightOperand/100);
			String TempQuery=query_tokens_arr[parent_no].query;
			// TempQuery=
			int index_where=TempQuery.indexOf("where");
			int index_between=TempQuery.indexOf("between");
			// to get coloum name such as l_discount
			String coloum_name=TempQuery.substring(index_where+5, index_between);

			//get sub of coloumns from parent query
			sql=TempQuery.substring(0, index_where+5); 
			sql+=coloum_name+between1+ " OR "+coloum_name+ between2;
			System.out.println("temp query .."+sql);



		}
		public static void  Print_Sharing()
		{
			// Find_Sharing type
			for(int i=0; i<query_num;i++)
			{
				System.out.println("--------------------------------------------");
				for(int j=0; j<query_num;j++)

					if (i!=j)   
					{
						System.out.print("overlapping ["+(i+1)+","+(j+1)+"]= "+Sharing_arr[i][j].sharinginfo);
						System.out.print("\tsharingtype  of ["+(i+1)+","+(j+1)+"]= "+Sharing_arr[i][j].sharingtype);
						System.out.print("\tsharedresult of ["+(i+1)+","+(j+1)+"]= "+Sharing_arr[i][j].sharedresult);
						System.out.println();

					}// end j
			} // end i
		}// end print sharing


		public static void Reused_Parent_Enumerator(int start ) throws SQLException, IOException, ClassNotFoundException
		{
			// init SharingGraph_arr
			// SharingGraph_query_num=start;

			SharingGraph_query_num=start;
			//start to get parent
			// for(int i=0; i<query_num;i++)
			while(SharingGraph_query_num<query_num)
			{
				int not_share=0;
				int i=SharingGraph_query_num;



				System.out.println("Find query parent of "+(i+1));


				List <Parent>  FullParentList =new ArrayList <Parent> ();  
				List <Parent>  PartailParentList =new ArrayList <Parent> ();  
				Parent  TempParent =new Parent(); 
				boolean isroot=true; 
				// find parent 
				for(int j=0; j<query_num;j++)
				{
					// dont comapre i,i
					boolean ismychild=SharingGraph_arr[i].ismychildern(SharingGraph_arr[j].queryno);
					//to check later if is root when has childern and no parent

					//if(ismychild)isroot=true;
					boolean ispartail= SharingGraph_arr[i].partialno==SharingGraph_query_num;

					if (i!=j && !ismychild )//&&!ispartail)
					{


						if(Sharing_arr[i][j].sharingtype.equals("F"))
						{
							TempParent =new Parent();
							TempParent.queryno=j;
							TempParent.sharedresult=Sharing_arr[i][j].sharedresult;
							TempParent.cost=Sharing_arr[i][j].cost;
							//TempParent.
							FullParentList.add(TempParent);
						}

						// if i is partail contain in j and j not fully contain so better to set j is parent of i 
						else if(Sharing_arr[i][j].sharingtype.equals("P")&&!Sharing_arr[j][i].sharingtype.equals("F") )
							//else if(Sharing_arr[i][j].sharingtype.equals("P"))
						{
							System.out.println("original_table_len.."+original_table_len);
							double sharedresults=Sharing_arr[i][j].sharedresult;
							double actualresults=SharingGraph_arr[i].actulresult;
							//int reminder=actualresults-sharedresults;                
							//double share_sel= (double) sharedresults/SharingGraph_arr[j].actulresult;
							// double reminder_sel=  (double) reminder/original_table_len;
							//double sel=1-share_sel+reminder_sel;

							TempParent =new Parent();
							TempParent.queryno=j;
							TempParent.sharedresult=sharedresults;
							TempParent.actualresult=SharingGraph_arr[i].actulresult;
							TempParent.sharedresult=sharedresults;
							TempParent.cost=Sharing_arr[i][j].cost;

							//TempParent.selectiviy=sel;        
							String s="Q"+(i+1)+", Q"+(j+1)+ " overlapping "+ Sharing_arr[i][j].overlapping;
							// s+=" sharedresults="+sharedresults+ "...reminder="+reminder;
							//s+=" share_sel="+share_sel+ "...reminder_sel="+reminder_sel;
							//System.out.println(s+ " selectiviy: "+ sel);
							PartailParentList.add(TempParent);
						}

						// here to set not share if and if sharing(i,j)=N and sharing(j,i)=N
						else if(Sharing_arr[i][j].sharingtype.equals("N") && Sharing_arr[j][i].sharingtype.equals("N"))
						{
							not_share++;
							System.out.println(" I am not share hhhh");
						}
					} // end if not (i,i)

				} // end for j   


				boolean AllnotShare=not_share==(query_num-1);
				boolean AllFull=FullParentList.size()==(query_num-1);
				boolean AllPartial=PartailParentList.size()==(query_num-1);
				boolean mixed=!FullParentList.isEmpty() && !PartailParentList.isEmpty();
				boolean someFull=(!AllnotShare && !FullParentList.isEmpty() && PartailParentList.isEmpty());
				boolean SomePartial=(!AllnotShare && FullParentList.isEmpty() && !PartailParentList.isEmpty());

				//boolean parentroo=isroot && i
				//assign to concurrent not share with other query
				if (AllnotShare)
				{
					System.out.println("not share query"+ (i+1));
					SharingGraph_arr[SharingGraph_query_num].queryno=i;
					SharingGraph_arr[SharingGraph_query_num].parentno=-1;
					SharingGraph_arr[SharingGraph_query_num].sharedresult=0;
					SharingGraph_arr[SharingGraph_query_num].level=0;
					SharingGraph_arr[SharingGraph_query_num].isshare=false;
					SharingGraph_arr[SharingGraph_query_num].query=query_tokens_arr[i].query; 
					SharingGraph_arr[SharingGraph_query_num].sharingtype="is not share";
					SharingGraph_query_num++;


					//isroot=false;

				}



				// here we can applay cost model for estimate sharing

				// All Full  get min of Full
				else if(AllFull ||someFull)
				{
					if (AllFull)
						System.out.println("All Full");
					else
						System.out.println("some Full");
					TempParent=new Parent();
					//int index_min=TempParent.Find_Min_ParentList(FullParentList);
					int index_min=TempParent.Find_Min_Cost_ParentList(FullParentList);
					TempParent=FullParentList.get(index_min);

					// update my sharing graph info
					int parentno= TempParent.queryno;
					SharingGraph_arr[SharingGraph_query_num].queryno=i;
					SharingGraph_arr[SharingGraph_query_num].parentno=TempParent.queryno;
					SharingGraph_arr[SharingGraph_query_num].sharedresult=TempParent.sharedresult;
					SharingGraph_arr[SharingGraph_query_num].level=SharingGraph_arr[parentno].level+1;
					//System.out.println("I am qury num"+ SharingGraph_arr[SharingGraph_query_num].queryno);
					// take the childern to avoid acyclic
					//SharingGraph_arr[SharingGraph_query_num].addme(SharingGraph_arr);
					SharingGraph_arr[parentno].getsubqueries(SharingGraph_arr[SharingGraph_query_num],SharingGraph_arr);
					SharingGraph_arr[SharingGraph_query_num].sharingtype="F";
					SharingGraph_arr[SharingGraph_query_num].overlapping=Sharing_arr[i][parentno].overlapping;

					// rewrite
					SharingGraph_arr[SharingGraph_query_num].query=query_tokens_arr[i].query; 

					System.out.println("query num "+ ( SharingGraph_arr[SharingGraph_query_num].queryno+1)+" min Parent of Full "+(TempParent.queryno+1)+" cost "+TempParent.cost);
					SharingGraph_query_num++;
					isroot=false;

				}


				// //get max of partail
				else if(AllPartial ||SomePartial) 
				{
					if (AllPartial && (MT_Overlp_Tech ||Histogram_Tech))
					{
						System.out.println("all partail");
						int index_max=TempParent.Find_Max_ParentList(PartailParentList);
						TempParent=PartailParentList.get(index_max);
					} 

					System.out.println("some partail");
					TempParent=new Parent();

					// here evalute ovelapping according to used Techinque
					if (MT_Overlp_Tech ||Histogram_Tech)
					{
						//int index_max=TempParent.Find_selectevity_ParentList(PartailParentList);//,original_table_len);

					}
					else if (CostModel_Tech)
					{
						int index_min=TempParent.Find_Min_Cost_ParentList(PartailParentList);
						TempParent=PartailParentList.get(index_min);
					}

					//System.out.println("query num"+ (SharingGraph_query_num+1)+" min Parent "+(TempParent.queryno+1)+" shared result "+TempParent.sharedresult);

					// update my sharing graph info
					int parentno= TempParent.queryno;
					SharingGraph_arr[SharingGraph_query_num].queryno=i;
					SharingGraph_arr[SharingGraph_query_num].parentno=TempParent.queryno;
					SharingGraph_arr[SharingGraph_query_num].sharedresult=TempParent.sharedresult;
					SharingGraph_arr[SharingGraph_query_num].level=SharingGraph_arr[parentno].level+1;
					//System.out.println("I am qury num"+ SharingGraph_arr[SharingGraph_query_num].queryno);
					SharingGraph_arr[SharingGraph_query_num].sharingtype="P";
					SharingGraph_arr[SharingGraph_query_num].overlapping=Sharing_arr[i][parentno].overlapping;

					// take the childern to avoid acyclic
					//SharingGraph_arr[SharingGraph_query_num].addme(SharingGraph_arr); 
					SharingGraph_arr[parentno].getsubqueries(SharingGraph_arr[SharingGraph_query_num],SharingGraph_arr);

					//rewrite
					SharingGraph_arr[SharingGraph_query_num].query=query_tokens_arr[i].query; 
					//SharingGraph_arr[SharingGraph_query_num]

					System.out.println("query num "+ ( SharingGraph_arr[SharingGraph_query_num].queryno+1)+" max Parent "+(TempParent.queryno+1)+" shared result "+TempParent.sharedresult);




					SharingGraph_query_num++;
					isroot=false;
				}

				// here we can applay cost model for estimate sharing
				//get min of full 
				else if(mixed)         
				{

					System.out.println("mixed Full and partial");
					//System.out.println("Print Full List");
					TempParent=new Parent();
					//TempParent.Print_ParentList(FullParentList);
					int index_min=TempParent.Find_Min_Cost_ParentList(FullParentList);
					TempParent=FullParentList.get(index_min);
					System.out.println("query num "+ (SharingGraph_query_num+1)+" Min of Full Parents "+(TempParent.queryno+1)+" shared result "+TempParent.sharedresult);

					//System.out.println("query num"+ (SharingGraph_query_num+1)+" min Parent "+(TempParent.queryno+1)+" shared result "+TempParent.sharedresult);

					// update my sharing graph info
					int parentno= TempParent.queryno;
					SharingGraph_arr[SharingGraph_query_num].queryno=i;
					SharingGraph_arr[SharingGraph_query_num].parentno=TempParent.queryno;
					SharingGraph_arr[SharingGraph_query_num].sharedresult=TempParent.sharedresult;
					SharingGraph_arr[SharingGraph_query_num].level=SharingGraph_arr[parentno].level+1;
					SharingGraph_arr[SharingGraph_query_num].sharingtype="F";
					SharingGraph_arr[SharingGraph_query_num].overlapping=Sharing_arr[i][parentno].overlapping;

					// take the childern to avoid acyclic
					SharingGraph_arr[parentno].getsubqueries(SharingGraph_arr[SharingGraph_query_num],SharingGraph_arr);
					// SharingGraph_arr[SharingGraph_query_num].addme(SharingGraph_arr); 
					// rewrite

					SharingGraph_arr[SharingGraph_query_num].query=query_tokens_arr[i].query; 


					SharingGraph_query_num++;
					isroot=false;

				}  


				if(!AllnotShare&&isroot)
				{
					System.out.println("I am a root");
					SharingGraph_arr[SharingGraph_query_num].queryno=i; 
					SharingGraph_arr[SharingGraph_query_num].level=0;
					SharingGraph_arr[SharingGraph_query_num].isroot=true;
					SharingGraph_arr[SharingGraph_query_num].query=query_tokens_arr[i].query; 
					SharingGraph_arr[SharingGraph_query_num].sharingtype="is root";

					SharingGraph_query_num++; 
					//SharingGraph_arr[SharingGraph_query_num].query.equals(multi_query_info_arr[i].orginal_query);

				} // ensd !AllnotShare&&isroot
				//Sharing_arr[2][0].overlapping

				// here to calculate the srtimated time 
				if(SharingGraph_arr[i].isroot)
				{
					query_tokens_arr[i].HiveReused_estimated_cost= query_tokens_arr[i].Hive_estimated_cost;
					System.out.println("Hive_estimated_cost "+query_tokens_arr[i].Hive_estimated_cost);
					System.out.println("Root its same as Hive ****HiveReused_estimated_cost "+query_tokens_arr[i].HiveReused_estimated_cost);

				}
				else 
				{
					int parentno=SharingGraph_arr[i].parentno;
					query_tokens_arr[i].HiveReused_estimated_cost= query_tokens_arr[parentno].estimated_cost;
					System.out.println("Hive_estimated_cost "+query_tokens_arr[i].Hive_estimated_cost);
					System.out.println("Sub query updated ****HiveReused_estimated_cost "+query_tokens_arr[i].HiveReused_estimated_cost);

				}    


			}// end for i

			System.out.println("SharingGraph_query_num" +SharingGraph_query_num);
			// twice update level to safe order of levels :) 
			SharingGraph.update_levels(SharingGraph_arr,  SharingGraph_query_num);
			SharingGraph.update_levels(SharingGraph_arr,  SharingGraph_query_num);

			// add me to parent sub queries
			SharingGraph.update_grandfathers(SharingGraph_arr, SharingGraph_query_num);
			SharingGraph.sortAllsubquries(SharingGraph_arr, SharingGraph_query_num);
			SharingGraph.rewrite(query_tokens_arr, SharingGraph_arr, SharingGraph_query_num);
			SharingGraph.setroots(SharingGraph_arr, SharingGraph_query_num);

			// SharingGraph.print_ArrayofGraphSharing(SharingGraph_arr, SharingGraph_query_num);
			//SharingGraph.printroots(SharingGraph_arr, SharingGraph_query_num);
			//SharingGraph.printnotshare(SharingGraph_arr, SharingGraph_query_num);
			SharingGraph.print_orignal_queries(SharingGraph_arr, SharingGraph_query_num);
			SharingGraph.print_Finaloutput(SharingGraph_arr, query_tokens_arr,SharingGraph_query_num,rows_num_in_million/10);
			//System.out.println("before sort");
			//SharingGraph_arr[3].printsubqueries(SharingGraph_arr);

			//System.out.println("SharingGraph_query_num"+SharingGraph_query_num);





		}//end of Find_Parent_Sharing















		public static void Find_Nonderiviedquery() throws SQLException, IOException
		{
			// 
			System.out.println("Find_Nonderiviedquery");
			for(int i=0; i<query_num;i++)

				if(SharingGraph_arr[i].sharingtype.equals("P"))
				{

					//String nonderivedquery="select L_PARTKEY from lineitem_t2 where l_discount between 6 and 8;";

					String nonderivedquery=SharingGraph_arr[i].SNDQ.get(0)+";";

					// generate non derived

					parse_one_query(nonderivedquery);
					int start=query_num;
					System.out.println(query_tokens_arr[query_num-1].query);
					System.out.println((SharingGraph_arr[i].queryno+1)+SharingGraph_arr[i].sharingtype);
					SharingGraph_arr[i].partialno=SharingGraph_query_num;
					SharingGraph_arr[SharingGraph_query_num]=new SharingGraph();
					SharingGraph_arr[SharingGraph_query_num].queryno=SharingGraph_query_num;
					//Find_Sharing(0,query_num-1);
					//Find_Parent_Sharing(SharingGraph_query_num);
					// if(SharingGraph_arr[i].level==SharingGraph_arr[SharingGraph_query_num-1].level)
					{
						//SharingGraph_arr[i].level=SharingGraph_arr[SharingGraph_query_num-1].level+1;
						//SharingGraph.update_levels(SharingGraph_arr,  SharingGraph_query_num);
						// SharingGraph.print_ArrayofGraphSharing(SharingGraph_arr, SharingGraph_query_num);


					}


					// set nonderivedquery 

					// SharingGraph_arr[SharingGraph_query_num].Fullquery=nonderivedquery;
					// SharingGraph_arr[SharingGraph_query_num].level=0;
					//SharingGraph_arr[SharingGraph_query_num].isshare=false;
					// SharingGraph_arr[SharingGraph_query_num].sharingtype="is nonderivedquery";
					SharingGraph_query_num++;
				}

			//query_num++;

			//i=0;
			//SharingGraph_query_num=0;
		}
		public static void  load_Histogram  () throws SQLException, ClassNotFoundException, IOException
		{


			Connection con ;
			String driverName = "org.apache.hive.jdbc.HiveDriver";
			java.sql.Statement stmt;


			Class.forName(driverName);
			con  = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
			stmt = con.createStatement();
			System.out.println("Load Histogram:" );

			//String sql="select * from My_Histogram where table_name='lineitem_1m'";
			String sql="select * from " +table_name;
			// String sql="select * from my_histogram_20m";// where table_name='lineitem'";

			stmt.execute(sql);
			ResultSet result = stmt.executeQuery(sql);
			ResultSetMetaData rsmd;

			int row=0; int sum=0;
			while (result.next()) {
				rsmd=result.getMetaData();
				// inint Histogram
				Histogram_arr[row]= new Histogram();
				Histogram_arr[row].Histogram();

				Histogram_arr[row].tablename=result.getString(1); 
				Histogram_arr[row].columnname=result.getString(2); 
				Histogram_arr[row].Distinct_Value=Integer.parseInt(result.getString(3)); 
				Histogram_arr[row].frequency=Integer.parseInt(result.getString(4)); 
				original_table_len+= Histogram_arr[row].frequency;
				row++;
			}
			Histogram_len=row;
			con.close();

		} //end load_Histogram


		public static void  print_Histogram()
		{
			String space="   ";
			System.out.println("tablename"+ space+"columnname"+space+"Distinct_Value"+space+"frequency"); 

			for(int i=0; i<Histogram_len;i++)
				Histogram_arr[i].print_Histogram();  
		}


		public static double  get_stat_Histogram(int distinctvalue)    
		{
			double temp=0;
			for(int i=0; i<Histogram_len;i++)
				if(Histogram_arr[i].Distinct_Value==distinctvalue)
				{
					temp=(Histogram_arr[i].frequency*rows_num_in_million)/10;
					return temp;
				}     
			return 0;

		}
		public static double  get_multi_stat_Histogram(int [] distinctvalues)    
		{ 
			int sumfrequency=0;
			for(int i=0; i<Histogram_len;i++)
				for(int j=0; j<distinctvalues.length;j++)
					if(Histogram_arr[i].Distinct_Value==distinctvalues[j])
						sumfrequency+= Histogram_arr[i].frequency;
			double temp=(sumfrequency*rows_num_in_million)/10;
			return temp;

		}
		public static double  get_range_stat_Histogram(int distinctvalue1, int distinctvalue2)    
		{
			double sumfrequency=0;
			for(int i=0; i<Histogram_len;i++)
			{ 
				int dv=Histogram_arr[i].Distinct_Value;
				if(dv>=distinctvalue1 && dv<= distinctvalue2)
					sumfrequency+=Histogram_arr[i].frequency;
			}
			double temp=(sumfrequency*rows_num_in_million)/10;
			// System.out.println("get_range_stat_Histogram"+temp);
			return temp;

		}

		public static double  get_Full_Histogram()    
		{
			int sumfrequency=0;
			for(int i=0; i<Histogram_len;i++)
			{ 

				sumfrequency+=Histogram_arr[i].frequency;
			}
			double temp=(sumfrequency*rows_num_in_million)/10;
			return temp;

		}


		// connection function
		public static void  connection_db  () throws SQLException
		{
			//replace "hive" here with the name of the user the queries should run as


		}

	
		public static void  parse_run_multi_query() throws IOException, SQLException
		{

			multi_queryfile = new File ("src/multi_query.sql");
			//File currentDir = new File("");
			// String filename=currentDir.getAbsolutePath()+"/SharedHive/"+multi_queryfile;	
			//System.out.println(filename);
			System.out.println("File exists :  " + multi_queryfile);
			if (!multi_queryfile.exists()){
				System.out.println("File not exists :  " + multi_queryfile);
				return;
			}



			// read sql from sql file    

			Scanner read = new Scanner (multi_queryfile).useDelimiter(" ");
			sql = "";
			total_time=0;

			while (read.hasNextLine())
			{
				//run_query=true;
				sql=read.nextLine();
				if(sql.isEmpty())sql=read.nextLine();
				String comment=sql.substring(0,1);
				//System.out.println(comment);
				//valid query
				if (!comment.equals("#") )
				{
					//System.out.println("valid query") ; 

					//init();
					parse_one_query(sql);
				

				}
			}

			//calculate_total_time();

			// compare_multi_query();
		}// end parse_run_multi_query function


		public static void parse_one_query(String sql) throws SQLException
		{
			try {
				ReadWriteFiles.write_single_query(sql);
				sqlfile = new File ("one_query.sql");
			} catch (IOException ex) {
				Logger.getLogger(MOTH.class.getName()).log(Level.SEVERE, null, ex);
			}
			sql=sql.substring(0,sql.length()-1);
			query_tokens_arr[query_num]= new query_tokens();
			query_tokens_arr[query_num].query_tokens();

			try {
				query_tokens_arr[query_num].query=sql;
				query_tokens_arr[query_num].parsing(sql,sqlfile);
				query_tokens_arr[query_num].get_TupleSize();
				System.out.println("TupleSize of Q"+(query_num +1)+"= "+query_tokens_arr[query_num].TupleSize);
			} catch (IOException ex) {
				Logger.getLogger(MOTH.class.getName()).log(Level.SEVERE, null, ex);
			}
			query_tokens_arr[query_num].query=sql;
			query_num++;
		}



		public static void calculate_total_time() 
		{
			System.out.println("---------------print_multi_query_info----------------------");
			total_time=0;
			for( int i=0;i<query_num;i++) 
			{
				total_time+=multi_query_info_arr[i].estimated_time;
				//System.out.println("query_num"+multi_query_info_arr[i].query_num+"   "+multi_query_info_arr[i].new_query+" time ="+multi_query_info_arr[i].estimated_time);
				multi_query_info_arr[i].print_multi_query_info();

			}
			//System.out.println("query_num"+multi_query_info_arr[0].query_num+"   "+multi_query_info_arr[0].orginal_query+" time ="+multi_query_info_arr[0].estimated_time);

			System.out.println("total_time for all queries "+total_time);   
		}



		public static void print_query_tokens()throws IOException
		{
			System.out.print("query_num\t"+(query_num+1)+"\t");
			query_tokens_arr[query_num].print_query_tokens(); 
		}







	}// end class