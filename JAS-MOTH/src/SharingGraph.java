/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package SharedHive;


import Test.TestFileBuffer;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author hadoop
 */

public class SharingGraph {
    
   
   public  int queryno=0;
   public  int rootno=-1;
   public  int parentno=-1;
   public  int Partialno=-1;
   public  int level=0;
   public  double actulresult =0;
   public  double sharedresult=0;
   public  String query="";
   public  String sharingtype="";
   public  String overlapping="";
   public  String Fullquery="";
   public boolean isshare=true;
   public boolean isroot=false;
   public boolean isleaf=false;
   //public boolean isPartial=true;
   public boolean isconcurrent=false;
   public double reused_datasize=0; // parent  data size
   public  List <Integer>  subqueries =new ArrayList <Integer> ();  
   public  List <String>  SNDQ =new ArrayList <String> ();
    // SNDQ;
     
    public int partialno;
   
   public  void SharingGraph()
   {
      this.rootno=-1;
      this.queryno=0;
      this.parentno=-1;
      this.level=0;
      this.actulresult =0;
      this.sharedresult=0;
      this.isshare=true;
     // this.subqueries.add(100); 
      this.query="";
      this.isroot=false;
      this.overlapping="";
   }
   public  void print_GraphSharing(SharingGraph sg)
   {
     //String space="   ";
     System.out.println("--------------------------");
     //System.out.println(this.subqueries.get(0));
     System.out.println("quey no "+ (sg.queryno+1)+" parent no "+(sg.parentno+1)+" level"+sg.level); 
     //System.out.println(this.sharingtype+ space+this.actualresults+space+this.sharedresult);
   }
   
   
   
   public  static void printFinalOutputQueryList(List <String> querylist)
   {
      // System.out.println("---------------Outputquerylist--------------------"+querylist.getClass());
     for(int i=0; i<querylist.size();i++)
     {
        System.out.println(querylist.get(i)); 
     }
   }
    
    
   public  static void print_ArrayofGraphSharing(SharingGraph SharingGraph_arr [], int len)
   {
       System.out.println("---------------SharingGraph Array--------------------");
     for(int i=0; i<len;i++)
     {
         //System.out.println("hello"); 
         String s="quey no= "+ (SharingGraph_arr[i].queryno+1);
         s+=" parent no "+(SharingGraph_arr[i].parentno+1);
         s+=" root no "+(SharingGraph_arr[i].rootno+1);
         s+=" level "+SharingGraph_arr[i].level;
         s+=" shared "+SharingGraph_arr[i].isshare;
         s+=" type "+SharingGraph_arr[i].sharingtype+"...";
         s+=SharingGraph_arr[i].overlapping;
         s+="..."+SharingGraph_arr[i].Fullquery;
         if(SharingGraph_arr[i].sharingtype.equals("P"))
         {
              for(int j=0; j<SharingGraph_arr[i].subqueries.size();j++)
              {
                  String ss=SharingGraph_arr[i].queryno+j+" nonderived query ";
                  ss+=SharingGraph_arr[i].subqueries.get(j);
                  System.out.println(ss); 
              }
         }
         System.out.println(s); 

       }
   }
    public  static void print_orignal_queries(SharingGraph SharingGraph_arr [], int len)
   {
       System.out.println("---------------SharingGraph Original query--------------------");
     for(int i=0; i<len;i++)
     {
         //System.out.println("hello");  
         System.out.println("quey no= "+ (SharingGraph_arr[i].queryno+1)+" Query "+SharingGraph_arr[i].query);

       }
   }
    
    public  static void print_Finaloutput(SharingGraph SharingGraph_arr [],query_tokens [] query_tokens_arr,  int len, double rows_num_in_10_million)
   {
       System.out.println("---------------Finaloutput New queries--------------------");
       double Total_datasize_RHDFS_HiveReused=0;
       double Total_datasize_MB=0;
       double Total_tuplesize=0;
       double lineitem_datasize_MB=(1189.117*rows_num_in_10_million) ; // size of  table lineitem_10m  millions;
     for(int i=0; i<len;i++)
     {
     //System.out.println("hello"); 
         String s="Q"+ (SharingGraph_arr[i].queryno+1);
         s+=" parent no "+(SharingGraph_arr[i].parentno+1);
         s+=" level "+SharingGraph_arr[i].level;
         s+=" shared "+SharingGraph_arr[i].isshare;
         s+=" type "+SharingGraph_arr[i].sharingtype+"...";
         s+=SharingGraph_arr[i].overlapping;
         if( SharingGraph_arr[i].isroot )
             //take whole line item size in MB is 1187.84
         SharingGraph_arr[i].reused_datasize= lineitem_datasize_MB;//query_tokens_arr[i].datasize_in_MB;
      
       //  else if( !SharingGraph_arr[i].isshare )
             //take whole line item size in MB is 1187.84
         //SharingGraph_arr[i].reused_datasize= lineitem_datasize_MB;//query_tokens_arr[i].datasize_in_MB;
    
         else
             // take data size of parent which reused it
             SharingGraph_arr[i].reused_datasize=query_tokens_arr[SharingGraph_arr[i].parentno].datasize_in_MB;
       
         s+="...reused data size in MB "+SharingGraph_arr[i].reused_datasize;
         s+="..."+SharingGraph_arr[i].Fullquery;
         Total_datasize_RHDFS_HiveReused+=SharingGraph_arr[i].reused_datasize;
         /* here data size of each query if dived by rows_num_in_10_million 
          * it gives averagre of tuplesize for all queries
          * 
          */
         Total_datasize_MB+=query_tokens_arr[i].datasize_in_MB; 
         Total_tuplesize+=query_tokens_arr[i].TupleSize;       
         System.out.println(s); 
         //System.out.println(SharingGraph_arr[i].subqueries.size());
       /*
         if(SharingGraph_arr[i].sharingtype.equals("P"))
         {
              for(int j=0; j<SharingGraph_arr[i].partailqueries.size();j++)
                   //for(int j=SharingGraph_arr[i].partailqueries.size(); j>=0;j--)
              {
                  String ss="Q"+ (SharingGraph_arr[i].queryno+1)+" nonderived query "+(j+1);
                  ss+=".."+SharingGraph_arr[i].partailqueries.get(j);
                  System.out.println(ss); 
              }// end For j
         }
        */

       } //end For i
     //multiply size of one table of line item by number of queries which use to retrive the source file
     double Total_datasize_RHDFS_Naive_approch=lineitem_datasize_MB*len;
     double  Data_Reduction=(Total_datasize_RHDFS_Naive_approch-Total_datasize_RHDFS_HiveReused)/Total_datasize_RHDFS_Naive_approch;
     System.out.println("Total_datasize_RHDFS_Naive_approch in GB= "+Total_datasize_RHDFS_Naive_approch/1024);
     System.out.println("Total_datasize_RHDFS_HiveReused in GB= "+Total_datasize_RHDFS_HiveReused/1024);
     System.out.println("Data_Reduction in GB= "+Data_Reduction);
     System.out.println("Total_datasize_GB="+Total_datasize_MB/1024);
     System.out.println("Total_tuplesize in Byte="+Total_tuplesize);
    
   } // end of print_new_queries
    
    /*
     * this function to get sub queries  
     */
    public   List <String> getsubquerylist(SharingGraph SharingGraph_arr [])
    {
        List <String> templist= new ArrayList <String> ();
        templist.add(this.Fullquery);
        for(int i=0; i<this.subqueries.size();i++)
          {
          int childnum=this.subqueries.get(i);
          int index=this.getindexofquery(SharingGraph_arr, childnum);
          templist.add(SharingGraph_arr[index].Fullquery);
        
         if(SharingGraph_arr[index].sharingtype.equals("P"))
         {
              for(int j=0; j<SharingGraph_arr[i].SNDQ.size();j++)
              {
                  //String s="quey no= "+ (SharingGraph_arr[i].queryno+1)+" nonderived query "+(j+1);
                  String s=SharingGraph_arr[index].SNDQ.get(j);
                  templist.add(s);
              }// end For j
         } // enf if
     } //end For  i
       return templist;
    }// end getsubquerylist
    
     public    static int getmaxLevel(SharingGraph SharingGraph_arr [], int len)
    {
        int maxlevel=0;
      for(int i=0; i<len;i++)
        if(SharingGraph_arr[i].level>=maxlevel)
            maxlevel=SharingGraph_arr[i].level;
      return maxlevel;
    }
     public   static List <String> getsubquerylistOrederedByLevel(SharingGraph SharingGraph_arr [], int len)
    {
        List <String> templist= new ArrayList <String> ();
        String s="";
        int maxlevel=getmaxLevel(SharingGraph_arr, len);
    for(int l=0;l<maxlevel;l++)    
   
    {   
        for(int i=0; i<len;i++)
     {
        if(SharingGraph_arr[i].level==l)
        
        {
            s=String.valueOf(SharingGraph_arr[i].level)+"...";
            s+=SharingGraph_arr[i].Fullquery;
            templist.add(s);
            
         if(SharingGraph_arr[i].sharingtype.equals("P"))
         {
              for(int j=0; j<SharingGraph_arr[i].SNDQ.size();j++)
              {
                  //String s="quey no= "+ (SharingGraph_arr[i].queryno+1)+" nonderived query "+(j+1);
                 s=String.valueOf(SharingGraph_arr[i].level)+"...";
                 s+=SharingGraph_arr[i].SNDQ.get(j);
                 templist.add(s);
              }// end For j
         } // enf if
     } //end For  i
     } // end if level
    }// end for l
       return templist;
    }// end getsubquerylist
     
     
     
   
   public   static List <String> getConcurrentQueryList(SharingGraph SharingGraph_arr [], int len) throws IOException
    {
        List <String> templist= new ArrayList <String> (); 
         List <String> buffer= new ArrayList <String> ();  
        //System.out.println("------------------getConcurrentQueryList----------------");
        for(int i=0; i<len;i++)
             {
                 // move non share query to concurrent list
                if(!SharingGraph_arr[i].isshare)
                 
                {
                templist.add(SharingGraph_arr[i].Fullquery);
                buffer.add(SharingGraph_arr[i].Fullquery+"\n");
                SharingGraph_arr[i].isconcurrent=true;
                //String s="Q"+(SharingGraph_arr[i].queryno+1);
                //s+="..."+SharingGraph_arr[i].Fullquery;
                //System.out.println(s);
                }
                
                /*move sub query with no childern and fully sharing
                 * ( its results is ready in parent table) to be concurrent
                 * 
                 */
                if((SharingGraph_arr[i].subqueries.isEmpty())&&SharingGraph_arr[i].sharingtype.equals("F"))
                {
                templist.add(SharingGraph_arr[i].Fullquery);
                buffer.add(SharingGraph_arr[i].Fullquery+"\n");
                 SharingGraph_arr[i].isconcurrent=true;
               // String s="Q"+(SharingGraph_arr[i].queryno+1);
                //s+="..."+SharingGraph_arr[i].Fullquery;
                //System.out.println(s);
                }
               
                  
                if((SharingGraph_arr[i].isconcurrent) &&SharingGraph_arr[i].sharingtype.equals("P"))
                {
                templist.add(SharingGraph_arr[i].Fullquery);
                buffer.add(SharingGraph_arr[i].Fullquery+"\n");
                 SharingGraph_arr[i].isconcurrent=true;
               // String s="Q"+(SharingGraph_arr[i].queryno+1);
                //s+="..."+SharingGraph_arr[i].Fullquery;
                //System.out.println(s);
                }
               
             } //end For  i
     ReadWriteFiles.writeBuffered("Concurrent", buffer, (1024*4));

       return templist;
    }// end getConcurrentQueryList  
   
   public   static  List<List<String>> getAllConcurrentQueryList(SharingGraph SharingGraph_arr [], int len) throws IOException
    {
        List<List<String>> Allquerylist= new ArrayList<List<String>> ();
        List <String> buffer= new ArrayList <String> ();  
        String s="";
        System.out.println("------------------getAllConcurrentQueryList----------------");
        for(int i=0; i<len;i++)
             {
                  List <String> templist= new ArrayList <String> ();  
                 /*
               * move to Nonconcurrent list when it is root as first
               */
                if(SharingGraph_arr[i].isroot)//||!SharingGraph_arr[i].isshare)
                 
                {
                  s="Q"+(SharingGraph_arr[i].queryno+1);
                  s+=" parent "+(SharingGraph_arr[i].parentno+1);
                  s+=" level "+(SharingGraph_arr[i].level);
                  s+=" ... "+(SharingGraph_arr[i].Fullquery);
                  System.out.println(s);
                  templist.add(SharingGraph_arr[i].Fullquery);
                  //buffer.add("#\n");
                  buffer.add(SharingGraph_arr[i].Fullquery+"\n");
                 // buffer.add();
              for(int j=0; j<SharingGraph_arr[i].subqueries.size();j++)
                  //for(int i=this.subqueries.size()-1; i>=0;i--)
                  {
                      int subqueryno=SharingGraph_arr[i].subqueries.get(j); 
                      int indexsubqueryno=SharingGraph_arr[i].getindexofquery(SharingGraph_arr, subqueryno);

                      /*
                       * move to Nonconcurrent list when it has childern
                       */
                     //if(!SharingGraph_arr[indexsubqueryno].subqueries.isEmpty()||SharingGraph_arr[indexsubqueryno].sharingtype.equals("P"))
                     // get sub query
                      {
                     templist.add(SharingGraph_arr[indexsubqueryno].Fullquery);
                     buffer.add(SharingGraph_arr[indexsubqueryno].Fullquery+"\n");
                     
                     s="Q"+(subqueryno+1);
                     s+=" parent "+(SharingGraph_arr[indexsubqueryno].parentno+1);
                     s+=" level "+(SharingGraph_arr[indexsubqueryno].level);
                     s+=" ... "+(SharingGraph_arr[indexsubqueryno].Fullquery);
                     System.out.println(s);
                   
                     /*
                       * move to Nonconcurrent list when its partailqueries then unoin query
                       */
                     }
                     
                     if(SharingGraph_arr[indexsubqueryno].sharingtype.equals("P"))
                     {
                     
                     s="Q"+(indexsubqueryno+1)+"1 Non derivied.."+SharingGraph_arr[indexsubqueryno].SNDQ.get(0);
                     System.out.println(s);
                     templist.add(SharingGraph_arr[indexsubqueryno].SNDQ.get(0));
                     buffer.add(SharingGraph_arr[indexsubqueryno].SNDQ.get(0)+"\n");
                    
                     s="Q"+(indexsubqueryno+1)+"2 Union partailqueries";
                     System.out.println(s);
                     //templist.add("Union");
                    // buffer.add("Union"+"\n");
                     }
                     //int non
                    
                     
            }// end for j 
             buffer.add("#\n");
            } // end if
            Allquerylist.add(templist);  
            //buffer.add("\n");
           //buffer.add("#\n");
            } //end For  i to get roots and its childern
    
       for(int i=0; i<len;i++)
             {
                  List <String> templist= new ArrayList <String> ();  
                 /*
               * move to Nonconcurrent list when it is root as first
               */
                if(!SharingGraph_arr[i].isshare)
                 
                {
                  s="Q"+(SharingGraph_arr[i].queryno+1);
                  s+=" parent "+(SharingGraph_arr[i].parentno+1);
                  s+=" level "+(SharingGraph_arr[i].level);
                  s+=" ... "+(SharingGraph_arr[i].Fullquery);
                  System.out.println(s);
                  //buffer.add("#\n");
                  templist.add(SharingGraph_arr[i].Fullquery);
                  //buffer.add("#");
                  buffer.add(SharingGraph_arr[i].Fullquery+"\n#\n");
                 // buffer.add("#");
                  Allquerylist.add(templist);
                }
             } // end for i to get non share
       ReadWriteFiles.writeBuffered("SharedHive", buffer, (1024*4));
       return Allquerylist;
    }// end getConcurrentQueryList  
     
     
     public   static List <String> getNonConcurrentQueryList(query_tokens [] query_tokens_arr, SharingGraph SharingGraph_arr [], int len)//, double [] estimated_cost_nonconcurrentlist)
    {
        List <String> templist= new ArrayList <String> (); 
        String s="";
       
        System.out.println("------------------getNonConcurrentQueryList----------------");
        for(int i=0; i<len;i++)
             {
                 /*
               * move to Nonconcurrent list when it is root as first
               */
                if(SharingGraph_arr[i].isroot)
                 
                {
                    
                  s="Q"+(SharingGraph_arr[i].queryno+1);
                  s+=" parent "+(SharingGraph_arr[i].parentno+1);
                  s+=" level "+(SharingGraph_arr[i].level);
                  s+=" ... "+(SharingGraph_arr[i].Fullquery);
                  System.out.println(s);
                  templist.add(SharingGraph_arr[i].Fullquery);
                  
                  // here to build temp
                  String TempSql="";
                  TempSql= CreateNDQ(query_tokens_arr,SharingGraph_arr[i].queryno,len);
                  if(!TempSql.isEmpty())
                  {
                  TempSql="create table  NDQ_t  as "+ TempSql;
                  templist.add(TempSql);
                  }
                  // if leaf partial query leave only excute partialpart and NDQ is excuted by NDQ
                  if (SharingGraph_arr[i].subqueries.isEmpty())SharingGraph_arr[i].isleaf=true;
                 // to get subquery 
                  for(int j=0; j<SharingGraph_arr[i].subqueries.size();j++)
                  //for(int i=this.subqueries.size()-1; i>=0;i--)
                  {
                      int subqueryno=SharingGraph_arr[i].subqueries.get(j); 
                      int indexsubqueryno=SharingGraph_arr[i].getindexofquery(SharingGraph_arr, subqueryno);
                     
                      /*s="Q"+(SharingGraph_arr[indexsubqueryno].queryno+1);
                       s+="--"+ SharingGraph_arr[indexsubqueryno].Fullquery;
                      System.out.println(s);*/
                      /*
                       * move to Nonconcurrent list if partail or Full
                       */
                    //if(!SharingGraph_arr[indexsubqueryno].subqueries.isEmpty()&&SharingGraph_arr[indexsubqueryno].sharingtype.equals("F"))
                  if(!SharingGraph_arr[indexsubqueryno].subqueries.isEmpty() && SharingGraph_arr[indexsubqueryno].sharingtype.equals("F"))
                   //if(SharingGraph_arr[indexsubqueryno].subqueries.size()<2 && SharingGraph_arr[indexsubqueryno].sharingtype.equals("F"))
                     
                     {
                     templist.add(SharingGraph_arr[indexsubqueryno].Fullquery);
                     s="Q"+(subqueryno+1);
                     s+=" parent "+(SharingGraph_arr[indexsubqueryno].parentno+1);
                     s+=" level "+(SharingGraph_arr[indexsubqueryno].level);
                     s+=" ... "+(SharingGraph_arr[indexsubqueryno].Fullquery);
                     System.out.println(s);
                      /*
                       * move to Nonconcurrent list when its partailqueries then unoin query
                       */
                     }
                     
                     
                   // if(!SharingGraph_arr[indexsubqueryno].subqueries.isEmpty()&&SharingGraph_arr[indexsubqueryno].sharingtype.equals("P"))
                   
                    if(SharingGraph_arr[indexsubqueryno].sharingtype.equals("P")
                            && !SharingGraph_arr[indexsubqueryno].isconcurrent)
                         
                     {
                     
                     s="Q"+(indexsubqueryno+1)+"1 Non derivied.."+SharingGraph_arr[indexsubqueryno].SNDQ.get(0);
                     System.out.println(s);
                     //templist.add(SharingGraph_arr[indexsubqueryno].partailqueries.get(0));
                     s="Q"+(indexsubqueryno+1)+"2 Union partailqueries";
                     //templist.add(SharingGraph_arr[indexsubqueryno].partailqueries.get(1));
                     templist.add(SharingGraph_arr[indexsubqueryno].Fullquery);
                     //HiveReused.estimated_cost_nonconcurrentlist.add(e)
                     System.out.println(s);
                     //templist.add("Union");
                     }
                     //int non
                     
                     
            } // end if
            }// end for j          
            } //end For  i
    
       return templist;
    }// end getNonConcurrentQueryList  
     
      public static String  CreateNDQ( query_tokens [] query_tokens_arr,int parent_no, int len)
   {
       /*
        * this fuction to store non dervied data into Temp
        */
   // find LeftOperand for temp as minum =min(LeftOperands)
      double MinLeftOperand=query_tokens_arr[0].LeftOperand; 
       for(int i=1; i<len;i++)
       {
           if((query_tokens_arr[i].LeftOperand<MinLeftOperand))// && i!=parent_no)
               MinLeftOperand=query_tokens_arr[i].LeftOperand;
       }
       
       // find LeftOperand for temp as minum =min(LeftOperands)
      double MaxRightOperand=query_tokens_arr[0].RightOperand; 
       for(int i=1; i<len;i++)
       {
           if((query_tokens_arr[i].RightOperand>=MaxRightOperand))//&& i!=parent_no )
           MaxRightOperand=query_tokens_arr[i].RightOperand;
       }
      //MinLeftOperand=MinLeftOperand/100;
      //MaxRightOperand=MaxRightOperand/100;
      System.out.println("MinLeftOperand= "+ MinLeftOperand+ " MaxRightOperand= "+ MaxRightOperand);
      if((MinLeftOperand==query_tokens_arr[parent_no].LeftOperand)&&(MaxRightOperand==query_tokens_arr[parent_no].RightOperand))
          return "";
      // write Temp Table Query
      //sql=select  count(*) from lineitem_100t where l_discount between 0.01 and 0.03 or l_discount between 0.05 and 0.06 
      String between1= " between "+Double.toString(MinLeftOperand/100)+" and "+Double.toString((query_tokens_arr[parent_no].LeftOperand-1)/100);
      String between2= " between "+Double.toString((query_tokens_arr[parent_no].RightOperand+1)/100)+" and "+Double.toString(MaxRightOperand/100);
      String TempQuery=query_tokens_arr[parent_no].query;
     // TempQuery=
      int index_where=TempQuery.indexOf("where");
      int index_between=TempQuery.indexOf("between");
      // to get coloum name such as l_discount
      String coloum_name=TempQuery.substring(index_where+5, index_between);
      
      //get sub of coloumns from parent query
     String  sql=TempQuery.substring(0, index_where+5); 
     sql+=coloum_name+between1+ " OR "+coloum_name+ between2;
     System.out.println("temp query .."+sql);
      return sql;
      
      
   }
     
     public  static String  union(String q1, String q2, int query_num) throws SQLException, ClassNotFoundException, IOException
 {
     /*
      * unoin statment
      * CREATE TABLE t1t22 AS SELECT log.L_ORDERKEY, log.L_DISCOUNT FROM 
      * ( SELECT l1.L_ORDERKEY, l1.L_DISCOUNT FROM 
      * t1 l1 UNION ALL SELECT l2.L_ORDERKEY, l2.L_DISCOUNT FROM t2 l2 ) log
      */
     String s="";
     String columns="";
     int index_select_keyword=0;
     int index_from_keyword=0;
     int index_where_keyword=0;
     String table_name="";
     //int index_between_keyword=0;
     //int index
    
     
     // get parsed of Q1 
     File sqlfile = new File ("one_query.sql");
     query_tokens qt1=new query_tokens();qt1.query_tokens();
     index_select_keyword=q1.indexOf("select"); 
     qt1.query=q1.substring(index_select_keyword);
     ReadWriteFiles.write_single_query( qt1.query);
     sqlfile = new File ("one_query.sql");
     qt1.parsing(q1, sqlfile);
     
     // get parsed of Q2 
     query_tokens qt2=new query_tokens();qt2.query_tokens();
     index_select_keyword=q2.indexOf("select"); 
     qt2.query=q2.substring(index_select_keyword);
     ReadWriteFiles.write_single_query( qt2.query);
     sqlfile = new File ("one_query.sql");
     qt2.parsing(q2, sqlfile);
     
     // give label  all for unoined table
     for(int i=0;i<qt1.columns_list.size();i++ )
     columns+="log."+qt1.columns_list.get(i)+",";
     
     columns=columns.substring(0, columns.length()-1);
     s="create table  t"+(query_num+1)+" as select "+columns;
      // give qt1 for first table
     columns=columns.replaceAll("log", "qt1");
     index_from_keyword=q1.indexOf("from");
     
     table_name=qt1.table_name;//q1.substring(13,index_from_keyword);
     s+=" From ( select "+columns+" FROM "+table_name+" qt1  where "+qt1.where_clauses;
     
      // give qt1 for second table
     columns=columns.replaceAll("qt1", "qt2");
     //index_as_keyword=q2.indexOf("as");
     table_name=qt2.table_name;//q2.substring(13,index_as_keyword);
     s+=" UNION ALL SELECT "+columns+" FROM " +table_name+" qt2 where "+ qt2.where_clauses;
     s+=" ) log";
  

System.out.println(s);
//s1=" create table t8 as select from t1";
return s;

 }
   public  static void update_levels(SharingGraph SharingGraph_arr [], int SharingGraph_arr_len)
    {
       // System.out.println ("in update_levels ");
      for(int i=0; i<SharingGraph_arr_len;i++)
     {
         int child_level=SharingGraph_arr[i].level;
         int parentnum=SharingGraph_arr[i].parentno;
         int parent_level=0;
         if(parentnum!=-1)
         {
          parent_level=SharingGraph_arr[parentnum].level;
         
        // System.out.println(" Q"+(i+1)+" parentnum "+(parentnum+1));
            // System.out.println(" before SharingGraph_arr.level "+(i+1)+ "...."+SharingGraph_arr[i].level);
    
         if(child_level!=(parent_level+1))
         {
             SharingGraph_arr[i].level=parent_level+1;
            // System.out.println(" after SharingGraph_arr.level "+(i+1)+ "...."+SharingGraph_arr[i].level);
            //update  childofchildquerynum levels
             //if( SharingGraph_arr[i].subqueries.size()>0)
              //   System.out.println("update  childofchildquerynum levels");
             for(int j=0; j<SharingGraph_arr[i].subqueries.size();j++)
          {
            int childofchildquerynum= SharingGraph_arr[i].subqueries.get(j) ;
            SharingGraph_arr[childofchildquerynum].level= SharingGraph_arr[i].level+1;
          }
            
         }
         }
     }   
    }
   
   public  void get_actulresult()
   {
       
   }
   
    public boolean ismychildern(int querynum)
    {
        //boolean ismychidern=false;
         for(int i=0; i<this.subqueries.size();i++)
         {
             int mysubquerynum=this.subqueries.get(i);
             if(mysubquerynum==querynum) return true;
         }
      return false;       
    }
   
     public  static void  sortAllsubquries(SharingGraph SharingGraph_arr [], int SharingGraph_arr_len)
     {
       for(int i=0;i< SharingGraph_arr_len;i++)
       {
          // if(SharingGraph_arr[i].isroot)
           int index=SharingGraph_arr[i].getindexofquery(SharingGraph_arr, SharingGraph_arr[i].queryno);
               SharingGraph_arr[i].sortsubquries(SharingGraph_arr,index);
       }
       
     }
    
     public void  sortsubquries(SharingGraph SharingGraph_arr [],int query_num)
        {
        
        //convert list to array
        Integer arraysubquery []= new  Integer [SharingGraph_arr[query_num].subqueries.size()];
        arraysubquery=SharingGraph_arr[query_num].subqueries.toArray(arraysubquery);
        
         for(int i=0; i<arraysubquery.length;i++)
         {
             //System.out.println("in sortsubquries "+(arraysubquery[i]+1));      
               
             for(int j=0; j<SharingGraph_arr[query_num].subqueries.size();j++) 
            {
               
                if(SharingGraph_arr[arraysubquery[i]].level<=SharingGraph_arr[arraysubquery[j]].level)
                {
                    int temp=arraysubquery[i];
                    arraysubquery[i]=arraysubquery[j];
                    arraysubquery[j]=temp;
                }
            }// end for j   
    } //end for i
         // convert sorted array into list
         SharingGraph_arr[query_num].subqueries.clear();
         SharingGraph_arr[query_num].subqueries=Arrays.asList(arraysubquery);
    }
    
    public  static void printroots(SharingGraph SharingGraph_arr [], int SharingGraph_arr_len)
    {
    
        for(int i=0; i<SharingGraph_arr_len;i++)            
                if (SharingGraph_arr[i].isroot) 
                {
                
                 System.out.println("I am root " + (SharingGraph_arr[i].queryno+1));
                }
        return;
    }
    
    
   public  static void Level_getsubqueries(SharingGraph SharingGraph_arr [], int SharingGraph_arr_len, int levelno)
    {
    
        for(int i=0; i<SharingGraph_arr_len;i++)            
                if (SharingGraph_arr[i].isroot) 
                {
                
                 System.out.println("I am root " + (SharingGraph_arr[i].queryno+1));
                }
        return;
    }
   
   
   
   public  static void printnotshare(SharingGraph SharingGraph_arr [], int SharingGraph_arr_len)
    {
    
        for(int i=0; i<SharingGraph_arr_len;i++)            
                if (!SharingGraph_arr[i].isshare) 
                {
                
                 System.out.println("I am concurrent " + (SharingGraph_arr[i].queryno+1));
                }
        return;
    }
    
   
   
   public int getindexofquery(SharingGraph SharingGraph_arr [], int querynum)
    {
     for(int i=0; i<SharingGraph_arr.length;i++)
         if(SharingGraph_arr[i].queryno==querynum) return i;
     return 0;
    }
    
    public   static void update_grandfathers(SharingGraph SharingGraph_arr [], int SharingGraph_arr_len)
    {
        int index=0;
        int son=0;
        int father=0;
        int grandfather=0;
        int templevel=0;
         for(int i=0; i<SharingGraph_arr_len;i++)
         {
            //System.out.println(" ************for Q..." +(i+1));
            son=SharingGraph_arr[i].queryno; 
            father=SharingGraph_arr[i].parentno;
            templevel=SharingGraph_arr[i].level-1;
            if(templevel>-1)
            {
                System.out.println("update_grandfathers "+ (son+1));
         for(int j=templevel; j>0;j--)
            
        {
             
            int fatherindex=SharingGraph_arr[i].getindexofquery(SharingGraph_arr, father);
            grandfather=SharingGraph_arr[fatherindex].parentno;
          
            //add father
            index= SharingGraph_arr[grandfather].subqueries.indexOf(fatherindex);
             if(index==-1)
            {
            SharingGraph_arr[grandfather].subqueries.add(fatherindex);
            System.out.println("add father" +(fatherindex+1));
            }
             
            // add son 
            index= SharingGraph_arr[grandfather].subqueries.indexOf(son);

            if(index==-1)
            {
            SharingGraph_arr[grandfather].subqueries.add(son);
            System.out.println("add son" +(son+1) +" to grandfather "+  (grandfather+1));
            }
            father=grandfather;
            
        }// end for j
        
    }// if
    } //end for i
         
    }//end function
    
    
    public  void addme(SharingGraph SharingGraph_arr [])
    {
           int parentindex=this.getindexofquery(SharingGraph_arr, this.queryno);//getindexofquery(this.parentno);
            
           //add me
            int index= SharingGraph_arr[parentindex].subqueries.indexOf(this.queryno);
             if(index==-1)
            {
            SharingGraph_arr[parentindex].subqueries.add(this.queryno);
            System.out.println("add me" +(this.queryno+1));
            }
    }
    
    
    public  void getsubqueries( SharingGraph child, SharingGraph [] SharingGraph_arr)
      {
         // System.out.println("getsubqueries for "+(child.queryno+1));
          //System.out.println("child.subqueries.size() "+(child.subqueries.size()));
          
          int L_parentno=child.parentno;
          for(int i=0; i<child.subqueries.size();i++)
          {
            int childquerynum= child.subqueries.get(i) ;
          // childer is not added before
            if(SharingGraph_arr[L_parentno].subqueries.indexOf(childquerynum)==-1)
           {
             SharingGraph_arr[L_parentno].subqueries.add(childquerynum);
             System.out.println("add subqueries for "+(SharingGraph_arr[L_parentno].queryno+1)+" child is "+(childquerynum+1));
           }
            
            //update level for sub queries
            //if(i)
            int index=SharingGraph_arr[L_parentno].getindexofquery(SharingGraph_arr, childquerynum);
            
            SharingGraph_arr[index].level=SharingGraph_arr[index].level+1;
          }
          // also add me
          SharingGraph_arr[L_parentno].subqueries.add(child.queryno);
         
          return ;
      }
    
    
    public void printsubqueries(SharingGraph [] SharingGraph_arr )
      {
           System.out.println("-------------------printsubqueries Q"+(this.queryno+1)+"------------------");
            String s="Q"+(this.queryno+1);
            s+=" parent "+(this.parentno+1);
             s+=" level "+(this.level);
             s+=" ... "+(this.Fullquery);
             System.out.println(s);
         for(int i=0; i<this.subqueries.size();i++)
          //for(int i=this.subqueries.size()-1; i>=0;i--)
          {
              int subqueryno=this.subqueries.get(i); 
             int indexsubqueryno=getindexofquery(SharingGraph_arr, subqueryno);
             s="Q"+(subqueryno+1);
             s+=" parent "+(SharingGraph_arr[indexsubqueryno].parentno+1);
             s+=" level "+(SharingGraph_arr[indexsubqueryno].level);
             s+=" ... "+(SharingGraph_arr[indexsubqueryno].Fullquery);
             System.out.println(s);
          }
      }          
   
     public  static void  setroots(SharingGraph [] SharingGraph_arr, int len )
      {  System.out.println("in set root");
          for(int i=0; i<len;i++)
          {
              //only set root for all sub queries
              if(SharingGraph_arr[i].isroot)
               for(int j=0; j<SharingGraph_arr[i].subqueries.size();j++)
          {
              int subqueryno=SharingGraph_arr[i].subqueries.get(j); 
              int indexsubqueryno=SharingGraph_arr[i].getindexofquery(SharingGraph_arr, subqueryno);
               SharingGraph_arr[indexsubqueryno].rootno=SharingGraph_arr[i].queryno;
              
           System.out.println("Q"+(subqueryno+1)+" root "+(SharingGraph_arr[subqueryno].rootno+1)+" level "+(SharingGraph_arr[subqueryno].level));
          } // end for j
          } // end of for i
      }   
   
   
   public static void rewrite( query_tokens []query_tokens_arr, SharingGraph [] SharingGraph_arr, int SharingGraph_arr_len) throws SQLException, IOException, ClassNotFoundException
   {
    
       
     //int parentindex=getindexofquery(SharingGraph_arr,querynum);
    for(int i=0; i<SharingGraph_arr_len;i++) 
  {
      // int queryindex=SharingGraph_arr[i].getindexofquery(SharingGraph_arr,SharingGraph_arr[i].queryno);
     int queryindex=i;
     String myquery="";
     String q1="";
     String q2="";
  //  DeleteTables.drop_table("t"+String.valueOf(queryindex+1));    
    myquery="create table t"+(queryindex+1)+" as ";
          
    // System.out.println("in rewrite"+ queryindex);
     // if is root dont need to rewrite
     if(SharingGraph_arr[queryindex].isroot) 
     {
         
         myquery+=SharingGraph_arr[queryindex].query;
         myquery=SharingGraph_arr[queryindex].modifyquery(myquery);
         SharingGraph_arr[queryindex].Fullquery=myquery;

         //System.out.println( SharingGraph_arr[queryindex].Fullquery);
        
         //return;
     }
     
     // if is not share dont need to rewrite
     else if(!SharingGraph_arr[queryindex].isshare) 
     {
         
         SharingGraph_arr[queryindex].Fullquery=SharingGraph_arr[queryindex].query;
         myquery+=SharingGraph_arr[queryindex].query;
         myquery=SharingGraph_arr[queryindex].modifyquery(myquery);
         SharingGraph_arr[queryindex].Fullquery=myquery;
     }
     
     else
     {
     // else rewrite Full and Partail
     int myqueryno=SharingGraph_arr[queryindex].queryno;
     String parent_output= "t"+(SharingGraph_arr[queryindex].parentno+1); 
     String table=query_tokens_arr[queryindex].table_name;
      
      // get columns list
      String columns=query_tokens_arr[myqueryno].columns_list.toString();
      // to remove bracktes
      columns=columns.substring(1, columns.length()-1);
      // get where statment
      Filter F= new Filter();
      F.query=SharingGraph_arr[queryindex].query;
      F.getWhereClauseToken(F.query);
      String where=" where "+F.column1+ " between "+ SharingGraph_arr[queryindex].overlapping;
      
      // write myquery
       myquery+="select "+columns+ " from "+ parent_output+where ;
      
      if(SharingGraph_arr[queryindex].sharingtype.equals("F")) 
      {  
          myquery=SharingGraph_arr[queryindex].modifyquery(myquery);
          SharingGraph_arr[queryindex].Fullquery=myquery;
      }
      
      
      // generate  non derived boundery
      
      if(SharingGraph_arr[queryindex].sharingtype.equals("P"))  
      {
          myquery="select "+columns+ " from "+ parent_output+where ;

          //myquery=myquery.replaceAll(" as", "1 as");
          myquery=SharingGraph_arr[queryindex].modifyquery(myquery);
          // add first partail
          SharingGraph_arr[queryindex].SNDQ.add(myquery);
         
          
          //  write non derived
          String overlap=SharingGraph_arr[queryindex].overlapping;
         
          int index=overlap.indexOf("and");
           
          String LeftOperand=overlap.substring(0, index);
          String RightOperand=overlap.substring(index+4, overlap.length());
          
          //System.out.println("overlap .."+overlap+" LeftOperand "+LeftOperand +"RightOperand "+RightOperand);
          int low1=0,low2=0,low3=0; 
          int high1=0,high2=0,high3=0; 
          
          low1=Integer.parseInt(LeftOperand.trim());
          high1=Integer.parseInt(RightOperand.trim());
          
          low2=F.LeftOperand-low1;
          high2=F.RightOperand-high1;
                 
          if(low2==0)
          {
            low3= high1+1;//F.RightOperand;
            high3=F.RightOperand;
          }
          
          if(high2==0)
          {
            low3= F.LeftOperand;//F.RightOperand;
            high3=low1-1;
          }
          
       String between=String.valueOf(low3)+" and "+String.valueOf(high3);
       where=" where "+F.column1+ " between "+ between;
       //myquery="create table t"+(queryindex+1)+"2 as ";
       myquery="";
       table="NDQ";
       myquery+="select "+columns+ " from "+ table+where ;

       myquery=SharingGraph_arr[queryindex].modifyquery(myquery);
       SharingGraph_arr[queryindex].SNDQ.add(myquery);
       
       q1=SharingGraph_arr[queryindex].SNDQ.get(0);
       q2=SharingGraph_arr[queryindex].SNDQ.get(1);
       // now union  partail queries
       myquery=union(q1,q2,SharingGraph_arr[queryindex].queryno);
       // for NDQ
       //myquery=q1+";"+q2;
       SharingGraph_arr[queryindex].Fullquery=myquery;
      }
      //if (queryindex==1)
      System.out.println("Query no "+(queryindex+1)+".partail_1 "+q1);
      System.out.println("Query no "+(queryindex+1)+".partail_2 "+q2);
      System.out.println("Query no "+(queryindex+1)+".whole "+myquery);
      // get unoin
      
     }// end else
     
  }// end for i
         
   }// end of rewrite
   
  public  static String modifyquery(String querystr )
   {
     String modifyquerystr="";
     
    int indexand=querystr.lastIndexOf("and");
    int indexbetween=querystr.indexOf("between"); 
    //double LeftOperand=0;
    //double RightOperand=0;
    String LeftOperandStr="";
    String RightOperandStr="";
    System.out.println(querystr);
    System.out.println("querystr.length "+querystr.length());
    System.out.println("indexbetween "+indexbetween);
    System.out.println("indexand "+indexand);
    LeftOperandStr=querystr.substring(indexbetween+8,indexand).trim();
    if(!LeftOperandStr.contains("."))
    LeftOperandStr=String.valueOf(Integer.parseInt(LeftOperandStr)*0.01);
    
    RightOperandStr=querystr.substring(indexand+4,querystr.length()).trim();
     if(!RightOperandStr.contains("."))
    RightOperandStr=String.valueOf(Integer.parseInt(RightOperandStr)*0.01); 
     
    modifyquerystr="between "+LeftOperandStr+" and "+String.valueOf(RightOperandStr);
     
   modifyquerystr=querystr.substring(0, indexbetween)+modifyquerystr; 
    System.out.println(modifyquerystr);
    return modifyquerystr;
   }
  
  
  
  public static void main(String[] args ) throws SQLException, ClassNotFoundException, IOException
    {
        SharingGraph sg=new SharingGraph();
        sg.SharingGraph();
        //sg.query="select L_ORDERKEY from lineitem_t2 where l_discount between 3 and 4";
         sg.query="between 3 and 4";
        sg.modifyquery(sg.query);
        String q1="select l_discount,L_ORDERKEY from t1 where l_discount between 3 and 7";
        String q2="select l_discount,L_ORDERKEY from t2 where l_discount between 3 and 7";
        union(q1,q2,3);
    }
   
   }// end of class
         
  

