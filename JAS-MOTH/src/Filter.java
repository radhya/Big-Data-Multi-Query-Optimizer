/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package SharedHive;

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
            //int overlaplen=0;
      //Q1.intersect(Q2);
      
        
            /*overlaplen=(Q1.intrsect_higherBound-Q1.intrsect_lowerBound)+1;
        //ol=Q1.overlp_count;
        {
            overlaplen=0;
        }
         System.out.println("intrsect_lowerBound= "+Q1.intrsect_lowerBound);
         System.out.println("intrsect_higherBound= "+Q1.intrsect_higherBound);
         System.out.println("ovelapping length="+ overlaplen);
        */
        //System.out.println(Q1 + " intersect(" + Q2 + ") = " + Q1.intersect(Q2));
         Q1.intersect(Q2);
        //if(Q1.intrsect_higherBound==0&& Q1.intrsect_lowerBound==0 )
       if(Q1.overlp_count<0)

         {
         // System.out.println("F1 is not subset from F2");
          overlap.intrsect_lowerBound=0;
          overlap.intrsect_higherBound=0;
          overlap.overlapp_type="N";

          return overlap;
          }
       
        else if(this.LeftOperand==F2.LeftOperand && this.RightOperand==F2.RightOperand)
         {
         //System.out.println("Full Sharing F1 is subset from F2");
         //Q1.intersect(Q2);
         
         //overlaplen=(Q1.intrsect_higherBound-Q1.intrsect_lowerBound)+1;
         overlap.intrsect_lowerBound=Q1.intrsect_lowerBound;
         overlap.intrsect_higherBound=Q1.intrsect_higherBound;
         overlap.overlapp_type="E";
         return overlap;
         }
       
        //  Full Sharing F1=[4-6] F2=[2-8] is 
         else if(this.LeftOperand>=F2.LeftOperand && this.RightOperand<=F2.RightOperand)
         {
         //System.out.println("Full Sharing F1 is subset from F2");
         //Q1.intersect(Q2);
         
         //overlaplen=(Q1.intrsect_higherBound-Q1.intrsect_lowerBound)+1;
         overlap.intrsect_lowerBound=Q1.intrsect_lowerBound;
         overlap.intrsect_higherBound=Q1.intrsect_higherBound;
         overlap.overlapp_type="F";
         return overlap;
         }
         
         
         //partial F1=[3-7] F2 is F2=[4-10] OR [1-5]  
         else if(this.LeftOperand<=F2.LeftOperand || this.RightOperand>=F2.RightOperand)
         {
         //System.out.println("F1 Partail Sharing F2");
         //Q1.intersect(Q2);
         //System.out.println(Q1 + " intersect(" + Q2 + ") = " + Q1.intersect(Q2));
         //overlaplen=(Q1.intrsect_higherBound-Q1.intrsect_lowerBound)+1;
         overlap.intrsect_lowerBound=Q1.intrsect_lowerBound;
         overlap.intrsect_higherBound=Q1.intrsect_higherBound;
         overlap.overlapp_type="P";

         return overlap;
         }
        
         //else if ( not sub set)         
        
         
      // ftech from histogram "select sum(frequencey) from histogram where Table_Name="+this.tablename+
         //"and Column_Name="+this.colomn1+" and Distinct_Value between"+Q1.intrsect_lowerBound+ " and "+Q1.intrsect_higherBound 
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
         //String s="";
        
          int i = sqlparser.parse( );
          String temp="";
           
                temp=sqlparser.sqlstatements.get( 0 ).getWhereClause( ).toString();
                temp=sqlparser.sqlstatements.get( 0 ).getWhereClause( ).toString();
                
                for(int j=0;j<sqlparser.sqlstatements.get( 0 ).tables.size();j++)            
            {
                     this.tablename= sqlparser.sqlstatements.get( 0 ).tables.getTable(j).getName();
                    //tabs.add(table.getName());
                   
                
            }
          //  if (token.equals("operator"))
                temp=sqlparser.sqlstatements.get( 0 ).getWhereClause( ).getCondition().getOperatorToken().toString();
          // if (token.equals("exp")){
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
                  
                 // String LO=String.valueOf(this.LeftOperand);
                  //String RO=String.valueOf(this.RightOperand);             
                  int t=this.LeftOperand;
                  this.LeftOperand=this.RightOperand;
                  this.RightOperand=t;
                  //String s="15, 3";
                 // s.replace("15", "100");
                 // this.query.replace(RO,String.valueOf(t));
                 // System.out.println(s);
                 // System.out.println(this.query);
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
   
