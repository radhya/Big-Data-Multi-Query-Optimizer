/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package SharedHive;

import java.util.List;

/**
 *
 * @author hadoop
 */
public  class Parent
      {
        int queryno=0;
        double actualresult=0;
        double sharedresult=0;
        double selectiviy=0;
        double cost=0;
        
        
        
     public  int  Find_Max_ParentList(List <Parent> ParentList)
     {
       double max=0;int index=0;
        Parent  TempParent =new Parent(); 
         for(int i=0;i<ParentList.size();i++)
         {
          TempParent=ParentList.get(i);
          if(TempParent.sharedresult>=max)
          {
           index=i;
           max=TempParent.sharedresult;
          }
         }
          return index;
     }
     
     
     public  int  Find_selectevity_ParentList(List <Parent> ParentList)//, int original_table_size)
     {
       double minsel=0;int index=0;
        Parent  TempParent =new Parent(); 
        
         for(int i=0;i<ParentList.size();i++)
         {
          TempParent=ParentList.get(i);
         // double shared_selectevity=TempParent.sharedresult/
          if(TempParent.sharedresult<=minsel)
          {
           index=i;
           minsel=TempParent.selectiviy;
          }
         }
          return index;
     }
     
      public  int  Find_Min_ParentList(List <Parent> ParentList)
     {
       double min=0;int index=0;
        Parent  TempParent =new Parent(); 
        TempParent=ParentList.get(0);
        min=TempParent.sharedresult;
         for(int i=0;i<ParentList.size();i++)
         {
          TempParent=ParentList.get(i);
          if(TempParent.sharedresult<=min)
          {
              
           index=i;
           min=TempParent.sharedresult;
          }
         }
          return index;
     }
       public  int  Find_Min_Cost_ParentList(List <Parent> ParentList)
     {
       double min=0;int index=0;
        Parent  TempParent =new Parent(); 
        TempParent=ParentList.get(0);
        min=TempParent.cost;
         for(int i=0;i<ParentList.size();i++)
         {
          TempParent=ParentList.get(i);
          if(TempParent.cost<min)
          {
              
           index=i;
           min=TempParent.cost;
            System.out.println("min cost.."+min+" index.."+(index+1));
          }
         }
          return index;
     }
      
       public  void  Print_ParentList(List <Parent> ParentList)
       {
          Parent  TempParent =new Parent(); 
         
          for(int i=0;i<ParentList.size();i++)
          {
            TempParent=ParentList.get(i);
  
          System.out.println("queryno "+(TempParent.queryno+1)+" sharedresult "+TempParent.sharedresult);
          } 
       }
      
      };