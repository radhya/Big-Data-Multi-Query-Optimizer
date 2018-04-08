package SharedHive;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author hadoop
 */



   public  class Histogram
   {
   
   public  String tablename ="";
   public  String columnname ="";
   public  int Distinct_Value =0;
   public  int frequency =0;
    
  
   
   public  void Histogram()
   {
    
    this.tablename ="";
    this.columnname ="";
    this.Distinct_Value =0;
    this.frequency=0;
    
   }
   public  void print_Histogram()
   {
     String space="   ";
     //System.out.println("tablename"+ space+"columnname"+space+"Distinct_Value"+space+"frequency"); 
     System.out.println(this.tablename+ space+this.columnname+space+this.Distinct_Value+space+this.frequency); 
   }
   
   }
         
  
