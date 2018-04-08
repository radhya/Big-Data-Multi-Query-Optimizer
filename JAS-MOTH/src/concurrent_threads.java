/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package SharedHive;

/**
 *
 * @author hadoop
 */
import java.util.concurrent.ConcurrentLinkedQueue;

    
 
class MyThread extends Thread {
   private ConcurrentLinkedQueue<MyThread> activeThreads;
    
   private final Object lock = new Object();
    
   private static int threadCount = 0;
   private final int threadId;
 
   public MyThread(ConcurrentLinkedQueue<MyThread> activeThreads) {
      this.activeThreads = activeThreads;
       
      synchronized(lock) { //make sure to protect shared data
         threadId = threadCount++;
      }
   }
 
   public void start() {
      System.out.println("["+threadId + "] Adding Thread to Active List");
      activeThreads.add(this);
      super.start();
   }
 
   public void run() {
      try {
         System.out.println("["+threadId + "] Thread sleeping for awhile.");
         sleep((int)Math.round(Math.random() * 5000) + 1000);  //sleep for a random amount
         System.out.println("["+threadId + "] Thread woke up.");
      }
      catch(Exception e) { e.printStackTrace(); }
 
      System.out.println("["+threadId + "] Removing Thread from Active List");
      activeThreads.remove(this);
   }
}
 
public class concurrent_threads {
   public static void main(String args[]) {
      ConcurrentLinkedQueue<MyThread> activeThreads = new ConcurrentLinkedQueue<MyThread>();
 
      while(true) {
         if(activeThreads.size() < 5) {
            new MyThread(activeThreads).start();
         }
         try { Thread.sleep(25); } catch(Exception e) {} //briefly pause as to not use all the CPU
      }
   }
}
