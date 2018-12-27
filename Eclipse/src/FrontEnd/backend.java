
/*

Good Afternoon John, I hope you are enjoying your break from classes!

I updated the backend code to be more in line with what we need for the final build of the project. 
I changed the row key to be: userID+messageNumber. I chose this convention because it would be 
unique and easily searchable. I wrote the code to display all the messages a particular user 
received. It will be simple enough to attach the code to an actionListener to update the message 
list on the fly. For the initial list population we can just create a basic log in component and 
extract the username from that component to populate the list in the beginning. In order to send a 
new message to a new user we just need to count how many messages the receiving user already has. 

I already have a counter implemented and it prints out what the next message number should be. 
If you have any questions, if I missed anything, or would like my help with the front end, just 
let me know :)

*/
package FrontEnd;

import java.io.IOException; 
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.client.*; 
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class backend
{
  public void databaseStuff() throws IOException 
  {
    System.out.println("HBase Start");
    
    // connect 
    Configuration config = HBaseConfiguration.create();
    
    // Get recordset
    HTable table = new HTable(config, "messages");
    
    // Add message to blog
    String index;
    String messageTracker;
    int messageNumber=0;
    int userTracker=0;
    for (int i=0; i<5001; i++)
    {   
      DateFormat sdf= new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      Calendar cal=Calendar.getInstance();
      String date=sdf.format(cal.getTime());
      messageTracker="0000"+messageNumber;
      messageTracker=messageTracker.substring(messageTracker.length()-4, messageTracker.length());
      
      index = "0000"+i;
      index = index.substring(index.length()-4, index.length());
      if(userTracker==10)
      {
        userTracker=0;
        messageNumber++;
      }
      
      Put p = new Put(Bytes.toBytes("userID"+messageTracker+" Message Number: "+userTracker));
      
	    p.add(Bytes.toBytes("message"), Bytes.toBytes("user:To"), Bytes.toBytes("userID"+messageTracker+"@hbase.com"));
        p.add(Bytes.toBytes("message"), Bytes.toBytes("user:date"), Bytes.toBytes(date));
        p.add(Bytes.toBytes("message"), Bytes.toBytes("message:Title"), Bytes.toBytes("Message Title:"+ index));
        p.add(Bytes.toBytes("message"), Bytes.toBytes("message:Body"), Bytes.toBytes("This is message:"+ index));
      
	  userTracker++;
        
      table.put(p);
             
    }  
    
	// GET MESSAGE
	/*
	  Assuming we use a log-in every time we run the program we can extract the user name
      to substitute for the place holder user 'userID0499' in order to populate that user's
      messages
    */
	        
    byte[] prefix=Bytes.toBytes("userID0499");
    Scan scan = new Scan(prefix);
    PrefixFilter prefixFilter = new PrefixFilter(prefix);
    scan.setFilter(prefixFilter);
    ResultScanner resultScanner = table.getScanner(scan);
       
    String valueStr1, valueStr2;
    try 
     {
      int nextMessage=0;
      for (Result rr : resultScanner)
      {
        
        byte [] value1 = rr.getValue(Bytes.toBytes("message"), Bytes.toBytes("messageTitle"));
        byte [] value2 = rr.getValue(Bytes.toBytes("message"), Bytes.toBytes("messageBody"));
        valueStr1 = Bytes.toString(value1);
        valueStr2 = Bytes.toString(value2);     
        System.out.println("GET: " + valueStr1 + " " + valueStr2);
        nextMessage++;
      }
         
      System.out.println("The next message should be: "+nextMessage);
    } 
    finally 
    {
      resultScanner.close();
    }
        
    table.close();
    System.out.println("HBase End");
  }
}