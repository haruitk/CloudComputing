
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class MP4HbaseClient {

    /** Drop tables if this value is set true. */
    static boolean INITIALIZE_AT_FIRST = false;

    private final byte[] table_MetaHumans = Bytes.toBytes("MetaHumans"), //
            columnfamily_background = Bytes.toBytes("background"), //
            columnfamily_powers = Bytes.toBytes("powers"), //
			qualifier_id = Bytes.toBytes("id"), //
			qualifier_gender = Bytes.toBytes("genger"), //
			qualifier_race = Bytes.toBytes("race"), //
			qualifier_side = Bytes.toBytes("side"), //
			qualifier_superspeed = Bytes.toBytes("superspeed"), //
			qualifier_superhearing = Bytes.toBytes("superhearing"), //
			qualifier_supervision = Bytes.toBytes("supervision"), //
			qualifier_flying = Bytes.toBytes("flying"), //
			qualifier_invisibility = Bytes.toBytes("invisibility"), //
			qualifier_icecontrol = Bytes.toBytes("icecontrol"), //
			qualifier_superhealing= Bytes.toBytes("superhealing"), //
			qualifier_magicpowers = Bytes.toBytes("magicpowers"), //
			qualifier_spidersense = Bytes.toBytes("spidersense"), //
            qualifier_superbrain = Bytes.toBytes("superbrain");

    private void createTable(HBaseAdmin admin) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(table_MetaHumans);
        desc.addFamily(new HColumnDescriptor(columnfamily_background));
        desc.addFamily(new HColumnDescriptor(columnfamily_powers));
        admin.createTable(desc);
    }

        private void deleteTable(HBaseAdmin admin) throws IOException {
        if (admin.tableExists(table_MetaHumans)) {
            admin.disableTable(table_MetaHumans);
            try {
                admin.deleteTable(table_MetaHumans);
            } finally {
            }
        }
    }


    public void run(Configuration config, String file) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(config);
        HTableFactory factory = new HTableFactory();

        if (INITIALIZE_AT_FIRST) {
            deleteTable(admin);
        }

        if (!admin.tableExists(table_MetaHumans)) {
            createTable(admin);
        }

          HTableInterface table = factory.createHTableInterface(config, table_MetaHumans);
          updateTable(file, admin, table );
          factory.releaseHTableInterface(table); // Disconnect
    }
    
    private void updateTable(String fileName, HBaseAdmin admin, HTableInterface table) throws IOException {
    	try{
    		  // Open the file that is the first 
    		  // command line parameter
    		  FileInputStream fstream = new FileInputStream(fileName);
    		  // Get the object of DataInputStream
    		  DataInputStream in = new DataInputStream(fstream);
    		  BufferedReader br = new BufferedReader(new InputStreamReader(in));
    		  String strLine;
    		  int rowNumber=1;
    		  
    		 //Read File Line By Line
    		  while ((strLine = br.readLine()) != null)   {
    			  String rowName = "row"+rowNumber;
    			  
    			  String[] line = strLine.split("\\s");
    			  if("id".equalsIgnoreCase(line[0]))
    			  {
    				  //Ignore first line in file showing header
    				  System.out.println("Row name: Header row");
        			  System.out.println("Row data: "+strLine);
    			  }
    			  else
    			  {
    				  System.out.println("Row name: "+rowName);
        			  System.out.println("Row data: "+strLine);
    			   // Add row data to table for each line in file
    		        Put p = new Put(Bytes.toBytes(rowName));
    		        p.add(columnfamily_background, qualifier_id, Bytes.toBytes(line[0]));
    		        p.add(columnfamily_background, qualifier_gender, Bytes.toBytes(line[1]));
    		        p.add(columnfamily_background, qualifier_race , Bytes.toBytes(line[2]));
    		        p.add(columnfamily_background, qualifier_side, Bytes.toBytes(line[3]));
    		        p.add(columnfamily_powers, qualifier_superspeed, Bytes.toBytes(line[4]));
    		        p.add(columnfamily_powers, qualifier_superhearing, Bytes.toBytes(line[5]));
    		        p.add(columnfamily_powers, qualifier_supervision, Bytes.toBytes(line[6]));
    		        p.add(columnfamily_powers, qualifier_flying, Bytes.toBytes(line[7]));
    		        p.add(columnfamily_powers, qualifier_invisibility, Bytes.toBytes(line[8]));
    		        p.add(columnfamily_powers, qualifier_icecontrol, Bytes.toBytes(line[9]));
    		        p.add(columnfamily_powers, qualifier_superhealing, Bytes.toBytes(line[10]));
    		        p.add(columnfamily_powers, qualifier_magicpowers, Bytes.toBytes(line[11]));
    		        p.add(columnfamily_powers, qualifier_spidersense, Bytes.toBytes(line[12]));
    		        p.add(columnfamily_powers, qualifier_superbrain, Bytes.toBytes(line[1]));
    		        table.put(p);
    		      
    		      //Increment row number
    			  rowNumber++;
    			  }
    		  }
    		  //Close the input stream
    		  in.close();
    		}
    	catch (Exception e)
    	{
    		System.err.println("Error: " + e.getMessage());
    	}
     }
    
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        MP4HbaseClient client = new MP4HbaseClient();
        String file = args[0];
        if(args.length>1)
        {
        	INITIALIZE_AT_FIRST = args[1]!=null?Boolean.parseBoolean(args[1]):false;
        	System.out.print("INITIALIZE_AT_FIRST: "+INITIALIZE_AT_FIRST);
        }

        try {
            HBaseAdmin.checkHBaseAvailable(config);
        } catch (MasterNotRunningException e) {
        	System.out.println("HBase is not running.");
            System.exit(1);
        }
        catch (Exception e) {
        	System.out.println("Exception in initializing");
            System.exit(1);
        }
        System.out.println("HBase is running!");

        client.run(config, file);
    }

}