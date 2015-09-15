import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class ScanTable{

   public static void main(String args[]) throws IOException{

      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();
      try (Connection conn = ConnectionFactory.createConnection(config);
           Table table = conn.getTable(TableName.valueOf("emp"))) {

          // Instantiating the Scan class
          Scan scan = new Scan();

          // Scanning the required columns
          scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
          scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"));

          // Getting the scan result
          try (ResultScanner scanner = table.getScanner(scan)) {

              // Reading values from scan result
              for (Result result = scanner.next(); result != null; result = scanner.next())

              System.out.println("Found row : " + result);
          }
      }
   }
}
