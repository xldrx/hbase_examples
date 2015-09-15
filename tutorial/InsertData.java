import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertData{

   public static void main(String[] args) throws IOException {

      // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();
      try (Connection conn = ConnectionFactory.createConnection(config);
           Table hTable = conn.getTable(TableName.valueOf("emp"))) {
          //
          // Instantiating Put class
          // accepts a row name.
          Put p = new Put(Bytes.toBytes("row1"));

          // adding values using add() method
          // accepts column family name, qualifier/row name ,value
          p.addColumn(Bytes.toBytes("personal"),
          Bytes.toBytes("name"),Bytes.toBytes("raju"));

          p.addColumn(Bytes.toBytes("personal"),
          Bytes.toBytes("city"),Bytes.toBytes("hyderabad"));

          p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("designation"),
          Bytes.toBytes("manager"));

          p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("salary"),
          Bytes.toBytes("50000"));

          // Saving the put Instance to the HTable.
          hTable.put(p);
          System.out.println("data inserted");
      }
   }
}
