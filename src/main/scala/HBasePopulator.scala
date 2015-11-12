/**
  * Created by scotts on 11/12/15.
  */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan, Put, Connection}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by ted.malaska on 11/12/15.
  */
object HBasePopulator {
  def main(args: Array[String]): Unit = {
    val connection = ConnectionFactory.createConnection(new Configuration())

    populate(100,
      10000,
      1,
      connection: Connection,
      "EnergyMonitorTable")

    megaScan(connection, "EnergyMonitorTable")
  }

  def createTable(): Unit = {

  }

  def populate(numOfUsers:Int,
               numOfRecords:Int,
               waitTimeEvery1000:Int,
               connection: Connection,
               tableStr:String): Unit = {

    val bufferedMutator = connection.getBufferedMutator(TableName.valueOf(tableStr))
    val generator = new EnergyMonitorDataGen(numOfUsers)

    for (i <- 0 to numOfRecords) {

      val record = generator.next()

      val put = new Put(Bytes.toBytes(record.userId + "_" +
        (Long.MaxValue - record.time)))
      put.addColumn(HBaseContants.columnFamily,
        HBaseContants.column, Bytes.toBytes(record.usedAmount.toString))

      bufferedMutator.mutate(put)

      if (i % 1000 == 0) {
        Thread.sleep(waitTimeEvery1000)
      }
    }
    bufferedMutator.flush()
  }

  def megaScan(connection:Connection, tableStr:String): Unit = {
    val table = connection.getTable(TableName.valueOf(tableStr))
    val scan = new Scan()
    scan.setBatch(1000)
    scan.setCaching(1000)
    scan.setCacheBlocks(false)

    val scanner = table.getScanner(scan)

    val it = scanner.iterator()
    while(it.hasNext) {
      val result = it.next()
      println(" - " + Bytes.toString(result.getRow) + ":" +
        Bytes.toString(result.getValue(HBaseContants.columnFamily,
          HBaseContants.column)))
    }
  }
}