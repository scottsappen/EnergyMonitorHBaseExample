import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.{TableName, HBaseTestingUtility}
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{FunSuite, BeforeAndAfterEach, BeforeAndAfterAll}

/**
  * Created by scotts on 11/12/15.
  */
class DataLoadSuite extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  var htu: HBaseTestingUtility = null

  override def beforeAll() {

    htu = HBaseTestingUtility.createLocalHTU()

    htu.cleanupTestDir()
    println("starting minicluster")
    htu.startMiniZKCluster()
    htu.startMiniHBaseCluster(1, 1)
    println(" - minicluster started")
    try {
      htu.deleteTable(Bytes.toBytes(HBaseContants.tableName))
    } catch {
      case e: Exception => {
        println(" - no table " + HBaseContants.tableName + " found")
      }
    }
    println(" - creating table " + HBaseContants.tableName)
    htu.createTable(Bytes.toBytes(HBaseContants.tableName), HBaseContants.columnFamily)
    println(" - created table")

  }

  override def afterAll() {
    htu.deleteTable(Bytes.toBytes(HBaseContants.tableName))
    println("shuting down minicluster")
    htu.shutdownMiniHBaseCluster()
    htu.shutdownMiniZKCluster()
    println(" - minicluster shut down")
    htu.cleanupTestDir()
  }

  test("test the load") {
    HBasePopulator.populate(100, 5000, 1, htu.getConnection, HBaseContants.tableName)
    HBasePopulator.megaScan(htu.getConnection, HBaseContants.tableName)

    val table = htu.getConnection.getTable(TableName.valueOf(HBaseContants.tableName))

    println("Single Record Test")

    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes("10_"))
    scan.setStopRow(Bytes.toBytes("10__"))
    scan.setCaching(1)
    val scanner = table.getScanner(scan)
    val it = scanner.iterator()
    val result = it.next()
    println(" - " + Bytes.toString(result.getRow) + ":" +
      Bytes.toString(result.getValue(HBaseContants.columnFamily,
        HBaseContants.column)))
  }
}