import java.io.{File, PrintWriter}
import java.nio.file.Files
import java.nio.{ByteBuffer, ByteOrder}

val FILE_IN_DIRECTORY: String = "D:\\Users\\###\\Documents\\BEX_exports"
val FILE_OUT_DIRECTORY: String = s"${FILE_IN_DIRECTORY}\\out\\"

val BYTE_LENGTH = 8                               // Each data point is 8 Bytes long
val CHANNEL_LENGTH = 32001                        // Each channel has 32001 data points
val DOWNSAMPLE_SCALE = 2                          // Downsample by 50%
val START_INDEX = 16000                           // Trim the first half of the graph (no readings)

def getListOfFiles(dir: String): List[File] = {
  val d = new File(dir)
  if (d.exists && d.isDirectory) {
    d.listFiles.filter(_.isFile).toList
  } else {
    List[File]()
  }
}

def startAt[A](l:Seq[Array[A]], n:Int): Seq[Array[A]] =
  l.drop(n)

def convertByteArraysToDouble(graph: Seq[Array[Byte]]): Seq[Double] =
  for (point <- graph) yield
    ByteBuffer.wrap(point).order(ByteOrder.LITTLE_ENDIAN).getDouble

def downsample[A](byteArrayList:Seq[A], n:Int): Seq[A] =
  byteArrayList
    .zipWithIndex
    .collect {
      case (bytes,i)
        if ((i+1) % n) == 0 => bytes
    }

def buildFileData(file: File): (String, String) =
  (file.getName,
    Files.readAllBytes(file.toPath)
      .grouped(BYTE_LENGTH)                         // Group raw bytes
      .grouped(CHANNEL_LENGTH)                      // Group bytes by channel
      .map(startAt(_, START_INDEX))                 // Trim the start of the data
      .map(downsample(_, DOWNSAMPLE_SCALE))         // Downsample by scale (2 = 50%)
      .map(convertByteArraysToDouble(_))            // Convert byte array to Double (64 bit float)
      .map(_.mkString(","))                         // Create a CSV string
      .mkString("\n"))                              // Put each channel on a new line

def printToFile(f: java.io.File)(op: PrintWriter => Unit): Unit = {
  val p = new java.io.PrintWriter(f)
  try { op(p) } finally { p.close() }
}

getListOfFiles(FILE_IN_DIRECTORY)
  .map(buildFileData(_))
  .foreach(file => {
    printToFile(new File(s"${FILE_OUT_DIRECTORY}${file._1}.csv")) { printWriter =>
      printWriter.print(file._2)
    }
  })