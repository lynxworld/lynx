import com.github.tototoshi.csv.CSVReader

import java.io.File
//new File(".").getCanonicalPath
val data = CSVReader.open(new File("/Users/huchuan/Documents/lynx/datasets/contains.csv")).iterator
data.take(10).map(_.length).toList
