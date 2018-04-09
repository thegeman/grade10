package science.atlarge.grade10.records.extraction

import science.atlarge.grade10.records.Record
import java.io.InputStream

interface LogFileParser : RecordExtractionRule<InputStream> {

    fun parseLine(line: String): Record?

    override fun extractAll(input: InputStream, recordCreationCallback: (Record) -> Unit) {
        input.bufferedReader().useLines { lines ->
            lines.forEach { line ->
                parseLine(line)?.run(recordCreationCallback)
            }
        }
    }

}
