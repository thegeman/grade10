package science.atlarge.grade10.examples.giraph

import science.atlarge.grade10.records.EventRecord
import science.atlarge.grade10.records.EventRecordType
import science.atlarge.grade10.records.Record
import science.atlarge.grade10.records.extraction.LogFileParser
import science.atlarge.grade10.util.TimestampNs

class GiraphLogFileParser : LogFileParser {

    override fun parseLine(line: String): Record? {
        return operationLogRegex.find(line)?.let { match ->
            val keyValuePairs = parseKeyValuePairs(match.groupValues[1])

            val timestampMilliseconds = keyValuePairs["time"]?.toLong()
                    ?: throw IllegalArgumentException("Line from Giraph log is missing the 'time' property: $line")
            val timestampNs: TimestampNs = timestampMilliseconds * 1_000_000
            val event = keyValuePairs["event"]
                    ?: throw IllegalArgumentException("Line from Giraph log is missing the 'event' property: $line")

            val eventType = when (event) {
                "start" -> EventRecordType.START
                "end" -> EventRecordType.END
                "single" -> EventRecordType.SINGLE
                else -> throw IllegalArgumentException("Line from Giraph log contains invalid 'event' property: $line")
            }

            val meta = keyValuePairs.filterKeys { it != "time" && it != "event" }

            EventRecord(timestampNs, eventType, meta)
        } ?: taskAttemptRegex.find(line)?.let { match ->
            val taskAttempt = match.groupValues[1]
            val hostname = match.groupValues[2]
            EventRecord(0, EventRecordType.SINGLE, mapOf(
                    "type" to "map-taskAttempt-to-hostname",
                    "taskAttempt" to taskAttempt,
                    "hostname" to hostname
            ))
        }
    }

    companion object {

        private val operationLogRegex = Regex("""org.apache.giraph.metrics.OperationLog\s*[:-]\s*(.*)""")
        private val taskAttemptRegex = Regex("""TaskAttemptImpl: TaskAttempt: \[([^]]*)].*on NM: \[(\w*)\..*]""")

        private fun parseKeyValuePairs(line: String): Map<String, String> =
                line.split(",")
                        .map { parseKeyValuePair(it) }
                        .toMap()

        private fun parseKeyValuePair(str: String): Pair<String, String> {
            val (k, v) = str.split("=", limit = 2).map(String::trim)
            return k to v
        }

    }

}
