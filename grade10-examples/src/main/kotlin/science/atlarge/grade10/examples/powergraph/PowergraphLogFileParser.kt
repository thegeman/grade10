package science.atlarge.grade10.examples.powergraph

import science.atlarge.grade10.records.EventRecord
import science.atlarge.grade10.records.EventRecordType
import science.atlarge.grade10.records.Record
import science.atlarge.grade10.records.extraction.LogFileParser

class PowergraphLogFileParser : LogFileParser {

    override fun parseLine(line: String): Record? {
        return operationLogRegex.find(line)?.let { match ->
            val keyValuePairs = parseKeyValuePairs(match.groupValues[1])

            val timestampNanoseconds = keyValuePairs["time"]?.toLong()
                    ?: throw IllegalArgumentException("Line from Powergraph log is missing the 'time' property: $line")
            val event = keyValuePairs["event"]
                    ?: throw IllegalArgumentException("Line from Powergraph log is missing the 'event' property: $line")

            val eventType = when (event) {
                "start" -> EventRecordType.START
                "end" -> EventRecordType.END
                "single" -> EventRecordType.SINGLE
                else -> throw IllegalArgumentException("Line from Powergraph log contains invalid 'event' property: $line")
            }

            val meta = keyValuePairs.filterKeys { it != "time" && it != "event" }

            EventRecord(timestampNanoseconds, eventType, meta)
        } ?: hostnameRankRegex.find(line)?.let { match ->
            val hostname = match.groupValues[1]
            val worker = match.groupValues[2]

            EventRecord(0, EventRecordType.SINGLE, mapOf(
                    "type" to "map-hostname-to-worker",
                    "hostname" to hostname,
                    "worker" to worker))
        }
    }

    companion object {

        private val operationLogRegex = Regex("""OperationLog: (.*)""")
        private val hostnameRankRegex = Regex("""\[(.*):\d*\] MCW rank (\d+)""")

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
