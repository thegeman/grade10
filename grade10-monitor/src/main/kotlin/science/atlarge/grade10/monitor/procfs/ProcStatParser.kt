package science.atlarge.grade10.monitor.procfs

import science.atlarge.grade10.metrics.RateObservations
import science.atlarge.grade10.monitor.util.*
import java.io.Closeable
import java.io.File

class ProcStatParser {

    private class ParserState(logFile: File) : Closeable {
        private val stream = logFile.inputStream().buffered()

        private val timestamps = LongArrayBuilder()
        private val totalCoreUtilization = DoubleArrayBuilder()
        private lateinit var coreUtilization: Array<DoubleArrayBuilder>

        private var numCpus: Int = 0
        private var currentTimestamp: Long = 0
        private lateinit var currentCpuMetrics: LongArray

        fun parse(): CpuUtilizationData {
            // Read first message to set initial timestamp and determine the number of CPUs
            readFirstMessage()
            timestamps.append(currentTimestamp)
            // Read and process messages until an exception occurs while reading
            try {
                while (true) {
                    readNextMessage()
                    timestamps.append(currentTimestamp)
                    computeUtilization()
                }
            } catch (e: Exception) {
                // Swallow exception and stop parsing more data
            }

            // Convert the result to the right data structures
            val timestampsArray = timestamps.toArray()
            val coreUtilizationData = Array(numCpus) { coreId ->
                val observations = RateObservations.from(timestampsArray, coreUtilization[coreId].toArray())
                CpuCoreUtilizationData(coreId, observations)
            }
            val totalCoreUtilizationData = RateObservations.from(timestampsArray, totalCoreUtilization.toArray())
            return CpuUtilizationData(totalCoreUtilizationData, numCpus, coreUtilizationData)
        }

        private fun readFirstMessage() {
            // Read initial timestamp
            currentTimestamp = stream.readLELong()
            // Read number of CPUs and create data structures to read CPU metrics and store utilization
            numCpus = stream.readLEB128Int()
            currentCpuMetrics = LongArray(numCpus * 10)
            coreUtilization = Array(numCpus) { DoubleArrayBuilder() }
            // Read initial CPU metric values
            for (i in 0..currentCpuMetrics.lastIndex) {
                currentCpuMetrics[i] = stream.readLEB128Long()
            }
        }

        private fun readNextMessage() {
            // Read delta timestamp
            currentTimestamp = stream.readLELong()
            // Check the number of CPUs
            require(stream.readLEB128Int() == numCpus) {
                "ProcStatParse currently does not support changing the number of CPUs"
            }
            // Read the new CPU metric values
            for (i in 0..currentCpuMetrics.lastIndex) {
                currentCpuMetrics[i] = stream.readLEB128Long()
            }
        }

        private fun computeUtilization() {
            var sumUtilization = 0.0
            for (cpuId in 0 until numCpus) {
                val cpuOffset = cpuId * 10
                var totalJiffies = 0L
                for (i in cpuOffset until cpuOffset + 10) {
                    totalJiffies += currentCpuMetrics[i]
                }
                val idleJiffies = currentCpuMetrics[cpuOffset + 3]
                val utilization = (totalJiffies - idleJiffies) / totalJiffies.toDouble()

                coreUtilization[cpuId].append(utilization)

                sumUtilization += utilization
            }
            totalCoreUtilization.append(sumUtilization)
        }

        override fun close() {
            stream.close()
        }
    }

    fun parse(logFile: File): CpuUtilizationData {
        return ParserState(logFile).use {
            it.parse()
        }
    }

}

class CpuUtilizationData(
        val totalCoreUtilization: RateObservations,
        val numCpuCores: Int,
        val cores: Array<CpuCoreUtilizationData>
) {

    init {
        cores.forEachIndexed { i, cpuCoreUtilizationData ->
            require(cpuCoreUtilizationData.coreId == i) { "coreId of the i'th CpuCoreUtilizationData must be i" }
        }
    }

}

class CpuCoreUtilizationData(val coreId: Int, val utilization: RateObservations)
