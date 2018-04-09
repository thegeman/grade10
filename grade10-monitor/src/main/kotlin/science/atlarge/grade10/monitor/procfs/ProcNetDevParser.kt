package science.atlarge.grade10.monitor.procfs

import science.atlarge.grade10.metrics.RateObservations
import science.atlarge.grade10.monitor.util.*
import java.io.File

class ProcNetDevParser {

    fun parse(logFile: File): NetworkUtilizationData {
        return logFile.inputStream().buffered().use { inStream ->
            // Read first message to determine number and names of interfaces
            val initialTimestamp = inStream.readLELong()
            require(inStream.read() == 0) { "Expecting monitoring info to start with an IFACE_LIST message" }
            val numInterfaces = inStream.readLEB128Int()
            val interfaceNames = Array(numInterfaces) { inStream.readString() }

            val timestamps = LongArrayBuilder()
            timestamps.append(initialTimestamp)
            val receivedUtilization = Array(numInterfaces) { DoubleArrayBuilder() }
            val sentUtilization = Array(numInterfaces) { DoubleArrayBuilder() }
            var lastTimestamp = 0L
            while (true) {
                val timestamp = inStream.tryReadLELong() ?: break
                timestamps.append(timestamp)
                try {
                    require(inStream.read() == 1) { "Repeated IFACE_LIST messages are currently not supported" }
                    inStream.readLEB128Int() // Skip number of interfaces

                    for (i in 0 until numInterfaces) {
                        val bytesReceived = inStream.readLEB128Long()
                        /*val packetsReceived =*/ inStream.readLEB128Long()
                        val bytesSent = inStream.readLEB128Long()
                        /*val packetsSent =*/ inStream.readLEB128Long()
                        receivedUtilization[i].append(bytesReceived.toDouble() * 1_000_000_000L / (timestamp - lastTimestamp))
                        sentUtilization[i].append(bytesSent.toDouble() * 1_000_000_000L / (timestamp - lastTimestamp))
                    }

                    lastTimestamp = timestamp
                } catch (e: Exception) {
                    timestamps.dropLast()
                    // Metric data should have one less element than the number of timestamps
                    receivedUtilization
                            .filter { it.size >= timestamps.size }
                            .forEach { it.dropLast() }
                    sentUtilization
                            .filter { it.size >= timestamps.size }
                            .forEach { it.dropLast() }
                    break
                }
            }

            val timestampArray = timestamps.toArray()
            val interfaces = interfaceNames.mapIndexed { i, interfaceId ->
                val received = RateObservations.from(timestampArray, receivedUtilization[i].toArray())
                val sent = RateObservations.from(timestampArray, sentUtilization[i].toArray())
                NetworkInterfaceUtilizationData(interfaceId, received, sent)
            }
            NetworkUtilizationData(interfaces)
        }
    }

}

class NetworkUtilizationData(interfaceData: Iterable<NetworkInterfaceUtilizationData>) {

    val interfaces = interfaceData.associateBy(NetworkInterfaceUtilizationData::interfaceId)

}

class NetworkInterfaceUtilizationData(
        val interfaceId: String,
        val bytesReceived: RateObservations,
        val bytesSent: RateObservations
)
