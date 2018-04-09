package science.atlarge.grade10.monitor.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import science.atlarge.grade10.metrics.RateObservations
import science.atlarge.grade10.monitor.procfs.NetworkInterfaceUtilizationData

class NetworkInterfaceUtilizationDataSerializer : Serializer<NetworkInterfaceUtilizationData>(true, true) {

    override fun write(kryo: Kryo, output: Output, networkInterfaceUtilizationData: NetworkInterfaceUtilizationData) {
        networkInterfaceUtilizationData.apply {
            output.writeString(interfaceId)
            kryo.writeObject(output, networkInterfaceUtilizationData.bytesReceived)
            kryo.writeObject(output, networkInterfaceUtilizationData.bytesSent)
        }
    }

    override fun read(kryo: Kryo, input: Input, type: Class<NetworkInterfaceUtilizationData>): NetworkInterfaceUtilizationData {
        val interfaceId = input.readString()
        val bytesReceived = kryo.readObject(input, RateObservations::class.java)
        val bytesSent = kryo.readObject(input, RateObservations::class.java)
        return NetworkInterfaceUtilizationData(interfaceId, bytesReceived, bytesSent)
    }

}
