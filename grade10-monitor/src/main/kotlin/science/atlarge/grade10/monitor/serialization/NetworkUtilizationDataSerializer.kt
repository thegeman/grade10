package science.atlarge.grade10.monitor.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import science.atlarge.grade10.monitor.procfs.NetworkInterfaceUtilizationData
import science.atlarge.grade10.monitor.procfs.NetworkUtilizationData

class NetworkUtilizationDataSerializer : Serializer<NetworkUtilizationData>(true, true) {

    override fun write(kryo: Kryo, output: Output, networkUtilizationData: NetworkUtilizationData) {
        networkUtilizationData.apply {
            output.writeVarInt(interfaces.size, true)
            interfaces.values.forEach {
                kryo.writeObject(output, it)
            }
        }
    }

    override fun read(kryo: Kryo, input: Input, type: Class<NetworkUtilizationData>): NetworkUtilizationData {
        val numInterfaces = input.readVarInt(true)
        val interfaces = ArrayList<NetworkInterfaceUtilizationData>(numInterfaces)
        repeat(numInterfaces) {
            interfaces.add(kryo.readObject(input, NetworkInterfaceUtilizationData::class.java))
        }
        return NetworkUtilizationData(interfaces)
    }

}
