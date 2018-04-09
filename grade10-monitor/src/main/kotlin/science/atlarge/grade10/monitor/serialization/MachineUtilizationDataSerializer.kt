package science.atlarge.grade10.monitor.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import science.atlarge.grade10.monitor.MachineUtilizationData
import science.atlarge.grade10.monitor.procfs.CpuUtilizationData
import science.atlarge.grade10.monitor.procfs.NetworkUtilizationData

class MachineUtilizationDataSerializer : Serializer<MachineUtilizationData>(true, true) {

    override fun write(kryo: Kryo, output: Output, machineUtilizationData: MachineUtilizationData) {
        machineUtilizationData.apply {
            output.writeString(hostname)
            kryo.writeObject(output, cpu)
            kryo.writeObject(output, network)
        }
    }

    override fun read(kryo: Kryo, input: Input, type: Class<MachineUtilizationData>): MachineUtilizationData {
        val hostname = input.readString()
        val cpu = kryo.readObject(input, CpuUtilizationData::class.java)
        val network = kryo.readObject(input, NetworkUtilizationData::class.java)
        return MachineUtilizationData(hostname, cpu, network)
    }

}
