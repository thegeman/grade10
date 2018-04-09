package science.atlarge.grade10.monitor.serialization

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import science.atlarge.grade10.monitor.MachineUtilizationData
import science.atlarge.grade10.monitor.MonitorOutput

class MonitorOutputSerializer : Serializer<MonitorOutput>(true, true) {

    override fun write(kryo: Kryo, output: Output, monitorOutput: MonitorOutput) {

        monitorOutput.apply {
            output.writeVarInt(machines.size, true)
            machines.values.forEach {
                kryo.writeObject(output, it)
            }
        }
    }

    override fun read(kryo: Kryo, input: Input, type: Class<MonitorOutput>): MonitorOutput {

        val numMachines = input.readVarInt(true)
        val machines = ArrayList<MachineUtilizationData>(numMachines)
        repeat(numMachines) {
            machines.add(kryo.readObject(input, MachineUtilizationData::class.java))
        }
        return MonitorOutput(machines)
    }

}
