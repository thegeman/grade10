package science.atlarge.grade10.monitor

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import science.atlarge.grade10.model.resources.ResourceModel
import science.atlarge.grade10.model.resources.buildResourceModel
import science.atlarge.grade10.monitor.PerfMonitorResourceModel.CPU_CORE_UTILIZATION_NAME
import science.atlarge.grade10.monitor.PerfMonitorResourceModel.CPU_NAME
import science.atlarge.grade10.monitor.PerfMonitorResourceModel.MACHINE_NAME
import science.atlarge.grade10.monitor.PerfMonitorResourceModel.NETWORK_BYTES_RECEIVED_NAME
import science.atlarge.grade10.monitor.PerfMonitorResourceModel.NETWORK_BYTES_TRANSMITTED_NAME
import science.atlarge.grade10.monitor.PerfMonitorResourceModel.NETWORK_NAME
import science.atlarge.grade10.monitor.PerfMonitorResourceModel.ROOT_NAME
import science.atlarge.grade10.monitor.PerfMonitorResourceModel.TOTAL_CPU_UTILIZATION_NAME
import science.atlarge.grade10.monitor.serialization.KryoConfiguration
import java.io.File
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

class MonitorOutput(machinesData: Iterable<MachineUtilizationData>) {

    val machines = machinesData.associateBy(MachineUtilizationData::hostname)

    fun toResourceModel(
            includePerCoreUtilization: Boolean = true,
            interfaceIdAndHostnameToCapacity: (interfaceId: String, hostname: String) -> Double = { _, _ -> 0.0 }
    ): ResourceModel {
        return buildResourceModel(PerfMonitorResourceModel.MODEL) {
            newSubresource(ROOT_NAME) {
                machines.forEach { hostname, machineData ->
                    newSubresource(MACHINE_NAME, hostname) {
                        // Add CPU data to machine
                        newConsumableMetric(TOTAL_CPU_UTILIZATION_NAME, machineData.cpu.totalCoreUtilization,
                                machineData.cpu.numCpuCores.toDouble())
                        if (includePerCoreUtilization) {
                            machineData.cpu.cores.forEachIndexed { i, coreData ->
                                newSubresource(CPU_NAME, i.toString()) {
                                    newConsumableMetric(CPU_CORE_UTILIZATION_NAME, coreData.utilization, 1.0)
                                }
                            }
                        }

                        // Add network data to machine
                        machineData.network.interfaces.forEach { interfaceId, netData ->
                            newSubresource(NETWORK_NAME, interfaceId) {
                                newConsumableMetric(NETWORK_BYTES_RECEIVED_NAME, netData.bytesReceived,
                                        interfaceIdAndHostnameToCapacity(interfaceId, hostname))
                                newConsumableMetric(NETWORK_BYTES_TRANSMITTED_NAME, netData.bytesSent,
                                        interfaceIdAndHostnameToCapacity(interfaceId, hostname))
                            }
                        }
                    }
                }
            }
        }
    }

    fun writeToFile(file: File, kryo: Kryo = KryoConfiguration.defaultInstance) {
        Output(GZIPOutputStream(file.outputStream())).use {
            it.writeVarInt(SERIALIZATION_VERSION, true)
            kryo.writeObject(it, this)
        }
    }

    companion object {

        const val SERIALIZATION_VERSION = 1

        fun readFromFile(file: File, kryo: Kryo = KryoConfiguration.defaultInstance): MonitorOutput? {
            Input(GZIPInputStream(file.inputStream())).use {
                return if (it.readVarInt(true) == SERIALIZATION_VERSION) {
                    kryo.readObject(it, MonitorOutput::class.java)
                } else {
                    null
                }
            }
        }

    }

}
