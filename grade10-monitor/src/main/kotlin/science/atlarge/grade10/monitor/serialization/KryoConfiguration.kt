package science.atlarge.grade10.monitor.serialization

import com.esotericsoftware.kryo.Kryo
import science.atlarge.grade10.metrics.RateObservations
import science.atlarge.grade10.monitor.MachineUtilizationData
import science.atlarge.grade10.monitor.MonitorOutput
import science.atlarge.grade10.monitor.procfs.CpuCoreUtilizationData
import science.atlarge.grade10.monitor.procfs.CpuUtilizationData
import science.atlarge.grade10.monitor.procfs.NetworkInterfaceUtilizationData
import science.atlarge.grade10.monitor.procfs.NetworkUtilizationData

object KryoConfiguration {

    val defaultInstance: Kryo by lazy {
        configureKryoInstance(Kryo())
    }

    private fun configureKryoInstance(kryo: Kryo): Kryo {
        return kryo.apply {
            register(MonitorOutput::class.java, MonitorOutputSerializer())
            register(MachineUtilizationData::class.java, MachineUtilizationDataSerializer())
            register(CpuUtilizationData::class.java, CpuUtilizationDataSerializer())
            register(CpuCoreUtilizationData::class.java, CpuCoreUtilizationDataSerializer())
            register(NetworkUtilizationData::class.java, NetworkUtilizationDataSerializer())
            register(NetworkInterfaceUtilizationData::class.java, NetworkInterfaceUtilizationDataSerializer())

            addDefaultSerializer(RateObservations::class.java, DoubleRateMetricSerializer())
        }
    }

}
