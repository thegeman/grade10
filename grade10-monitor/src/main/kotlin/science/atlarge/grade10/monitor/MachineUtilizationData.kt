package science.atlarge.grade10.monitor

import science.atlarge.grade10.monitor.procfs.CpuUtilizationData
import science.atlarge.grade10.monitor.procfs.NetworkUtilizationData

class MachineUtilizationData(
        val hostname: String,
        val cpu: CpuUtilizationData,
        val network: NetworkUtilizationData
)
