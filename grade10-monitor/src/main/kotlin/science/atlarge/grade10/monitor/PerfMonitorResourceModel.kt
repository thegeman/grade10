package science.atlarge.grade10.monitor

import science.atlarge.grade10.model.resources.MetricClass
import science.atlarge.grade10.model.resources.ResourceTypeRepeatability
import science.atlarge.grade10.model.resources.buildResourceModelSpecification

object PerfMonitorResourceModel {

    val MODEL = buildResourceModelSpecification {
        newSubresourceType(ROOT_NAME) {
            description = "Root of the perf-monitor resource model representing a set of monitored machines"

            newSubresourceType(MACHINE_NAME, ResourceTypeRepeatability.Many("hostname")) {
                newMetricType(TOTAL_CPU_UTILIZATION_NAME, MetricClass.CONSUMABLE)

                newSubresourceType(CPU_NAME, ResourceTypeRepeatability.Many("core")) {
                    newMetricType(CPU_CORE_UTILIZATION_NAME, MetricClass.CONSUMABLE)
                }

                newSubresourceType(NETWORK_NAME, ResourceTypeRepeatability.Many("interface")) {
                    newMetricType(NETWORK_BYTES_RECEIVED_NAME, MetricClass.CONSUMABLE)
                    newMetricType(NETWORK_BYTES_TRANSMITTED_NAME, MetricClass.CONSUMABLE)
                }
            }
        }
    }

    const val ROOT_NAME = "perf-monitor"
    const val MACHINE_NAME = "machine"
    const val CPU_NAME = "cpu"
    const val NETWORK_NAME = "network"

    const val TOTAL_CPU_UTILIZATION_NAME = "total-cpu-utilization"
    const val CPU_CORE_UTILIZATION_NAME = "utilization"
    const val NETWORK_BYTES_RECEIVED_NAME = "bytes-received"
    const val NETWORK_BYTES_TRANSMITTED_NAME = "bytes-transmitted"

}
