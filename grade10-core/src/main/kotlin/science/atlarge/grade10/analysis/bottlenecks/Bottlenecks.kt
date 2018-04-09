package science.atlarge.grade10.analysis.bottlenecks

import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.execution.PhaseType
import science.atlarge.grade10.model.resources.Metric
import science.atlarge.grade10.model.resources.MetricType

typealias BottleneckStatus = Byte
typealias BottleneckStatusArray = ByteArray

object BottleneckStatusConstants {

    const val NONE: BottleneckStatus = 0
    const val LOCAL: BottleneckStatus = 1
    const val GLOBAL: BottleneckStatus = 2

}

interface BottleneckStatusIterator : Iterator<BottleneckStatus> {

    override fun next(): BottleneckStatus = nextBottleneckStatus()

    fun nextBottleneckStatus(): BottleneckStatus

}

sealed class BottleneckSource {

    data class MetricBottleneck(val metric: Metric) : BottleneckSource() {

        override fun toString(): String {
            return "Metric(\"${metric.path}\")"
        }

    }

    data class MetricTypeBottleneck(val metricType: MetricType) : BottleneckSource() {

        override fun toString(): String {
            return "MetricType(\"${metricType.path}\")"
        }

    }

    object NoBottleneck : BottleneckSource() {

        override fun toString(): String {
            return "None"
        }

    }

}

class PhaseAndBottleneckStatus {

    lateinit var phase: Phase

    var bottleneckStatus: BottleneckStatus = BottleneckStatusConstants.NONE

}

class PhaseTypeAndBottleneckStatus {

    lateinit var phaseType: PhaseType

    var bottleneckStatus: BottleneckStatus = BottleneckStatusConstants.NONE

}
