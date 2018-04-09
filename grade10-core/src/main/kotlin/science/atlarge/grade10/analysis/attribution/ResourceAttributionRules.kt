package science.atlarge.grade10.analysis.attribution

import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.execution.PhaseType
import science.atlarge.grade10.model.resources.Metric
import science.atlarge.grade10.model.resources.MetricClass
import science.atlarge.grade10.model.resources.MetricType
import science.atlarge.grade10.serialization.Grade10Deserializer
import science.atlarge.grade10.serialization.Grade10Serializer

abstract class ResourceAttributionRuleProvider {

    operator fun get(phaseType: PhaseType, metricType: MetricType): ResourceAttributionRule {
        return when (metricType.metricClass) {
            MetricClass.CONSUMABLE -> getRuleForConsumableResource(phaseType, metricType)
            MetricClass.BLOCKING -> getRuleForBlockingResource(phaseType, metricType)
        }
    }

    operator fun get(phase: Phase, blockingMetric: Metric.Blocking): BlockingResourceAttributionRule {
        return getRuleForBlockingResource(phase.type, blockingMetric.type)
    }

    operator fun get(phase: Phase, consumableMetric: Metric.Consumable): ConsumableResourceAttributionRule {
        return getRuleForConsumableResource(phase.type, consumableMetric.type)
    }

    protected abstract fun getRuleForBlockingResource(phaseType: PhaseType, metricType: MetricType):
            BlockingResourceAttributionRule

    protected abstract fun getRuleForConsumableResource(phaseType: PhaseType, metricType: MetricType):
            ConsumableResourceAttributionRule

}

sealed class ResourceAttributionRule

sealed class ConsumableResourceAttributionRule : ResourceAttributionRule() {

    data class Greedy(val maxRate: Double) : ConsumableResourceAttributionRule() {

        init {
            require(maxRate > 0.0)
        }

    }

    object Sink : ConsumableResourceAttributionRule()
    object None : ConsumableResourceAttributionRule()

    fun serialize(output: Grade10Serializer) {
        when (this) {
            None -> output.writeByte(0)
            Sink -> output.writeByte(1)
            is Greedy -> {
                output.writeByte(2)
                output.writeDouble(maxRate)
            }
        }
    }

    companion object {

        fun deserialize(input: Grade10Deserializer): ConsumableResourceAttributionRule {
            val type = input.readByte()
            return when (type) {
                0.toByte() -> None
                1.toByte() -> Sink
                2.toByte() -> Greedy(input.readDouble())
                else -> throw IllegalArgumentException("Unknown ID of ConsumableAttributionRule: $type")
            }
        }

    }

}

sealed class BlockingResourceAttributionRule : ResourceAttributionRule() {

    object Full : BlockingResourceAttributionRule()
    object None : BlockingResourceAttributionRule()

}
