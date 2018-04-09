package science.atlarge.grade10.analysis.bottlenecks

import science.atlarge.grade10.analysis.attribution.ResourceAttributionResult
import science.atlarge.grade10.model.execution.ExecutionModel
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.Metric

object BottleneckIdentification {

    fun execute(
            executionModel: ExecutionModel,
            settings: BottleneckIdentificationSettings,
            resourceAttributionResult: ResourceAttributionResult
    ): BottleneckIdentificationResult {
        val metricBottleneckIdentificationResult = MetricBottleneckIdentificationStep.execute(
                executionModel,
                settings,
                resourceAttributionResult
        )

        val metricTypeBottleneckIdentificationResult = MetricTypeBottleneckIdentificationStep.execute(
                executionModel,
                settings,
                resourceAttributionResult,
                metricBottleneckIdentificationResult
        )

        return BottleneckIdentificationResult(
                metricBottleneckIdentificationResult,
                metricTypeBottleneckIdentificationResult
        )
    }

}

data class BottleneckIdentificationSettings(
        val globalBottleneckThresholdFactor: (Metric.Consumable) -> Double = { _ -> 0.95 },
        val localBottleneckThresholdFactor: (Metric.Consumable, Phase) -> Double = { _, _ -> 0.95 },
        val bottleneckPredicate: PerPhaseBottleneckPredicate = AnyBottleneckPredicate
)

class BottleneckIdentificationResult(
        val metricBottlenecks: MetricBottleneckIdentificationStepResult,
        val metricTypeBottlenecks: MetricTypeBottleneckIdentificationStepResult
)
