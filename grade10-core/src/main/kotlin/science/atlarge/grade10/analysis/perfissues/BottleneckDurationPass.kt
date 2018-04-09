package science.atlarge.grade10.analysis.perfissues

import science.atlarge.grade10.analysis.PhaseHierarchyAnalysisResult
import science.atlarge.grade10.analysis.PhaseHierarchyAnalysisRule
import science.atlarge.grade10.analysis.bottlenecks.BottleneckIdentificationResult
import science.atlarge.grade10.analysis.bottlenecks.BottleneckSource
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.execution.PhaseType
import science.atlarge.grade10.model.execution.PhaseTypeRepeatability
import science.atlarge.grade10.model.resources.Metric
import science.atlarge.grade10.model.resources.MetricType
import science.atlarge.grade10.util.FractionalTimeSliceCount
import science.atlarge.grade10.util.TimeSliceCount

class BottleneckDurationPass(
        private val includeMetricBottlenecks: Boolean = false
) : PerformanceIssueIdentificationPass<BottleneckDurationPhaseResult> {

    override val passName: String
        get() = "Bottleneck Duration"
    override val description: String
        get() = "Bottleneck Duration"

    override fun getRule(input: PerformanceIssueIdentificationPassInput):
            PhaseHierarchyAnalysisRule<BottleneckDurationPhaseResult> {
        return BottleneckAnalysisRule(input.bottleneckIdentificationResult, includeMetricBottlenecks)
    }

    override fun extractPerformanceIssues(
            analysisResult: PhaseHierarchyAnalysisResult<BottleneckDurationPhaseResult>
    ): Iterable<PerformanceIssue> {
        return analysisResult.phases.flatMap { phase ->
            val phaseResults = analysisResult[phase]
            phaseResults.bottleneckStatisticsPerPhaseTypeAndSource.flatMap { (targetPhaseType, bottlenecks) ->
                bottlenecks.map { (source, statistics) ->
                    BottleneckDurationPerformanceIssue(phase, targetPhaseType, source, statistics)
                }
            }
        }
    }

}

private class BottleneckAnalysisRule(
        private val bottleneckIdentificationResult: BottleneckIdentificationResult,
        private val includeMetricBottlenecks: Boolean
) : PhaseHierarchyAnalysisRule<BottleneckDurationPhaseResult> {

    override fun analyzeLeafPhase(leafPhase: Phase): BottleneckDurationPhaseResult {
        val results = hashMapOf<BottleneckSource, BottleneckDurationStatistics>()

        val metricTypeBottlenecks = bottleneckIdentificationResult.metricTypeBottlenecks[leafPhase]
        results[BottleneckSource.NoBottleneck] = BottleneckDurationStatistics(metricTypeBottlenecks.timeNotBottlenecked)
        metricTypeBottlenecks.metricTypes.forEach { metricType ->
            results[BottleneckSource.MetricTypeBottleneck(metricType)] =
                    BottleneckDurationStatistics(metricTypeBottlenecks.timeBottleneckedOnMetricType(metricType))
        }

        if (includeMetricBottlenecks) {
            val metricBottlenecks = bottleneckIdentificationResult.metricBottlenecks[leafPhase]
            metricBottlenecks.metrics.forEach { metric ->
                results[BottleneckSource.MetricBottleneck(metric)] =
                        BottleneckDurationStatistics(metricBottlenecks.timeBottleneckedOnMetric(metric))
            }
        }

        return BottleneckDurationPhaseResult(mapOf(leafPhase.type to results))
    }

    override fun combineSubphaseResults(
            compositePhase: Phase,
            subphaseResults: Map<Phase, BottleneckDurationPhaseResult>
    ): BottleneckDurationPhaseResult {
        val results = hashMapOf<PhaseType, Map<BottleneckSource, BottleneckDurationStatistics>>()
        subphaseResults.entries.groupBy({ it.key.type }, { it.value.bottleneckStatisticsPerPhaseTypeAndSource })
                .forEach { subphaseType, subphaseTypeResults ->
                    if (subphaseTypeResults.size == 1) {
                        results.putAll(subphaseTypeResults.first())
                    } else {
                        subphaseTypeResults.flatMap { it.toList() }
                                .groupBy({ it.first }, { it.second })
                                .forEach { phaseType, bottleneckStatistics ->
                                    results[phaseType] = combinePhaseBottleneckStatistics(bottleneckStatistics,
                                            subphaseType.repeatability)
                                }
                    }
                }
        return BottleneckDurationPhaseResult(results)
    }

    private fun combinePhaseBottleneckStatistics(
            statistics: List<Map<BottleneckSource, BottleneckDurationStatistics>>,
            phaseTypeRepeatability: PhaseTypeRepeatability
    ): Map<BottleneckSource, BottleneckDurationStatistics> {
        val isPivotSequential = when (phaseTypeRepeatability) {
            is PhaseTypeRepeatability.SequentialRepeated -> true
            is PhaseTypeRepeatability.ConcurrentRepeated -> false
            else -> throw IllegalArgumentException()
        }
        val phaseCount = statistics.size
        return statistics.flatMap { it.toList() }
                .groupBy({ it.first }, { it.second })
                .map { (bottleneckSource, bottleneckStatistics) ->
                    var totalDuration = 0L
                    var sumEstimatedImpact = 0.0
                    for (bottleneckStatistic in bottleneckStatistics) {
                        totalDuration += bottleneckStatistic.totalDurationAcrossPhases
                        sumEstimatedImpact += bottleneckStatistic.estimatedRuntimeImpact
                    }
                    bottleneckSource to BottleneckDurationStatistics(totalDuration,
                            if (isPivotSequential) sumEstimatedImpact else sumEstimatedImpact / phaseCount)
                }
                .toMap()
    }

}

class BottleneckDurationPhaseResult(
        val bottleneckStatisticsPerPhaseTypeAndSource: Map<PhaseType, Map<BottleneckSource, BottleneckDurationStatistics>>
) {

    val phaseTypes = bottleneckStatisticsPerPhaseTypeAndSource.keys

    fun bottleneckSourcesForPhaseType(phaseType: PhaseType): Set<BottleneckSource> {
        return bottleneckStatisticsPerPhaseTypeAndSource[phaseType]?.keys ?: emptySet()
    }

    operator fun get(phaseType: PhaseType): Map<BottleneckSource, BottleneckDurationStatistics> =
            bottleneckStatisticsPerPhaseTypeAndSource[phaseType]
                    ?: throw IllegalArgumentException("No result found for phase type \"${phaseType.path}\"")

    operator fun get(phaseType: PhaseType, bottleneckSource: BottleneckSource): BottleneckDurationStatistics =
            this[phaseType][bottleneckSource] ?: throw IllegalArgumentException(
                    "No result found for phase type \"${phaseType.path}\" and source \"$bottleneckSource\"")

}

data class BottleneckDurationStatistics(
        val totalDurationAcrossPhases: TimeSliceCount,
        val estimatedRuntimeImpact: FractionalTimeSliceCount = totalDurationAcrossPhases.toDouble()
)

class BottleneckDurationPerformanceIssue(
        val aggregatePhase: Phase,
        val targetPhaseType: PhaseType,
        val bottleneckSource: BottleneckSource,
        val bottleneckDurationStatistics: BottleneckDurationStatistics
) : PerformanceIssue {

    override val affectedPhases: Set<Phase> = setOf(aggregatePhase)
    override val affectedPhaseTypes: Set<PhaseType> = setOf(targetPhaseType)
    override val affectedMetrics: Set<Metric>? = when (bottleneckSource) {
        is BottleneckSource.MetricBottleneck -> setOf(bottleneckSource.metric)
        is BottleneckSource.MetricTypeBottleneck -> null
        BottleneckSource.NoBottleneck -> null
    }
    override val affectedMetricTypes: Set<MetricType>? = when (bottleneckSource) {
        is BottleneckSource.MetricBottleneck -> null
        is BottleneckSource.MetricTypeBottleneck -> setOf(bottleneckSource.metricType)
        BottleneckSource.NoBottleneck -> null
    }

    override val estimatedImpact: FractionalTimeSliceCount
        get() = bottleneckDurationStatistics.estimatedRuntimeImpact

    override fun toDisplayString(): String {
        return if (aggregatePhase.type === targetPhaseType) {
            "Bottlenecks on source $bottleneckSource for phase \"${aggregatePhase.path}\""
        } else {
            "Bottlenecks on source $bottleneckSource for all phases of type \"${targetPhaseType.path}\"" +
                    " descending from phase \"${aggregatePhase.path}\""
        }
    }

}
