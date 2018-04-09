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

class CriticalPathBottleneckDurationPass(
        private val includeMetricBottlenecks: Boolean = false
) : PerformanceIssueIdentificationPass<CriticalPathBottleneckDurationPhaseResult> {

    override val passName: String
        get() = "Critical-path Bottleneck Duration"
    override val description: String
        get() = "Critical-path Bottleneck Duration"

    override fun getRule(input: PerformanceIssueIdentificationPassInput):
            PhaseHierarchyAnalysisRule<CriticalPathBottleneckDurationPhaseResult> {
        return CriticalPathBottleneckAnalysisRule(input.bottleneckIdentificationResult, includeMetricBottlenecks)
    }

    override fun extractPerformanceIssues(
            analysisResult: PhaseHierarchyAnalysisResult<CriticalPathBottleneckDurationPhaseResult>
    ): Iterable<PerformanceIssue> {
        return analysisResult.phases.flatMap { phase ->
            val phaseResults = analysisResult[phase]
            phaseResults.bottleneckStatisticsPerSource.map { (source, statistics) ->
                CriticalPathBottleneckDurationPerformanceIssue(phase, source, statistics)
            }
        }
    }

}

private class CriticalPathBottleneckAnalysisRule(
        private val bottleneckIdentificationResult: BottleneckIdentificationResult,
        private val includeMetricBottlenecks: Boolean
) : PhaseHierarchyAnalysisRule<CriticalPathBottleneckDurationPhaseResult> {

    override fun analyzeLeafPhase(leafPhase: Phase): CriticalPathBottleneckDurationPhaseResult {
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

        return CriticalPathBottleneckDurationPhaseResult(results)
    }

    override fun combineSubphaseResults(
            compositePhase: Phase,
            subphaseResults: Map<Phase, CriticalPathBottleneckDurationPhaseResult>
    ): CriticalPathBottleneckDurationPhaseResult {
        val results = subphaseResults.entries.groupBy({ it.key.type }, { it.value.bottleneckStatisticsPerSource })
                .map { (subphaseType, subphaseTypeResults) ->
                    if (subphaseTypeResults.size == 1) {
                        subphaseType to subphaseTypeResults.first()
                    } else {
                        subphaseType to combinePhaseBottleneckStatistics(subphaseTypeResults,
                                subphaseType.repeatability)
                    }
                }
                .flatMap { (phaseType, phaseTypeResults) ->
                    phaseTypeResults.map { (source, stats) ->
                        source to (phaseType to stats)
                    }
                }
                .groupBy({ it.first }, { it.second })
                .mapValues { it.value.toMap() }
                .map { (source, statsByPhaseType) ->
                    val sequentialImpactByPhaseType = hashMapOf<PhaseType, Double>()
                    fun computeSequentialImpactByPhase(phaseType: PhaseType): Double {
                        return sequentialImpactByPhaseType.getOrPut(phaseType) {
                            val sequentialImpactOfDependencies = phaseType.dependencies.map { dependency ->
                                computeSequentialImpactByPhase(dependency)
                            }.max() ?: 0.0
                            val impactOfPhaseType = statsByPhaseType[phaseType]?.estimatedRuntimeImpact ?: 0.0
                            sequentialImpactOfDependencies + impactOfPhaseType
                        }
                    }

                    val maxSequentialImpact = statsByPhaseType.keys.map(::computeSequentialImpactByPhase).max()!!
                    val totalDuration = statsByPhaseType.values.map { it.totalDurationAcrossPhases }
                            .fold(0L) { a, b -> a + b }
                    source to BottleneckDurationStatistics(totalDuration, maxSequentialImpact)
                }
                .toMap()
        return CriticalPathBottleneckDurationPhaseResult(results)
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

class CriticalPathBottleneckDurationPhaseResult(
        val bottleneckStatisticsPerSource: Map<BottleneckSource, BottleneckDurationStatistics>
) {

    val bottleneckSources = bottleneckStatisticsPerSource.keys

    operator fun get(bottleneckSource: BottleneckSource): BottleneckDurationStatistics =
            bottleneckStatisticsPerSource[bottleneckSource]
                    ?: throw IllegalArgumentException("No result found for source \"$bottleneckSource\"")

}

class CriticalPathBottleneckDurationPerformanceIssue(
        val aggregatePhase: Phase,
        val bottleneckSource: BottleneckSource,
        val bottleneckDurationStatistics: BottleneckDurationStatistics
) : PerformanceIssue {

    override val affectedPhases: Set<Phase> = setOf(aggregatePhase)
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
        return "Bottlenecks on source $bottleneckSource for critical path of phase \"${aggregatePhase.path}\""
    }

}
