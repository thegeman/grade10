package science.atlarge.grade10.analysis.bottlenecks

import science.atlarge.grade10.analysis.PhaseHierarchyAnalysis
import science.atlarge.grade10.analysis.PhaseHierarchyAnalysisResult
import science.atlarge.grade10.analysis.PhaseHierarchyAnalysisRule
import science.atlarge.grade10.analysis.attribution.BlockingMetricAttributionIterator
import science.atlarge.grade10.analysis.attribution.MetricSampleIterator
import science.atlarge.grade10.analysis.attribution.ResourceAttributionResult
import science.atlarge.grade10.model.execution.ExecutionModel
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.MetricType
import science.atlarge.grade10.util.TimeSliceCount

object MetricTypeBottleneckIdentificationStep {

    fun execute(
            executionModel: ExecutionModel,
            bottleneckIdentificationSettings: BottleneckIdentificationSettings,
            resourceAttributionResult: ResourceAttributionResult,
            metricBottleneckIdentificationResult: MetricBottleneckIdentificationStepResult
    ): MetricTypeBottleneckIdentificationStepResult {
        return PhaseHierarchyAnalysis.analyze(
                executionModel,
                MetricTypeBottleneckIdentificationRule(
                        bottleneckIdentificationSettings,
                        resourceAttributionResult,
                        metricBottleneckIdentificationResult
                )
        )
    }

}

typealias MetricTypeBottleneckIdentificationStepResult = PhaseHierarchyAnalysisResult<PhaseMetricTypeBottlenecks>

class PhaseMetricTypeBottlenecks(
        val phase: Phase,
        private val metricTypeBottlenecks: Map<MetricType, () -> BottleneckStatusIterator>
) {

    val metricTypes: Set<MetricType>
        get() = metricTypeBottlenecks.keys

    private var cachedTimeNotBottlenecked: TimeSliceCount = 0L
    private val cachedBottleneckDurations: MutableMap<MetricType, TimeSliceCount> = mutableMapOf()
    private val cachedTotalBottlenecks: BottleneckStatusArray = BottleneckStatusArray(phase.timeSliceDuration.toInt())
    private var cacheInitialized = false

    val timeNotBottlenecked: TimeSliceCount
        get() {
            initializeCache()
            return cachedTimeNotBottlenecked
        }

    fun timeBottleneckedOnMetricType(metricType: MetricType): TimeSliceCount {
        initializeCache()
        return cachedBottleneckDurations[metricType]
                ?: throw IllegalArgumentException("No result found for metric type \"${metricType.path}\"")
    }

    fun bottleneckIterator(): BottleneckStatusIterator {
        initializeCache()
        return object : BottleneckStatusIterator {

            val data = cachedTotalBottlenecks
            var nextIndex = 0

            override fun hasNext(): Boolean = nextIndex < data.size

            override fun nextBottleneckStatus(): BottleneckStatus {
                val value = data[nextIndex]
                nextIndex++
                return value
            }

        }
    }

    fun bottleneckIterator(metricType: MetricType): BottleneckStatusIterator {
        return metricTypeBottlenecks[metricType]?.invoke()
                ?: throw IllegalArgumentException("No result found for metric type \"${metricType.path}\"")
    }

    private fun initializeCache() {
        if (cacheInitialized) {
            return
        }

        synchronized(this) {
            if (!cacheInitialized) {
                metricTypeBottlenecks.keys.forEach { addMetricTypeToCache(it) }

                var durationNotBottlenecked = 0L
                for (i in 0 until cachedTotalBottlenecks.size) {
                    if (cachedTotalBottlenecks[i] == BottleneckStatusConstants.NONE) {
                        durationNotBottlenecked++
                    }
                }
                cachedTimeNotBottlenecked = durationNotBottlenecked

                cacheInitialized = true
            }
        }
    }

    private fun addMetricTypeToCache(metricType: MetricType) {
        var bottleneckDuration = 0L
        val iterator = metricTypeBottlenecks[metricType]!!.invoke()
        for (i in 0 until cachedTotalBottlenecks.size) {
            val nextStatus = iterator.nextBottleneckStatus()
            cachedTotalBottlenecks[i] = maxOf(cachedTotalBottlenecks[i], nextStatus)
            if (nextStatus != BottleneckStatusConstants.NONE) {
                bottleneckDuration++
            }
        }
        cachedBottleneckDurations[metricType] = bottleneckDuration
    }

}

private class MetricTypeBottleneckIdentificationRule(
        private val bottleneckIdentificationSettings: BottleneckIdentificationSettings,
        private val resourceAttributionResult: ResourceAttributionResult,
        private val metricBottleneckIdentificationResult: MetricBottleneckIdentificationStepResult
) : PhaseHierarchyAnalysisRule<PhaseMetricTypeBottlenecks> {

    override fun analyzeLeafPhase(leafPhase: Phase): PhaseMetricTypeBottlenecks {
        val metricBottlenecks = metricBottleneckIdentificationResult[leafPhase]
        val phaseAttribution = resourceAttributionResult.resourceAttribution[leafPhase]

        val consumableMetricsByType = phaseAttribution.consumableMetrics.groupBy { it.type }
        val unusedConsumableMetricsByType = phaseAttribution.unusedConsumableMetrics.groupBy { it.type }
        val consumableMetricTypeBottlenecks = consumableMetricsByType
                .map { (consumableMetricType, metricsForType) ->
                    val unusedMetricsForType = unusedConsumableMetricsByType[consumableMetricType] ?: emptyList()
                    val unusedMetricThresholds = unusedMetricsForType.map {
                        it to bottleneckIdentificationSettings.globalBottleneckThresholdFactor(it)
                    }.toMap()

                    consumableMetricType to {
                        val iterators = metricsForType.map { metricBottlenecks.bottleneckIterator(it) } +
                                unusedMetricsForType.map {
                                    GlobalConsumableResourceBottleneckIterator(
                                            resourceAttributionResult.resourceSampling.sampleIterator(
                                                    it, leafPhase.firstTimeSlice, leafPhase.lastTimeSlice
                                            ),
                                            unusedMetricThresholds[it]!!
                                    )
                                }
                        AggregateBottleneckStatusIterator(iterators)
                    }
                }
                .toMap()

        val blockingMetricsByType = phaseAttribution.blockingMetrics.groupBy { it.type }
        val unusedBlockingMetricsByType = phaseAttribution.unusedBlockingMetrics.groupBy { it.type }
        val blockingMetricTypeBottlenecks = blockingMetricsByType
                .map { (blockingMetricType, metricsForType) ->
                    val unusedMetricsForType = unusedBlockingMetricsByType[blockingMetricType] ?: emptyList()

                    blockingMetricType to {
                        val iterators = metricsForType.map { metricBottlenecks.bottleneckIterator(it) } +
                                unusedMetricsForType.map {
                                    LeafPhaseBlockingMetricBottleneckIterator(
                                            BlockingMetricAttributionIterator(leafPhase, it)
                                    )
                                }
                        AggregateBottleneckStatusIterator(iterators)
                    }
                }
                .toMap()

        val metricTypeBottlenecks = blockingMetricTypeBottlenecks + consumableMetricTypeBottlenecks
        return PhaseMetricTypeBottlenecks(leafPhase, metricTypeBottlenecks)
    }

    override fun combineSubphaseResults(
            compositePhase: Phase,
            subphaseResults: Map<Phase, PhaseMetricTypeBottlenecks>
    ): PhaseMetricTypeBottlenecks {
        val phaseMetricPairs = subphaseResults.flatMap { (subphase, results) ->
            results.metricTypes.map { subphase to it }
        }
        val phasesPerMetricType = phaseMetricPairs.groupBy({ it.second }, { it.first })
        val iteratorPerMetricType = phasesPerMetricType
                .map { (metricType, subphases) ->
                    val bottleneckIterators = subphases
                            .map {
                                it to { subphaseResults[it]!!.bottleneckIterator(metricType) }
                            }
                            .toMap()
                    metricType to {
                        CompositePhaseBottleneckIterator(compositePhase, bottleneckIterators,
                                bottleneckIdentificationSettings.bottleneckPredicate)
                    }
                }
                .toMap()
        return PhaseMetricTypeBottlenecks(compositePhase, iteratorPerMetricType)
    }

}

private class GlobalConsumableResourceBottleneckIterator(
        private val sampleIterator: MetricSampleIterator,
        globalBottleneckThresholdFactor: Double
) : BottleneckStatusIterator {

    private val threshold = globalBottleneckThresholdFactor * sampleIterator.capacity

    override fun hasNext(): Boolean = sampleIterator.hasNext()

    override fun nextBottleneckStatus(): BottleneckStatus =
            if (sampleIterator.nextSample() >= threshold) {
                BottleneckStatusConstants.GLOBAL
            } else {
                BottleneckStatusConstants.NONE
            }

}

private class AggregateBottleneckStatusIterator(
        private val bottleneckStatusIterators: List<BottleneckStatusIterator>
) : BottleneckStatusIterator {

    override fun hasNext(): Boolean = bottleneckStatusIterators[0].hasNext()

    override fun nextBottleneckStatus(): BottleneckStatus {
        var isGlobal = true
        var hasBottleneck = false
        bottleneckStatusIterators.forEach {
            when (it.nextBottleneckStatus()) {
                BottleneckStatusConstants.NONE -> {
                    isGlobal = false
                }
                BottleneckStatusConstants.LOCAL -> {
                    hasBottleneck = true
                    isGlobal = false
                }
                BottleneckStatusConstants.GLOBAL -> {
                    hasBottleneck = true
                }
            }
        }
        return when {
            !hasBottleneck -> BottleneckStatusConstants.NONE
            !isGlobal -> BottleneckStatusConstants.LOCAL
            else -> BottleneckStatusConstants.GLOBAL
        }
    }

}
