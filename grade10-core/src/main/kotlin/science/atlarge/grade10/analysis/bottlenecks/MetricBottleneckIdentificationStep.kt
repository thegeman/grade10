package science.atlarge.grade10.analysis.bottlenecks

import science.atlarge.grade10.analysis.PhaseHierarchyAnalysis
import science.atlarge.grade10.analysis.PhaseHierarchyAnalysisResult
import science.atlarge.grade10.analysis.PhaseHierarchyAnalysisRule
import science.atlarge.grade10.analysis.attribution.*
import science.atlarge.grade10.model.execution.ExecutionModel
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.Metric
import science.atlarge.grade10.util.TimeSliceCount

object MetricBottleneckIdentificationStep {

    fun execute(
            executionModel: ExecutionModel,
            bottleneckIdentificationSettings: BottleneckIdentificationSettings,
            resourceAttributionResult: ResourceAttributionResult
    ): MetricBottleneckIdentificationStepResult {
        return PhaseHierarchyAnalysis.analyze(
                executionModel,
                MetricBottleneckIdentificationRule(
                        bottleneckIdentificationSettings,
                        resourceAttributionResult
                )
        )
    }

}

typealias MetricBottleneckIdentificationStepResult = PhaseHierarchyAnalysisResult<PhaseMetricBottlenecks>

class PhaseMetricBottlenecks(
        val phase: Phase,
        private val metricBottlenecks: Map<Metric, () -> BottleneckStatusIterator>
) {

    val metrics: Set<Metric>
        get() = metricBottlenecks.keys

    private var cachedTimeNotBottlenecked: TimeSliceCount = 0L
    private val cachedBottleneckDurations: MutableMap<Metric, TimeSliceCount> = mutableMapOf()
    private val cachedTotalBottlenecks: BottleneckStatusArray = BottleneckStatusArray(phase.timeSliceDuration.toInt())
    private var cacheInitialized = false

    val timeNotBottlenecked: TimeSliceCount
        get() {
            initializeCache()
            return cachedTimeNotBottlenecked
        }

    fun timeBottleneckedOnMetric(metric: Metric): TimeSliceCount {
        initializeCache()
        return cachedBottleneckDurations[metric]
                ?: throw IllegalArgumentException("No result found for metric \"${metric.path}\"")
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

    fun bottleneckIterator(metric: Metric): BottleneckStatusIterator {
        return metricBottlenecks[metric]?.invoke()
                ?: throw IllegalArgumentException("No result found for metric \"${metric.path}\"")
    }

    private fun initializeCache() {
        if (cacheInitialized) {
            return
        }

        synchronized(this) {
            if (!cacheInitialized) {
                metricBottlenecks.keys.forEach { addMetricToCache(it) }

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

    private fun addMetricToCache(metric: Metric) {
        var bottleneckDuration = 0L
        val iterator = metricBottlenecks[metric]!!.invoke()
        for (i in 0 until cachedTotalBottlenecks.size) {
            val nextStatus = iterator.nextBottleneckStatus()
            cachedTotalBottlenecks[i] = maxOf(cachedTotalBottlenecks[i], nextStatus)
            if (nextStatus != BottleneckStatusConstants.NONE) {
                bottleneckDuration++
            }
        }
        cachedBottleneckDurations[metric] = bottleneckDuration
    }

}

private class MetricBottleneckIdentificationRule(
        private val bottleneckIdentificationSettings: BottleneckIdentificationSettings,
        private val resourceAttributionResult: ResourceAttributionResult
) : PhaseHierarchyAnalysisRule<PhaseMetricBottlenecks> {

    override fun analyzeLeafPhase(leafPhase: Phase): PhaseMetricBottlenecks {
        val phaseAttribution = resourceAttributionResult.resourceAttribution[leafPhase]
        val consumableMetricBottlenecks = phaseAttribution.consumableMetrics
                .map { consumableMetric ->
                    consumableMetric to {
                        val activePhaseDetection = resourceAttributionResult.activePhaseDetection
                        val resourceSampling = resourceAttributionResult.resourceSampling
                        LeafPhaseConsumableMetricBottleneckIterator(
                                leafPhase,
                                consumableMetric,
                                activePhaseDetection.activeIterator(leafPhase),
                                phaseAttribution.iterator(consumableMetric),
                                resourceSampling.sampleIterator(consumableMetric, leafPhase.firstTimeSlice,
                                        leafPhase.lastTimeSlice),
                                bottleneckIdentificationSettings
                        )
                    }
                }
                .toMap()
        val blockingMetricBottlenecks = phaseAttribution.blockingMetrics
                .map { blockingMetric ->
                    blockingMetric to {
                        LeafPhaseBlockingMetricBottleneckIterator(
                                phaseAttribution.iterator(blockingMetric)
                        )
                    }
                }
                .toMap()
        val metricBottlenecks = consumableMetricBottlenecks + blockingMetricBottlenecks
        return PhaseMetricBottlenecks(leafPhase, metricBottlenecks)
    }

    override fun combineSubphaseResults(
            compositePhase: Phase,
            subphaseResults: Map<Phase, PhaseMetricBottlenecks>
    ): PhaseMetricBottlenecks {
        val phaseMetricPairs = subphaseResults.flatMap { (subphase, results) ->
            results.metrics.map { subphase to it }
        }
        val phasesPerMetric = phaseMetricPairs.groupBy({ it.second }, { it.first })
        val iteratorPerMetric = phasesPerMetric
                .map { (metric, subphases) ->
                    val bottleneckIterators = subphases
                            .map {
                                it to { subphaseResults[it]!!.bottleneckIterator(metric) }
                            }
                            .toMap()
                    metric to {
                        CompositePhaseBottleneckIterator(compositePhase, bottleneckIterators,
                                bottleneckIdentificationSettings.bottleneckPredicate)
                    }
                }
                .toMap()
        return PhaseMetricBottlenecks(compositePhase, iteratorPerMetric)
    }

}

internal class LeafPhaseBlockingMetricBottleneckIterator(
        private val blockingMetricAttributionIterator: BlockingMetricAttributionIterator
) : BottleneckStatusIterator {

    override fun hasNext(): Boolean = blockingMetricAttributionIterator.hasNext()

    override fun nextBottleneckStatus(): BottleneckStatus {
        return if (blockingMetricAttributionIterator.nextIsBlocked()) {
            BottleneckStatusConstants.GLOBAL
        } else {
            BottleneckStatusConstants.NONE
        }
    }

}

internal class LeafPhaseConsumableMetricBottleneckIterator(
        phase: Phase,
        metric: Metric.Consumable,
        phaseActiveIterator: PhaseActiveIterator,
        consumableMetricAttributionIterator: ConsumableMetricAttributionIterator,
        metricSampleIterator: MetricSampleIterator,
        bottleneckIdentificationSettings: BottleneckIdentificationSettings
) : BottleneckStatusIterator {

    private val bottlenecks: BottleneckStatusArray = BottleneckStatusArray(phase.timeSliceDuration.toInt())
    var nextIndex = 0

    init {
        val localThresholdFactor = bottleneckIdentificationSettings.localBottleneckThresholdFactor(metric, phase)
        val globalThreshold = bottleneckIdentificationSettings.globalBottleneckThresholdFactor(metric) *
                metric.capacity
        for (i in 0 until bottlenecks.size) {
            consumableMetricAttributionIterator.computeNext()
            val attributedUsage = consumableMetricAttributionIterator.attributedUsage
            val availableCapacity = consumableMetricAttributionIterator.availableCapacity
            val metricSample = metricSampleIterator.nextSample()
            val isActive = phaseActiveIterator.nextIsActive()
            bottlenecks[i] = when {
                !isActive -> BottleneckStatusConstants.NONE
                metricSample >= globalThreshold -> BottleneckStatusConstants.GLOBAL
                attributedUsage >= availableCapacity * localThresholdFactor -> BottleneckStatusConstants.LOCAL
                else -> BottleneckStatusConstants.NONE
            }
        }
    }

    override fun hasNext(): Boolean = nextIndex < bottlenecks.size

    override fun nextBottleneckStatus(): BottleneckStatus {
        val value = bottlenecks[nextIndex]
        nextIndex++
        return value
    }

}

internal class CompositePhaseBottleneckIterator(
        private val phase: Phase,
        private val subphaseBottlenecks: Map<Phase, () -> BottleneckStatusIterator>,
        private val bottleneckPredicate: PerPhaseBottleneckPredicate
) : BottleneckStatusIterator {

    private val phaseList = subphaseBottlenecks.keys
            .filter {
                it.firstTimeSlice <= phase.lastTimeSlice &&
                        it.lastTimeSlice >= phase.firstTimeSlice &&
                        it.timeSliceDuration > 0
            }
            .toTypedArray()
    private val phaseIndexToActiveIndex = IntArray(phaseList.size) { -1 }
    private val activeIndexToPhaseIndex = IntArray(phaseList.size) { -1 }
    private var activePhaseCount = 0

    private val activePhaseIterators = Array<BottleneckStatusIterator?>(phaseList.size) { null }
    private val activePhaseStatuses = BottleneckStatusArray(phaseList.size)

    private val phaseIndexByStartTimeSlice = (0 until phaseList.size).sortedBy { phaseList[it].firstTimeSlice }
    private var phasesStarted = 0
    private var nextPhaseStartTimeSlice: Long

    private val phaseIndexByEndTimeSlice = (0 until phaseList.size).sortedBy { phaseList[it].lastTimeSlice }
    private var phasesEnded = 0
    private var nextPhaseEndTimeSlice: Long

    private var nextTimeSlice = phase.firstTimeSlice

    private val phaseAndBottleneckStatusIterator = PhaseAndBottleneckStatusIterator(phaseList, activeIndexToPhaseIndex,
            activePhaseStatuses)

    init {
        nextPhaseStartTimeSlice = 0L
        nextPhaseEndTimeSlice = 0L
    }

    override fun hasNext(): Boolean = nextTimeSlice <= phase.lastTimeSlice

    override fun nextBottleneckStatus(): BottleneckStatus {
        // Advance one time slice
        val currentTime = nextTimeSlice
        nextTimeSlice++
        // - Check if any new phases start
        while (nextPhaseStartTimeSlice == currentTime) {
            val startingPhaseIndex = phaseIndexByStartTimeSlice[phasesStarted]

            phaseIndexToActiveIndex[startingPhaseIndex] = activePhaseCount
            activeIndexToPhaseIndex[activePhaseCount] = startingPhaseIndex
            activePhaseIterators[activePhaseCount] = subphaseBottlenecks[phaseList[activePhaseCount]]!!.invoke()
            activePhaseCount++

            phasesStarted++
            nextPhaseStartTimeSlice = if (phasesStarted < phaseIndexByStartTimeSlice.size) {
                phaseList[phaseIndexByStartTimeSlice[phasesStarted]].firstTimeSlice
            } else {
                phase.firstTimeSlice - 1
            }
        }
        // - Process all active phases
        for (i in 0 until activePhaseCount) {
            activePhaseStatuses[i] = activePhaseIterators[i]!!.nextBottleneckStatus()
        }
        val combinedStatus = bottleneckPredicate.combineSubPhaseBottlenecks(phaseAndBottleneckStatusIterator)
        // - Check if any phases end
        while (nextPhaseEndTimeSlice == currentTime) {
            val endingPhaseIndex = phaseIndexByEndTimeSlice[phasesStarted]
            val activeIndex = phaseIndexToActiveIndex[endingPhaseIndex]

            phaseIndexToActiveIndex[endingPhaseIndex] = -1
            if (activeIndex != activePhaseCount - 1) {
                val swapPhaseIndex = activeIndexToPhaseIndex[activePhaseCount - 1]
                phaseIndexToActiveIndex[swapPhaseIndex] = activeIndex
                activeIndexToPhaseIndex[activeIndex] = swapPhaseIndex
                activePhaseIterators[activeIndex] = activePhaseIterators[activePhaseCount - 1]
            }
            activePhaseCount--
            activeIndexToPhaseIndex[activePhaseCount] = -1
            activePhaseIterators[activePhaseCount] = null

            phasesEnded++
            nextPhaseEndTimeSlice = if (phasesEnded < phaseIndexByEndTimeSlice.size) {
                phaseList[phaseIndexByEndTimeSlice[phasesEnded]].lastTimeSlice
            } else {
                phase.firstTimeSlice - 1
            }
        }

        return combinedStatus
    }

}

private class PhaseAndBottleneckStatusIterator(
        private val phases: Array<Phase>,
        private val activePhaseIndices: IntArray,
        private val activePhaseStatuses: BottleneckStatusArray
) : Iterator<PhaseAndBottleneckStatus> {

    private var pointer = 0
    private var activePhaseCount = 0
    private val phaseBottleneckPair = PhaseAndBottleneckStatus()

    fun reset(activePhaseCount: Int) {
        this.pointer = 0
        this.activePhaseCount = activePhaseCount
    }

    override fun hasNext(): Boolean = pointer < activePhaseCount

    override fun next(): PhaseAndBottleneckStatus {
        val index = activePhaseIndices[pointer]
        phaseBottleneckPair.phase = phases[index]
        phaseBottleneckPair.bottleneckStatus = activePhaseStatuses[index]
        pointer++
        return phaseBottleneckPair
    }

}
