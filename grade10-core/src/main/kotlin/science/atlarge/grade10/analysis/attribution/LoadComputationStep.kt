package science.atlarge.grade10.analysis.attribution

import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.Metric
import science.atlarge.grade10.serialization.Grade10Deserializer
import science.atlarge.grade10.serialization.Grade10Serializer
import science.atlarge.grade10.util.TimeSliceId

object LoadComputationStep {

    fun execute(
            phaseMetricMappingCache: PhaseMetricMappingCache,
            resourceAttributionRules: ResourceAttributionRuleProvider,
            activePhaseDetectionStepResult: ActivePhaseDetectionStepResult
    ): LoadComputationStepResult {
        val greedyLoadPerMetric = mutableMapOf<Metric.Consumable, DoubleArray>()
        val sinkLoadPerMetric = mutableMapOf<Metric.Consumable, IntArray>()
        val startTimeSlice = phaseMetricMappingCache.phases.map { it.firstTimeSlice }.min() ?: 0L
        val endTimeSliceInclusive = phaseMetricMappingCache.phases.map { it.lastTimeSlice }.max() ?: -1L

        phaseMetricMappingCache.consumableMetrics.forEach { consumableMetric ->
            val (greedy, sink) = computeGreedyAndSinkLoadForMetric(
                    consumableMetric,
                    phaseMetricMappingCache.consumableMetricToLeafPhaseMapping[consumableMetric] ?: emptyList(),
                    resourceAttributionRules,
                    activePhaseDetectionStepResult,
                    startTimeSlice,
                    endTimeSliceInclusive
            )
            greedyLoadPerMetric[consumableMetric] = greedy
            sinkLoadPerMetric[consumableMetric] = sink
        }

        return LoadComputationStepResult(greedyLoadPerMetric, sinkLoadPerMetric, startTimeSlice, endTimeSliceInclusive)
    }

    private fun computeGreedyAndSinkLoadForMetric(
            metric: Metric.Consumable,
            phases: List<Phase>,
            resourceAttributionRules: ResourceAttributionRuleProvider,
            activePhaseDetectionStepResult: ActivePhaseDetectionStepResult,
            startTimeSlice: TimeSliceId,
            endTimeSliceInclusive: TimeSliceId
    ): Pair<DoubleArray, IntArray> {
        val timeSliceCount = (endTimeSliceInclusive - startTimeSlice + 1).toInt()
        val greedyLoad = DoubleArray(timeSliceCount)
        val sinkLoad = IntArray(timeSliceCount)

        phases.forEach { phase ->
            val rule = resourceAttributionRules[phase, metric]
            val activeIterator = activePhaseDetectionStepResult.activeIterator(phase)
            when (rule) {
                is ConsumableResourceAttributionRule.Greedy -> {
                    var index = (phase.firstTimeSlice - startTimeSlice).toInt()
                    val load = rule.maxRate
                    while (activeIterator.hasNext()) {
                        if (activeIterator.nextIsActive()) {
                            greedyLoad[index] += load
                        }
                        index++
                    }
                }
                ConsumableResourceAttributionRule.Sink -> {
                    var index = (phase.firstTimeSlice - startTimeSlice).toInt()
                    while (activeIterator.hasNext()) {
                        if (activeIterator.nextIsActive()) {
                            sinkLoad[index]++
                        }
                        index++
                    }
                }
                else -> {
                    // Ignore
                }
            }
        }

        return greedyLoad to sinkLoad
    }

}

class LoadComputationStepResult(
        private val greedyLoadPerMetric: Map<Metric.Consumable, DoubleArray>,
        private val sinkLoadPerMetric: Map<Metric.Consumable, IntArray>,
        private val startTimeSlice: TimeSliceId,
        private val endTimeSliceInclusive: TimeSliceId
) {

    val metrics: Set<Metric.Consumable>
        get() = greedyLoadPerMetric.keys

    init {
        val numTimeSlices = (endTimeSliceInclusive - startTimeSlice + 1).toInt()
        greedyLoadPerMetric.values.forEach {
            require(it.size == numTimeSlices) {
                "All load arrays must span the full range of time slices"
            }
        }
        sinkLoadPerMetric.values.forEach {
            require(it.size == numTimeSlices) {
                "All load arrays must span the full range of time slices"
            }
        }

        require(greedyLoadPerMetric.keys == sinkLoadPerMetric.keys) {
            "Greedy and sink loads must be defined for the same metrics"
        }
    }

    fun loadIterator(
            metric: Metric.Consumable,
            fromTimeSlice: TimeSliceId = startTimeSlice,
            toTimeSliceInclusive: TimeSliceId = endTimeSliceInclusive
    ): LoadIterator {
        require(metric in greedyLoadPerMetric) { "Load for metric is not defined" }
        return LoadIterator(
                greedyLoadPerMetric[metric]!!,
                sinkLoadPerMetric[metric]!!,
                startIndex = (fromTimeSlice - startTimeSlice).toInt(),
                endIndexInclusive = (toTimeSliceInclusive - startTimeSlice).toInt(),
                startTimeSlice = fromTimeSlice
        )
    }

    fun serialize(output: Grade10Serializer) {
        // 1. First time slice
        output.writeLong(startTimeSlice)
        // 2. Number of time slices
        val numTimeSlices = (endTimeSliceInclusive - startTimeSlice + 1).toInt()
        output.writeVarInt(numTimeSlices, true)
        // 3. Number of metrics
        output.writeVarInt(metrics.size, true)
        // 4. For each metric:
        metrics.forEach { metric ->
            // 4.1. The metric
            output.write(metric)
            // 4.2. Greedy load array
            output.writeDoubles(greedyLoadPerMetric[metric]!!)
            // 4.3. Sink load array
            output.writeInts(sinkLoadPerMetric[metric]!!, true)
        }
    }

    companion object {

        fun deserialize(input: Grade10Deserializer): LoadComputationStepResult {
            // 1. First time slice
            val startTimeSlice = input.readLong()
            // 2. Number of time slices
            val numTimeSlices = input.readVarInt(true)
            val endTimeSliceInclusive = startTimeSlice + numTimeSlices - 1
            // 3. Number of metrics
            val numMetrics = input.readVarInt(true)
            // 4. For each metric:
            val greedyLoadPerMetric = mutableMapOf<Metric.Consumable, DoubleArray>()
            val sinkLoadPerMetric = mutableMapOf<Metric.Consumable, IntArray>()
            repeat(numMetrics) {
                // 4.1. The metric
                val metric = input.readMetric() as Metric.Consumable
                // 4.2. Greedy load array
                greedyLoadPerMetric[metric] = input.readDoubles(numTimeSlices)
                // 4.3. Sink load array
                sinkLoadPerMetric[metric] = input.readInts(numTimeSlices, true)
            }
            return LoadComputationStepResult(greedyLoadPerMetric, sinkLoadPerMetric, startTimeSlice,
                    endTimeSliceInclusive)
        }

    }

}

class LoadIterator(
        private val greedyLoadArray: DoubleArray,
        private val sinkLoadArray: IntArray,
        startIndex: Int = 0,
        private val endIndexInclusive: Int = greedyLoadArray.lastIndex,
        startTimeSlice: TimeSliceId
) {

    private var nextIndex = startIndex

    var timeSlice: TimeSliceId = startTimeSlice - 1
        private set

    var greedyLoad: Double = 0.0
        private set

    var sinkLoad: Int = 0
        private set

    init {
        require(startIndex >= 0) {
            "Iterator cannot start before the start of the array"
        }
        require(endIndexInclusive <= greedyLoadArray.lastIndex) {
            "Iterator cannot end past the end of the array"
        }
        require(greedyLoadArray.size == sinkLoadArray.size) {
            "Greedy load and sink load arrays must be of the same size"
        }
    }

    fun hasNext(): Boolean = nextIndex <= endIndexInclusive

    fun computeNext() {
        greedyLoad = greedyLoadArray[nextIndex]
        sinkLoad = sinkLoadArray[nextIndex]
        timeSlice++
        nextIndex++
    }

}
