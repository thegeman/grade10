package science.atlarge.grade10.analysis.attribution

import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.Metric
import science.atlarge.grade10.serialization.Grade10Deserializer
import science.atlarge.grade10.serialization.Grade10Serializer
import science.atlarge.grade10.util.TimeSliceId

interface ResourceSamplingStep {

    fun execute(
            metrics: List<Metric.Consumable>,
            loadComputationStepResult: LoadComputationStepResult,
            startTimeSlice: TimeSliceId,
            endTimeSliceInclusive: TimeSliceId
    ): ResourceSamplingStepResult {
        val metricSamples = metrics.map { metric ->
            metric to sampleMetric(
                    metric,
                    loadComputationStepResult,
                    startTimeSlice,
                    endTimeSliceInclusive
            )
        }.toMap()
        return ResourceSamplingStepResult(metricSamples, startTimeSlice, endTimeSliceInclusive)
    }

    fun execute(
            metrics: List<Metric.Consumable>,
            loadComputationStepResult: LoadComputationStepResult,
            phases: List<Phase>
    ): ResourceSamplingStepResult {
        val startTimeSlice = phases.map { it.firstTimeSlice }.min() ?: 0L
        val endTimeSliceInclusive = phases.map { it.lastTimeSlice }.max() ?: -1L
        return execute(metrics, loadComputationStepResult, startTimeSlice, endTimeSliceInclusive)
    }

    fun sampleMetric(
            metric: Metric.Consumable,
            loadComputationStepResult: LoadComputationStepResult,
            startTimeSlice: TimeSliceId,
            endTimeSliceInclusive: TimeSliceId
    ): DoubleArray

}

object DefaultResourceSamplingStep : ResourceSamplingStep {

    override fun sampleMetric(
            metric: Metric.Consumable,
            loadComputationStepResult: LoadComputationStepResult,
            startTimeSlice: TimeSliceId,
            endTimeSliceInclusive: TimeSliceId
    ): DoubleArray {
        val timeSliceCount = (endTimeSliceInclusive - startTimeSlice + 1).toInt()
        if (timeSliceCount <= 0) {
            return DoubleArray(0)
        }

        val samples = DoubleArray(timeSliceCount)
        val observationIterator = metric.observedUsage.observationIterator()

        while (observationIterator.hasNext()) {
            observationIterator.nextObservationPeriod()
            if (observationIterator.periodStartTimeSlice > endTimeSliceInclusive) {
                break
            } else {
                val sample = observationIterator.observation
                val fromIndex = maxOf(0,
                        (observationIterator.periodStartTimeSlice - startTimeSlice).toInt())
                val toIndex = minOf(timeSliceCount - 1,
                        (observationIterator.periodEndTimeSlice - startTimeSlice).toInt())
                for (i in fromIndex..toIndex) {
                    samples[i] = sample
                }
            }
        }

        return samples
    }

}

object PhaseAwareResourceSamplingStep : ResourceSamplingStep {

    override fun sampleMetric(
            metric: Metric.Consumable,
            loadComputationStepResult: LoadComputationStepResult,
            startTimeSlice: TimeSliceId,
            endTimeSliceInclusive: TimeSliceId
    ): DoubleArray {
        val timeSliceCount = (endTimeSliceInclusive - startTimeSlice + 1).toInt()
        if (timeSliceCount <= 0) {
            return DoubleArray(0)
        }

        val samples = DoubleArray(timeSliceCount)
        var lastSampleIndexSet = -1

        var greedyLoadCache = DoubleArray(0)
        var sinkLoadCache = IntArray(0)
        var capacityCache = DoubleArray(0)

        val observationIterator = metric.observedUsage.observationIterator()
        while (observationIterator.hasNext()) {
            observationIterator.nextObservationPeriod()
            if (observationIterator.periodStartTimeSlice > endTimeSliceInclusive) {
                break
            } else if (observationIterator.periodEndTimeSlice < startTimeSlice) {
                continue
            } else {
                val fromTimeSlice = maxOf(startTimeSlice, observationIterator.periodStartTimeSlice)
                val toTimeSlice = minOf(endTimeSliceInclusive, observationIterator.periodEndTimeSlice)
                val periodLength = (toTimeSlice - fromTimeSlice + 1).toInt()
                val indexOffset = (fromTimeSlice - startTimeSlice).toInt()

                // Sanity check: filling samples in increasing order of index
                require(indexOffset > lastSampleIndexSet)
                lastSampleIndexSet = indexOffset + periodLength - 1

                // Resize greedyLoadCache, sinkLoadCache, and capacityCache if needed
                if (periodLength > greedyLoadCache.size) {
                    greedyLoadCache = DoubleArray(periodLength * 2)
                    sinkLoadCache = IntArray(periodLength * 2)
                    capacityCache = DoubleArray(periodLength * 2)
                }

                // Fill capacityCache
                for (i in 0 until periodLength) {
                    capacityCache[i] = metric.capacity
                }

                // Fill greedyLoadCache and sinkLoadCache from the LoadComputationStep results
                val loadIterator = loadComputationStepResult.loadIterator(metric, fromTimeSlice, toTimeSlice)
                var cacheIndex = 0
                while (loadIterator.hasNext()) {
                    loadIterator.computeNext()
                    greedyLoadCache[cacheIndex] = loadIterator.greedyLoad
                    sinkLoadCache[cacheIndex] = loadIterator.sinkLoad
                    cacheIndex++
                }

                // Distribute the total sample over greedy, sink, and background load
                var remainingSample = observationIterator.observation * observationIterator.periodTimeSliceCount
                // - Greedy
                if (remainingSample > 0.0) {
                    var remainingGreedyLoad = 0.0
                    for (i in 0 until periodLength) {
                        if (capacityCache[i] > 0.0) {
                            remainingGreedyLoad += greedyLoadCache[i]
                        }
                    }
                    val greedyAssignmentOrder = (0 until periodLength)
                            .filter { greedyLoadCache[it] > 0.0 && capacityCache[it] > 0.0 }
                            .sortedByDescending { greedyLoadCache[it] / capacityCache[it] }
                    greedyAssignmentOrder.forEach { i ->
                        val deltaSample = minOf(remainingSample * greedyLoadCache[i] / remainingGreedyLoad,
                                capacityCache[i])
                        samples[indexOffset + i] += deltaSample
                        capacityCache[i] -= deltaSample
                        remainingSample -= deltaSample
                        remainingGreedyLoad -= greedyLoadCache[i]
                    }
                }
                // - Sink
                if (remainingSample > 0.0) {
                    var remainingSinkLoad = 0
                    for (i in 0 until periodLength) {
                        if (capacityCache[i] > 0.0) {
                            remainingSinkLoad += sinkLoadCache[i]
                        }
                    }
                    val sinkAssignmentOrder = (0 until periodLength)
                            .filter { sinkLoadCache[it] > 0.0 && capacityCache[it] > 0.0 }
                            .sortedByDescending { sinkLoadCache[it] / capacityCache[it] }
                    sinkAssignmentOrder.forEach { i ->
                        val deltaSample = minOf(remainingSample * sinkLoadCache[i] / remainingSinkLoad,
                                capacityCache[i])
                        samples[indexOffset + i] += deltaSample
                        capacityCache[i] -= deltaSample
                        remainingSample -= deltaSample
                        remainingSinkLoad--
                    }
                }
                // - Background
                if (remainingSample > 0.0) {
                    var remainingLoad = 0
                    for (i in 0 until periodLength) {
                        if (capacityCache[i] > 0.0) {
                            remainingLoad++
                        }
                    }
                    val assignmentOrder = (0 until periodLength)
                            .filter { capacityCache[it] > 0.0 }
                            .sortedByDescending { 1.0 / capacityCache[it] }
                    assignmentOrder.forEach { i ->
                        val deltaSample = minOf(remainingSample / remainingLoad, capacityCache[i])
                        samples[indexOffset + i] += deltaSample
                        capacityCache[i] -= deltaSample
                        remainingSample -= deltaSample
                        remainingLoad--
                    }
                }
            }
        }

        return samples
    }

}

class ResourceSamplingStepResult(
        private val metricSamples: Map<Metric.Consumable, DoubleArray>,
        private val startTimeSlice: TimeSliceId,
        private val endTimeSliceInclusive: TimeSliceId
) {

    val metrics: Set<Metric.Consumable>
        get() = metricSamples.keys

    init {
        val numTimeSlices = (endTimeSliceInclusive - startTimeSlice + 1).toInt()
        metricSamples.values.forEach {
            require(it.size == numTimeSlices) {
                "All sample arrays must span the full range of time slices"
            }
        }
    }

    fun sampleIterator(
            metric: Metric.Consumable,
            fromTimeSlice: TimeSliceId = startTimeSlice,
            toTimeSliceInclusive: TimeSliceId = endTimeSliceInclusive
    ): MetricSampleIterator {
        require(metric in metricSamples) { "No result found for metric \"${metric.path}" }
        return MetricSampleIterator(
                metric.capacity,
                metricSamples[metric]!!,
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
            // 4.2. Sample array
            output.writeDoubles(metricSamples[metric]!!)
        }
    }

    companion object {

        fun deserialize(input: Grade10Deserializer): ResourceSamplingStepResult {
            // 1. First time slice
            val startTimeSlice = input.readLong()
            // 2. Number of time slices
            val numTimeSlices = input.readVarInt(true)
            val endTimeSliceInclusive = startTimeSlice + numTimeSlices - 1
            // 3. Number of metrics
            val numMetrics = input.readVarInt(true)
            // 4. For each metric:
            val metricSamples = mutableMapOf<Metric.Consumable, DoubleArray>()
            repeat(numMetrics) {
                // 4.1. The metric
                val metric = input.readMetric() as Metric.Consumable
                // 4.2. Sample array
                metricSamples[metric] = input.readDoubles(numTimeSlices)
            }
            return ResourceSamplingStepResult(metricSamples, startTimeSlice, endTimeSliceInclusive)
        }

    }

}

class MetricSampleIterator(
        val capacity: Double,
        private val sampleArray: DoubleArray,
        startIndex: Int = 0,
        private val endIndexInclusive: Int = sampleArray.lastIndex,
        private val startTimeSlice: TimeSliceId
) {

    private var nextIndex = startIndex

    val nextTimeSlice: TimeSliceId
        get() = startTimeSlice + nextIndex

    init {
        require(startIndex >= 0) {
            "Iterator cannot start before the start of the array"
        }
        require(endIndexInclusive <= sampleArray.lastIndex) {
            "Iterator cannot end past the end of the array"
        }
    }

    fun hasNext(): Boolean = nextIndex <= endIndexInclusive

    fun nextSample(): Double {
        val sample = sampleArray[nextIndex]
        nextIndex++
        return sample
    }

}
