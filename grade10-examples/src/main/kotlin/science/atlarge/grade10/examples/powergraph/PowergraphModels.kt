package science.atlarge.grade10.examples.powergraph

import science.atlarge.grade10.metrics.TimePeriodList
import science.atlarge.grade10.model.execution.PhaseTypeRepeatability
import science.atlarge.grade10.model.execution.buildExecutionModelSpecification
import science.atlarge.grade10.model.resources.*
import science.atlarge.grade10.monitor.util.readLELong
import science.atlarge.grade10.util.TimestampNsRange
import java.io.File
import java.nio.file.Path

object PowergraphExecutionModel {

    val specification = buildExecutionModelSpecification {
        newSubphaseType("LoadGraph") {
            newSubphaseType("WorkerLoadGraph", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("worker"))
        }
        newSubphaseType("InitializeEngine") {
            newSubphaseType("WorkerInitializeEngine", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("worker"))
        }
        newSubphaseType("ComputeMain") {
            newSubphaseType("Superstep", repeatability = PhaseTypeRepeatability.SequentialRepeated("superstep")) {
                for (stage in listOf("ExchangeMessages", "ReceiveMessages", "Gather", "Apply", "Scatter")) {
                    newSubphaseType(stage) {
                        newSubphaseType("Worker$stage", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("worker")) {
                            newSubphaseType("${stage}Communication")
                            newSubphaseType("${stage}Thread", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("thread"))
                        }
                    }
                }
                newSubphaseType("CountActiveVertices")

                subphaseType("ReceiveMessages") after subphaseType("ExchangeMessages")
                subphaseType("CountActiveVertices") after subphaseType("ReceiveMessages")
                subphaseType("Gather") after subphaseType("CountActiveVertices")
                subphaseType("Apply") after subphaseType("Gather")
                subphaseType("Scatter") after subphaseType("Apply")
            }
        }
        newSubphaseType("WriteResult") {
            newSubphaseType("WorkerWriteResult", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("worker"))
        }

        subphaseType("InitializeEngine") after subphaseType("LoadGraph")
        subphaseType("ComputeMain") after subphaseType("InitializeEngine")
        subphaseType("WriteResult") after subphaseType("ComputeMain")
    }

}

object PowergraphResourceModel {

    const val ROOT_NAME = "powergraph"
    const val WORKER_NAME = "worker"
    const val THREAD_NAME = "thread"

    const val READ_GATHER_MSG_NAME = "read-gather-messages"

    val specification = buildResourceModelSpecification {
        newSubresourceType(ROOT_NAME) {
            newSubresourceType(WORKER_NAME, repeatability = ResourceTypeRepeatability.Many("id")) {
                newSubresourceType(THREAD_NAME, repeatability = ResourceTypeRepeatability.Many("id")) {
                    newMetricType(READ_GATHER_MSG_NAME, type = MetricClass.BLOCKING)
                }
            }
        }
    }

    private val gatherWaitFilenameRegex = Regex("""gather-waits-(\d*)-(\d*)""")

    fun fromLogDirectories(logDirectories: List<Path>): ResourceModel {
        val gatherWaitMetricFiles = logDirectories.asSequence()
                .flatMap { it.toFile().walk() }
                .filter(File::isFile)
                .mapNotNull { logFile ->
                    gatherWaitFilenameRegex.find(logFile.name)?.let { match ->
                        val workerId = match.groupValues[1]
                        val threadId = match.groupValues[2]
                        workerId to (threadId to logFile)
                    }
                }
                .groupBy({ it.first }, { it.second })
                .mapValues { (_, v) -> v.map { (tid, file) -> tid to parseGatherWaitMetric(file) }.toMap() }

        return buildResourceModel(specification) {
            newSubresource(ROOT_NAME) {
                gatherWaitMetricFiles.forEach { workerId, threads ->
                    newSubresource(WORKER_NAME, instanceId = workerId) {
                        threads.forEach { threadId, metricData ->
                            newSubresource(THREAD_NAME, instanceId = threadId) {
                                newBlockingMetric(READ_GATHER_MSG_NAME, metricData)
                            }
                        }
                    }
                }
            }
        }
    }

    private fun parseGatherWaitMetric(gatherWaitMetricFile: File): TimePeriodList {
        gatherWaitMetricFile.inputStream().buffered().use { input ->
            val ranges = ArrayList<TimestampNsRange>()
            while (true) {
                try {
                    val endNs = input.readLELong()
                    val durationNs = input.readLELong()
                    val startNs = endNs - durationNs + 1
                    ranges.add(startNs..endNs)
                } catch (e: Exception) {
                    break
                }
            }
            return TimePeriodList(ranges)
        }
    }

}
