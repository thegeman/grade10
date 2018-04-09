package science.atlarge.grade10.examples.giraph

import science.atlarge.grade10.metrics.TimePeriodList
import science.atlarge.grade10.model.execution.PhaseTypeRepeatability
import science.atlarge.grade10.model.execution.buildExecutionModelSpecification
import science.atlarge.grade10.model.resources.*
import science.atlarge.grade10.records.EventRecord
import science.atlarge.grade10.records.EventRecordType
import science.atlarge.grade10.records.RecordStore
import science.atlarge.grade10.util.TimestampNs

object GiraphExecutionModel {

    val specification = buildExecutionModelSpecification {
        newSubphaseType("Setup") {
            newSubphaseType("WorkerSetup", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("worker"))
        }
        newSubphaseType("LoadGraph") {
            newSubphaseType("WorkerLoadGraph", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("worker"))
        }
        newSubphaseType("Execute") {
            newSubphaseType("Superstep", repeatability = PhaseTypeRepeatability.SequentialRepeated("superstep")) {
                newSubphaseType("WorkerSuperstep", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("worker")) {
                    newSubphaseType("Communicate")
                    newSubphaseType("PreCompute")
                    newSubphaseType("Compute") {
                        newSubphaseType("ComputeThread", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("thread")) {
                            newSubphaseType("PreVertexCompute")
                            newSubphaseType("VertexCompute") {
                                newSubphaseType("ComputePartition", repeatability = PhaseTypeRepeatability.SequentialRepeated("partition"))
                            }
                            newSubphaseType("PostVertexCompute")

                            subphaseType("VertexCompute") after subphaseType("PreVertexCompute")
                            subphaseType("PostVertexCompute") after subphaseType("VertexCompute")
                        }
                    }
                    newSubphaseType("PostCompute")

                    subphaseType("Compute") after subphaseType("PreCompute")
                    subphaseType("PostCompute") after subphaseType("Compute")
                }
            }
        }
        newSubphaseType("Cleanup")

        subphaseType("LoadGraph") after subphaseType("Setup")
        subphaseType("Execute") after subphaseType("LoadGraph")
        subphaseType("Cleanup") after subphaseType("Execute")
    }

}

val giraphModelWithGC = buildExecutionModelSpecification {
    newSubphaseType("Setup") {
        newSubphaseType("WorkerSetup", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("worker"))
    }
    newSubphaseType("LoadGraph") {
        newSubphaseType("WorkerLoadGraph", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("worker"))
    }
    newSubphaseType("Execute") {
        newSubphaseType("Superstep", repeatability = PhaseTypeRepeatability.SequentialRepeated("superstep")) {
            newSubphaseType("WorkerSuperstep", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("worker")) {
                newSubphaseType("Communicate")
                newSubphaseType("PreCompute")
                newSubphaseType("Compute") {
                    newSubphaseType("ComputeThread", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("thread")) {
                        newSubphaseType("PreVertexCompute")
                        newSubphaseType("VertexCompute") {
                            newSubphaseType("ComputePartition", repeatability = PhaseTypeRepeatability.SequentialRepeated("partition"))
                        }
                        newSubphaseType("PostVertexCompute")

                        subphaseType("VertexCompute") after subphaseType("PreVertexCompute")
                        subphaseType("PostVertexCompute") after subphaseType("VertexCompute")
                    }
                }
                newSubphaseType("PostCompute")

                subphaseType("Compute") after subphaseType("PreCompute")
                subphaseType("PostCompute") after subphaseType("Compute")
            }
        }
    }
    newSubphaseType("Cleanup")
    newSubphaseType("GarbageCollectEvents") {
        newSubphaseType("WorkerGarbageCollectEvents", repeatability = PhaseTypeRepeatability.ConcurrentRepeated("worker")) {
            newSubphaseType("GC", repeatability = PhaseTypeRepeatability.SequentialRepeated("gc-id"))
        }
    }

    subphaseType("LoadGraph") after subphaseType("Setup")
    subphaseType("Execute") after subphaseType("LoadGraph")
    subphaseType("Cleanup") after subphaseType("Execute")
}

object GiraphResourceModel {

    const val ROOT_NAME = "giraph"
    const val WORKER_NAME = "worker"
    const val JVM_NAME = "jvm"
    const val COMPUTE_THREAD_NAME = "compute-thread"

    const val GC_NAME = "garbage-collect"
    const val MSG_QUEUE_NAME = "wait-on-message-queue"

    val specification = buildResourceModelSpecification {
        newSubresourceType(ROOT_NAME) {
            newSubresourceType(WORKER_NAME, ResourceTypeRepeatability.Many("id")) {
                newSubresourceType(JVM_NAME) {
                    newMetricType(GC_NAME, MetricClass.BLOCKING)
                }

                newSubresourceType(COMPUTE_THREAD_NAME, ResourceTypeRepeatability.Many("id")) {
                    newMetricType(MSG_QUEUE_NAME, MetricClass.BLOCKING)
                }
            }
        }
    }

    fun fromRecords(records: RecordStore): ResourceModel {
        val garbageCollectRecords = hashMapOf<String /*worker*/, MutableList<LongRange>>()
        val waitMessageQueueRecords = hashMapOf<String /*worker*/, MutableMap<String /*thread*/,
                MutableList<LongRange>>>()

        records.asSequence()
                .filterIsInstance<EventRecord>()
                .filter { it.type == EventRecordType.SINGLE }
                .forEach { record ->
                    when (record.tags["type"]) {
                        "garbageCollect" -> {
                            val worker = record.tags["worker"]
                                    ?: throw IllegalArgumentException("Record \"$record\" is missing worker tag")
                            val startTime = record.tags["start"]?.toLongOrNull()
                                    ?: throw IllegalArgumentException("Record \"$record\" is missing start tag")
                            val startTimeNs: TimestampNs = startTime * 1_000_000
                            garbageCollectRecords.getOrPut(worker, { arrayListOf() })
                                    .add(startTimeNs + 1..record.timestamp)
                        }
                        "waitMessageQueue" -> {
                            val worker = record.tags["worker"]
                                    ?: throw IllegalArgumentException("Record \"$record\" is missing worker tag")
                            val thread = record.tags["thread"]
                                    ?: throw IllegalArgumentException("Record \"$record\" is missing thread tag")
                            val startTime = record.tags["start"]?.toLongOrNull()
                                    ?: throw IllegalArgumentException("Record \"$record\" is missing start tag")
                            val startTimeNs: TimestampNs = startTime * 1_000_000
                            waitMessageQueueRecords.getOrPut(worker, { hashMapOf() })
                                    .getOrPut(thread, { arrayListOf() })
                                    .add(startTimeNs + 1..record.timestamp)
                        }
                    }
                }

        val garbageCollectMetrics = garbageCollectRecords.mapValues { (_, gcRanges) ->
            TimePeriodList(gcRanges)
        }
        val waitMessageQueueMetrics = waitMessageQueueRecords.mapValues { (_, threads) ->
            threads.mapValues { (_, waitMessageQueueRanges) ->
                TimePeriodList(waitMessageQueueRanges)
            }
        }

        return buildResourceModel(specification) {
            newSubresource(ROOT_NAME) {
                (garbageCollectMetrics.keys + waitMessageQueueMetrics.keys).forEach { workerId ->
                    newSubresource(WORKER_NAME, workerId) {
                        garbageCollectMetrics[workerId]?.let { gcMetric ->
                            newSubresource(JVM_NAME) {
                                newBlockingMetric(GC_NAME, gcMetric)
                            }
                        }

                        waitMessageQueueMetrics[workerId]?.forEach { threadId, metric ->
                            newSubresource(COMPUTE_THREAD_NAME, threadId) {
                                newBlockingMetric(MSG_QUEUE_NAME, metric)
                            }
                        }
                    }
                }
            }
        }
    }

}
