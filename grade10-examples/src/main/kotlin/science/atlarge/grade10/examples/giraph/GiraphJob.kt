package science.atlarge.grade10.examples.giraph

import science.atlarge.grade10.Grade10Platform
import science.atlarge.grade10.Grade10PlatformJob
import science.atlarge.grade10.analysis.attribution.*
import science.atlarge.grade10.model.Path
import science.atlarge.grade10.model.PhaseToResourceMapping
import science.atlarge.grade10.model.PhaseToResourceMappingEntry
import science.atlarge.grade10.model.execution.*
import science.atlarge.grade10.model.resources.MetricType
import science.atlarge.grade10.model.resources.ResourceModel
import science.atlarge.grade10.model.resources.mergeResourceModels
import science.atlarge.grade10.monitor.MonitorOutput
import science.atlarge.grade10.monitor.MonitorOutputParser
import science.atlarge.grade10.records.EventRecord
import science.atlarge.grade10.records.EventRecordType
import science.atlarge.grade10.records.RecordStore
import java.io.File
import java.nio.file.Path as JPath

class GiraphJob(
        override val inputDirectories: List<JPath>,
        override val outputDirectory: JPath
) : Grade10PlatformJob {

    override val platform: Grade10Platform
        get() = GiraphPlatform

    private val records = RecordStore()
    private val workerToHostnameMap = mutableMapOf<String, String>()

    override fun parseInput(): Pair<ExecutionModel, ResourceModel> {
        val (executionModel, giraphResourceModel) = parseGiraphLogs()
        val perfMonitorResourceModel = parseResourceMonitorOutput()
        val resourceModel = mergeResourceModels(listOf(giraphResourceModel, perfMonitorResourceModel))
        return executionModel to resourceModel
    }

    private fun parseGiraphLogs(): Pair<ExecutionModel, ResourceModel> {
        val giraphLogsDir = inputDirectories.find { it.resolve("giraph-logs").toFile().exists() }!!
                .resolve("giraph-logs")
        // Parse Giraph logs for records
        val inputData = giraphLogsDir
                .toFile()
                .walk()
                .filter(File::isFile)
                .map(File::inputStream)
                .toList()
        val logParser = GiraphLogFileParser()
        inputData.forEach { inputFile -> logParser.extractAll(inputFile) { records.add(it) } }

        // TODO: Remove filtering and renaming of records by updating Giraph patch
        records.removeIf {
            if (it is EventRecord) {
                val phase = it.tags["phase"]
                if (phase != null) {
                    phase.contains("VertexCompute") ||
                            phase.contains("ComputePartition") ||
                            phase == "Execute" ||
                            (phase == "Superstep" && it.tags["superstep"] != "-1")
                } else false
            } else false
        }
        records.addAll(records.mapNotNull {
            if (it is EventRecord) {
                val phase = it.tags["phase"]
                if (phase == "Superstep" && it.tags["superstep"] == "-1") {
                    val newTags = mapOf("phase" to "PreApplication", "worker" to "-1")
                    EventRecord(it.timestamp, it.type, newTags)
                } else null
            } else null
        })
        val startOfCleanup = records.asSequence()
                .filterIsInstance<EventRecord>()
                .filter { it.tags["phase"] == "PostApplication" && it.type == EventRecordType.START }
                .map { it.timestamp }
                .min()!!
        val endOfCleanup = records.asSequence()
                .filterIsInstance<EventRecord>()
                .find { it.tags["phase"] == "GiraphJob" && it.type == EventRecordType.END }!!
                .timestamp
        records.add(EventRecord(startOfCleanup, EventRecordType.START, mapOf("phase" to "Cleanup")))
        records.add(EventRecord(endOfCleanup, EventRecordType.END, mapOf("phase" to "Cleanup")))
        records.removeIf {
            if (it is EventRecord) {
                val phase = it.tags["phase"]
                phase == "PostApplication" ||
                        phase == "GiraphJob" ||
                        (phase == "Superstep" && it.tags["superstep"] == "-1")
            } else false
        }
        records.addAll(records.mapNotNull {
            if (it is EventRecord) {
                val phase = it.tags["phase"]
                if (phase == "WorkerSuperstep") {
                    EventRecord(it.timestamp, it.type, it.tags + ("phase" to "Communicate"))
                } else null
            } else null
        })
        PhaseRenameRecordModificationRule("PreApplication", "WorkerLoadGraph").modify(records)
        PhaseRenameRecordModificationRule("Setup", "WorkerSetup").modify(records)

        // Extract PhaseRecords from pairs of EventRecords
        PhaseRecordDerivationRule(GiraphExecutionModel.specification).modify(records)

        // Derive the start and end time of phases from their children
        ParentPhaseRecordDerivationRule(GiraphExecutionModel.specification.resolvePhaseType(
                Path.absolute("Execute", "Superstep"))!!).modify(records)
        ParentPhaseRecordDerivationRule(GiraphExecutionModel.specification.resolvePhaseType(
                Path.absolute("Execute"))!!).modify(records)
        ParentPhaseRecordDerivationRule(GiraphExecutionModel.specification.resolvePhaseType(
                Path.absolute("Setup"))!!).modify(records)
        ParentPhaseRecordDerivationRule(GiraphExecutionModel.specification.resolvePhaseType(
                Path.absolute("LoadGraph"))!!).modify(records)
        ParentPhaseRecordDerivationRule(GiraphExecutionModel.specification.rootPhaseType).modify(records)

        // Build the execution model
        val executionModel = jobExecutionModelFromRecords(records, GiraphExecutionModel.specification)

        // Build the Giraph resource model
        val resourceModel = GiraphResourceModel.fromRecords(records)

        // Extract worker-to-hostname mapping from Giraph log records
        extractWorkerHostnameMapping()

        return Pair(executionModel, resourceModel)
    }

    private fun parseResourceMonitorOutput(): ResourceModel {
        val resourceMonitorDir = inputDirectories.find { it.resolve("resource-monitor").toFile().exists() }!!
                .resolve("resource-monitor")
        val monitorDataCache = resourceMonitorDir.parent.resolve("resource-monitor.bin.gz")
        // Check if the perf monitor output is cached, otherwise parse and cache it
        val cachedMonitorData = if (monitorDataCache.toFile().exists()) {
            MonitorOutput.readFromFile(monitorDataCache.toFile())
        } else {
            null
        }
        val monitorData = cachedMonitorData ?: MonitorOutputParser.parseDirectory(resourceMonitorDir)
        if (cachedMonitorData == null) {
            monitorData.writeToFile(monitorDataCache.toFile())
        }
        return monitorData.toResourceModel(includePerCoreUtilization = false) { interfaceId, _ ->
            // TODO: DO NOT HARDCODE THESE RESOURCE UTILIZATION LIMITS
            if (interfaceId.startsWith("ib")) 1e9
            else 1.25e8
        }
    }

    private fun extractWorkerHostnameMapping() {
        val taskAttemptToWorkerMap = hashMapOf<String, String>()
        val taskAttemptToHostnameMap = hashMapOf<String, String>()
        records.asSequence()
                .filterIsInstance<EventRecord>()
                .filter { it.type == EventRecordType.SINGLE }
                .forEach { record ->
                    when (record.tags["type"]) {
                        "map-taskAttempt-to-worker" -> {
                            taskAttemptToWorkerMap[record.tags["taskAttempt"]!!] = record.tags["worker"]!!
                        }
                        "map-taskAttempt-to-hostname" -> {
                            taskAttemptToHostnameMap[record.tags["taskAttempt"]!!] = record.tags["hostname"]!!
                        }
                    }
                }

        taskAttemptToWorkerMap
                .mapNotNull { (taskAttempt, worker) ->
                    taskAttemptToHostnameMap[taskAttempt]?.let { hostname -> worker to hostname }
                }
                .forEach { (worker, hostname) ->
                    workerToHostnameMap[worker] = hostname
                }
    }

    override fun resourceAttributionSettings(
            executionModel: ExecutionModel,
            resourceModel: ResourceModel
    ): ResourceAttributionSettings {
        return ResourceAttributionSettings(
                createPhaseToResourceMapping(executionModel, resourceModel),
                GiraphResourceAttributionRuleProvider,
                PhaseAwareResourceSamplingStep,
                ResourceAttributionCacheSetting.USE_OR_REFRESH_CACHE
        )
    }

    private fun createPhaseToResourceMapping(
            executionModel: ExecutionModel,
            resourceModel: ResourceModel
    ): PhaseToResourceMapping {
        val mappingEntries = arrayListOf<PhaseToResourceMappingEntry>()

        executionModel.rootPhase.subphasesForType("Setup")
                .flatMap { it.subphasesForType("WorkerSetup") }
                .forEach { workerSetup ->
                    mappingEntries.add(PhaseToResourceMappingEntry(
                            workerSetup,
                            resources = listOfNotNull(
                                    resourceModel.resolveResource(Path.absolute("perf-monitor",
                                            "machine[${workerToHostnameMap[workerSetup.instanceId]}]")),
                                    resourceModel.resolveResource(Path.absolute("giraph",
                                            "worker[${workerSetup.instanceId}]", "jvm"))
                            )
                    ))
                }

        executionModel.rootPhase.subphasesForType("LoadGraph")
                .flatMap { it.subphasesForType("WorkerLoadGraph") }
                .forEach { workerLoadGraph ->
                    mappingEntries.add(PhaseToResourceMappingEntry(
                            workerLoadGraph,
                            resources = listOfNotNull(
                                    resourceModel.resolveResource(Path.absolute("perf-monitor",
                                            "machine[${workerToHostnameMap[workerLoadGraph.instanceId]}]")),
                                    resourceModel.resolveResource(Path.absolute("giraph",
                                            "worker[${workerLoadGraph.instanceId}]", "jvm"))
                            )
                    ))
                }

        executionModel.rootPhase.subphasesForType("Cleanup")
                .firstOrNull()
                ?.let { cleanup ->
                    mappingEntries.add(PhaseToResourceMappingEntry(
                            cleanup,
                            resources = listOfNotNull(
                                    resourceModel.resolveResource(Path.absolute("perf-monitor"))
                            )
                    ))
                }

        executionModel.rootPhase.subphasesForType("Execute")
                .flatMap { it.subphasesForType("Superstep") }
                .flatMap { it.subphasesForType("WorkerSuperstep") }
                .forEach { workerSuperstep ->
                    val machineResource = resourceModel.resolveResource(Path.absolute("perf-monitor",
                            "machine[${workerToHostnameMap[workerSuperstep.instanceId]}]"))
                    val workerResource = resourceModel.resolveResource(Path.absolute("giraph",
                            "worker[${workerSuperstep.instanceId}]"))
                    val jvmResource = workerResource?.resolveResource(Path.relative("jvm"))

                    mappingEntries.add(PhaseToResourceMappingEntry(
                            workerSuperstep,
                            resources = listOfNotNull(machineResource, workerResource)
                    ))

                    workerSuperstep.subphasesForType("Communicate").forEach {
                        mappingEntries.add(PhaseToResourceMappingEntry(it, listOfNotNull(machineResource, jvmResource)))
                    }
                    workerSuperstep.subphasesForType("PreCompute").forEach {
                        mappingEntries.add(PhaseToResourceMappingEntry(it, listOfNotNull(machineResource, jvmResource)))
                    }
                    workerSuperstep.subphasesForType("PostCompute").forEach {
                        mappingEntries.add(PhaseToResourceMappingEntry(it, listOfNotNull(machineResource, jvmResource)))
                    }
                    workerSuperstep.subphasesForType("Compute")
                            .flatMap { it.subphasesForType("ComputeThread") }
                            .forEach { computeThread ->
                                val threadResource = workerResource?.resolveResource(Path.relative(
                                        "compute-thread[${computeThread.instanceId}]"))
                                mappingEntries.add(PhaseToResourceMappingEntry(
                                        computeThread,
                                        listOfNotNull(machineResource, jvmResource, threadResource)
                                ))
                            }
                }

        return PhaseToResourceMapping(executionModel, resourceModel, mappingEntries)
    }

}

private object GiraphResourceAttributionRuleProvider : ResourceAttributionRuleProvider() {

    private val WORKER_SUPERSTEP_PATH = Path.absolute("Execute", "Superstep", "WorkerSuperstep")
    private val COMMUNICATE_PATH = Path.absolute("Execute", "Superstep", "WorkerSuperstep",
            "Communicate")
    private val COMPUTE_THREAD_PATH = Path.absolute("Execute", "Superstep", "WorkerSuperstep",
            "Compute", "ComputeThread")
    private val GC_PATH = Path.absolute("giraph", "worker", "jvm", "garbage-collect")
    private val MSG_QUEUE_PATH = Path.absolute("giraph", "worker", "compute-thread",
            "wait-on-message-queue")
    private val CPU_UTILIZATION_PATH = Path.absolute("perf-monitor", "machine",
            "total-cpu-utilization")
    private val NET_BYTES_RECEIVED_PATH = Path.absolute("perf-monitor", "machine", "network",
            "bytes-received")
    private val NET_BYTES_TRANSMITTED_PATH = Path.absolute("perf-monitor", "machine", "network",
            "bytes-transmitted")

    override fun getRuleForBlockingResource(
            phaseType: PhaseType,
            metricType: MetricType
    ): BlockingResourceAttributionRule {
        return when (metricType.path) {
            GC_PATH -> {
                BlockingResourceAttributionRule.Full
            }
            MSG_QUEUE_PATH -> {
                if (phaseType.path in COMPUTE_THREAD_PATH) {
                    BlockingResourceAttributionRule.Full
                } else {
                    BlockingResourceAttributionRule.None
                }
            }
            else -> BlockingResourceAttributionRule.None
        }
    }

    override fun getRuleForConsumableResource(
            phaseType: PhaseType,
            metricType: MetricType
    ): ConsumableResourceAttributionRule {
        return when (metricType.path) {
            CPU_UTILIZATION_PATH -> {
                if (phaseType.path in COMPUTE_THREAD_PATH) {
                    ConsumableResourceAttributionRule.Greedy(1.0)
                } else {
                    ConsumableResourceAttributionRule.Sink
                }
            }
            in listOf(NET_BYTES_RECEIVED_PATH, NET_BYTES_TRANSMITTED_PATH) -> {
                when {
                    phaseType.path == COMMUNICATE_PATH -> ConsumableResourceAttributionRule.Sink
                    phaseType.path in WORKER_SUPERSTEP_PATH -> ConsumableResourceAttributionRule.None
                    else -> ConsumableResourceAttributionRule.Sink
                }
            }
            else -> ConsumableResourceAttributionRule.None
        }
    }

}
