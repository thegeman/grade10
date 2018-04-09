package science.atlarge.grade10.examples.powergraph

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

class PowergraphJob(
        override val inputDirectories: List<JPath>,
        override val outputDirectory: JPath
) : Grade10PlatformJob {

    override val platform: Grade10Platform
        get() = PowergraphPlatform

    private val records = RecordStore()
    private val workerToHostnameMap = mutableMapOf<String, String>()

    override fun parseInput(): Pair<ExecutionModel, ResourceModel> {
        val (executionModel, powergraphResourceModel) = parsePowergraphLogs()
        val perfMonitorResourceModel = parseResourceMonitorOutput()
        val resourceModel = mergeResourceModels(listOf(powergraphResourceModel, perfMonitorResourceModel))
        return executionModel to resourceModel
    }

    private fun parsePowergraphLogs(): Pair<ExecutionModel, ResourceModel> {
        val powergraphLogsDir = inputDirectories.find { it.resolve("powergraph-logs").toFile().exists() }!!
                .resolve("powergraph-logs")
        // Parse Powergraph logs for records
        val inputData = powergraphLogsDir
                .toFile()
                .walk()
                .filter(File::isFile)
                .map(File::inputStream)
                .toList()
        val logParser = PowergraphLogFileParser()
        inputData.forEach { inputFile -> logParser.extractAll(inputFile) { records.add(it) } }

        // TODO: Remove filtering and renaming of records by updating Powergraph patch
        records.addAll(records.mapNotNull {
            if (it is EventRecord) {
                val phase = it.tags["phase"]
                if (phase != null && phase.contains("Intialize")) {
                    EventRecord(it.timestamp, it.type, it.tags + ("phase" to phase.replace("Intialize", "Initialize")))
                } else null
            } else null
        })
        records.removeIf {
            if (it is EventRecord) {
                val phase = it.tags["phase"]
                phase?.contains("Intialize") ?: false
            } else false
        }
        records.addAll(records.mapNotNull { rec ->
            if (rec is EventRecord) {
                val phase = rec.tags["phase"]
                if (phase != null) {
                    listOf("ExchangeMessages", "ReceiveMessages", "Gather", "Apply", "Scatter")
                            .find { phase == "Worker$it" }
                            ?.let { EventRecord(rec.timestamp, rec.type, rec.tags + ("phase" to "${it}Communication")) }
                } else null
            } else null
        })
        val orphanSuperstepRecords = records.filterIsInstance<EventRecord>()
                .filter {
                    val phase = it.tags["phase"]
                    phase == "Superstep" && it.type == EventRecordType.START
                }
                .filterNot { startRecord ->
                    val superstep = startRecord.tags["superstep"]!!
                    records.filterIsInstance<EventRecord>()
                            .any { endRecord ->
                                val phase = endRecord.tags["phase"]
                                val endSuperstep = endRecord.tags["superstep"]
                                endRecord.type == EventRecordType.END && phase == "Superstep" && endSuperstep == superstep
                            }
                }
        records.addAll(orphanSuperstepRecords.map { superstepRecord ->
            val targetSuperstep = superstepRecord.tags["superstep"]!!
            val countActiveVerticesEndRecord = records.filterIsInstance<EventRecord>()
                    .find {
                        val phase = it.tags["phase"]
                        val superstep = it.tags["superstep"]
                        it.type == EventRecordType.END && phase == "CountActiveVertices" && targetSuperstep == superstep
                    }!!
            EventRecord(countActiveVerticesEndRecord.timestamp, countActiveVerticesEndRecord.type,
                    countActiveVerticesEndRecord.tags + ("phase" to "Superstep"))
        })

        // Extract PhaseRecords from pairs of EventRecords
        PhaseRecordDerivationRule(PowergraphExecutionModel.specification).modify(records)

        // Derive the start and end time of phases from their children
        ParentPhaseRecordDerivationRule(PowergraphExecutionModel.specification.rootPhaseType).modify(records)

        // Build the execution model
        val executionModel = jobExecutionModelFromRecords(records, PowergraphExecutionModel.specification)

        // Build the Powergraph resource model
        val resourceModel = PowergraphResourceModel.fromLogDirectories(inputDirectories)

        // Extract worker-to-hostname mapping from Powergraph log records
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
        records.asSequence()
                .filterIsInstance<EventRecord>()
                .filter { it.type == EventRecordType.SINGLE }
                .filter { it.tags["type"] == "map-hostname-to-worker" }
                .forEach { record ->
                    workerToHostnameMap[record.tags["worker"]!!] = record.tags["hostname"]!!
                }
    }

    override fun resourceAttributionSettings(
            executionModel: ExecutionModel,
            resourceModel: ResourceModel
    ): ResourceAttributionSettings {
        return ResourceAttributionSettings(
                createPhaseToResourceMapping(executionModel, resourceModel),
                PowergraphResourceAttributionRuleProvider,
                PhaseAwareResourceSamplingStep,
                ResourceAttributionCacheSetting.USE_OR_REFRESH_CACHE
        )
    }

    private fun createPhaseToResourceMapping(
            executionModel: ExecutionModel,
            resourceModel: ResourceModel
    ): PhaseToResourceMapping {
        val mappingEntries = arrayListOf<PhaseToResourceMappingEntry>()

        val perfMonitorResource = resourceModel.resolveResource(Path.absolute("perf-monitor"))!!

        executionModel.rootPhase.subphasesForType("ComputeMain")
                .flatMap { it.subphasesForType("Superstep") }
                .forEach { superstep ->
                    superstep.subphasesForType("CountActiveVertices")
                            .forEach { countActiveVertices ->
                                mappingEntries.add(PhaseToResourceMappingEntry(
                                        countActiveVertices,
                                        resources = listOf(perfMonitorResource)
                                ))
                            }

                    superstep.subphases.values
                            .flatMap { it.subphases.values }
                            .filter { "Worker" in it.type.name }
                            .forEach { workerPhase ->
                                val machineResource = resourceModel.resolveResource(Path.absolute(
                                        "perf-monitor", "machine[${workerToHostnameMap[workerPhase.instanceId]}]"))
                                val workerResource = resourceModel.resolveResource(Path.absolute(
                                        "powergraph", "worker[${workerPhase.instanceId}]"))

                                mappingEntries.add(PhaseToResourceMappingEntry(
                                        workerPhase,
                                        resources = listOfNotNull(machineResource, workerResource)
                                ))

                                workerPhase.subphases.values
                                        .filter { "Thread" in it.type.name }
                                        .forEach { threadPhase ->
                                            val threadResource = workerResource?.resolveResource(Path.relative(
                                                    "thread[${threadPhase.instanceId}]"))

                                            mappingEntries.add(PhaseToResourceMappingEntry(
                                                    threadPhase,
                                                    resources = listOfNotNull(machineResource, threadResource)
                                            ))
                                        }
                            }
                }

        executionModel.rootPhase.subphases.values
                .flatMap { it.subphases.values }
                .filter { "Worker" in it.type.name }
                .forEach { workerPhase ->
                    val machineResource = resourceModel.resolveResource(Path.absolute(
                            "perf-monitor", "machine[${workerToHostnameMap[workerPhase.instanceId]}]"))
                    val workerResource = resourceModel.resolveResource(Path.absolute(
                            "powergraph", "worker[${workerPhase.instanceId}]"))

                    mappingEntries.add(PhaseToResourceMappingEntry(
                            workerPhase,
                            resources = listOfNotNull(machineResource, workerResource)
                    ))
                }
        return PhaseToResourceMapping(executionModel, resourceModel, mappingEntries)
    }

}

private object PowergraphResourceAttributionRuleProvider : ResourceAttributionRuleProvider() {

    private val GATHER_THREAD_PATH = Path.absolute("ComputeMain", "Superstep", "Gather", "WorkerGather",
            "GatherThread")
    private val READ_GATHER_MSG_PATH = Path.absolute("powergraph", "worker", "thread",
            "read-gather-messages")
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
//		return when (metricType.path) {
//			READ_GATHER_MSG_PATH -> {
//				if (phaseType.path == GATHER_THREAD_PATH) {
//					BlockingResourceAttributionRule.Full
//				} else {
//					BlockingResourceAttributionRule.None
//				}
//			}
//			else -> BlockingResourceAttributionRule.None
//		}
        return BlockingResourceAttributionRule.None
    }

    override fun getRuleForConsumableResource(
            phaseType: PhaseType,
            metricType: MetricType
    ): ConsumableResourceAttributionRule {
        return when (metricType.path) {
            CPU_UTILIZATION_PATH -> {
                if ("Thread" in phaseType.name) {
                    ConsumableResourceAttributionRule.Greedy(1.0)
                } else {
                    ConsumableResourceAttributionRule.Sink
                }
            }
            in listOf(NET_BYTES_RECEIVED_PATH, NET_BYTES_TRANSMITTED_PATH) -> {
                if ("Thread" in phaseType.name) {
                    ConsumableResourceAttributionRule.None
                } else {
                    ConsumableResourceAttributionRule.Sink
                }
            }
            else -> ConsumableResourceAttributionRule.None
        }
    }

}
