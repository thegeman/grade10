package science.atlarge.grade10.serialization

import com.esotericsoftware.kryo.io.Input
import science.atlarge.grade10.model.Path
import science.atlarge.grade10.model.execution.ExecutionModel
import science.atlarge.grade10.model.execution.ExecutionModelSpecification
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.execution.PhaseType
import science.atlarge.grade10.model.resources.*
import science.atlarge.grade10.serialization.Grade10Serializer.Companion.SERIALIZATION_VERSION
import java.io.InputStream

class Grade10Deserializer private constructor(inputStream: InputStream) : Input(inputStream) {

    private lateinit var phaseTypes: Array<PhaseType>
    private lateinit var phases: Array<Phase>
    private lateinit var resourceTypes: Array<ResourceType>
    private lateinit var resources: Array<Resource>
    private lateinit var metricTypes: Array<MetricType>
    private lateinit var metrics: Array<Metric>

    init {
        require(readVarInt(true) == SERIALIZATION_VERSION)
    }

    fun readPhaseType(): PhaseType? {
        val phaseTypeIndex = readVarInt(true)
        return if (phaseTypeIndex in 1..phaseTypes.size) {
            phaseTypes[phaseTypeIndex - 1]
        } else {
            null
        }
    }

    fun readPhase(): Phase? {
        val phaseIndex = readVarInt(true)
        return if (phaseIndex in 1..phases.size) {
            phases[phaseIndex - 1]
        } else {
            null
        }
    }

    fun readResourceType(): ResourceType? {
        val resourceTypeIndex = readVarInt(true)
        return if (resourceTypeIndex in 1..resourceTypes.size) {
            resourceTypes[resourceTypeIndex - 1]
        } else {
            null
        }
    }

    fun readResource(): Resource? {
        val resourceIndex = readVarInt(true)
        return if (resourceIndex in 1..resources.size) {
            resources[resourceIndex - 1]
        } else {
            null
        }
    }

    fun readMetricType(): MetricType? {
        val metricTypeIndex = readVarInt(true)
        return if (metricTypeIndex in 1..metricTypes.size) {
            metricTypes[metricTypeIndex - 1]
        } else {
            null
        }
    }

    fun readMetric(): Metric? {
        val metricIndex = readVarInt(true)
        return if (metricIndex in 1..metrics.size) {
            metrics[metricIndex - 1]
        } else {
            null
        }
    }

    private fun readExecutionModelSpecification(executionModelSpecification: ExecutionModelSpecification) {
        val phaseTypes = arrayListOf<PhaseType>()

        // 1. Number of phase types
        val numPhaseTypes = readVarInt(true)
        // 2. Per phase type:
        repeat(numPhaseTypes) { i ->
            // 2.1. 1-based index of parent (0 for no parent)
            val parentIndex = readVarInt(true)
            // 2.2. Name of phase type
            val name = readString()

            // Lookup phase type
            require(parentIndex in 0..i) { "Invalid parent index: $parentIndex" }
            require(name != "..") { "Invalid name: .." }
            if (parentIndex == 0) {
                phaseTypes.add(executionModelSpecification.rootPhaseType)
            } else {
                val parent = phaseTypes[parentIndex - 1]
                val phaseType = parent.resolve(Path.relative(name))
                require(phaseType != null) {
                    "No phase type found for parent \"${parent.path}\" and name \"$name\""
                }
                phaseTypes.add(phaseType!!)
            }
        }

        this.phaseTypes = phaseTypes.toTypedArray()
    }

    private fun readExecutionModel(executionModel: ExecutionModel) {
        val phases = arrayListOf<Phase>()

        // 1. Number of phases
        val numPhases = readVarInt(true)
        // 2. Per phase:
        repeat(numPhases) { i ->
            // 2.1. 1-based index of parent (0 for no parent)
            val parentIndex = readVarInt(true)
            // 2.2. Short name of phase
            val name = readString()

            // Lookup phase
            require(parentIndex in 0..i) { "Invalid parent index: $parentIndex" }
            require(name != "..") { "Invalid name: .." }
            if (parentIndex == 0) {
                phases.add(executionModel.rootPhase)
            } else {
                val parent = phases[parentIndex - 1]
                val phase = parent.resolve(Path.relative(name))
                require(phase != null) { "No phase found for parent \"${parent.path}\" and name \"$name\"" }
                phases.add(phase!!)
            }
        }

        this.phases = phases.toTypedArray()
    }

    private fun readResourceModelSpecification(resourceModelSpecification: ResourceModelSpecification) {
        val resourceTypes = arrayListOf<ResourceType>()

        // 1. Number of resource types
        val numResourceTypes = readVarInt(true)
        // 2. Per resource type:
        repeat(numResourceTypes) { i ->
            // 2.1. 1-based index of parent (0 for no parent)
            val parentIndex = readVarInt(true)
            // 2.2. Name of resource type
            val name = readString()

            // Lookup resource type
            require(parentIndex in 0..i) { "Invalid parent index: $parentIndex" }
            require(name != "..") { "Invalid name: .." }
            if (parentIndex == 0) {
                resourceTypes.add(resourceModelSpecification.rootResourceType)
            } else {
                val parent = resourceTypes[parentIndex - 1]
                val resourceType = parent.resolveResourceType(Path.relative(name))
                require(resourceType != null) {
                    "No resource type found for parent \"${parent.path}\" and name \"$name\""
                }
                resourceTypes.add(resourceType!!)
            }
        }

        val metricTypes = arrayListOf<MetricType>()

        // 3. Number of metric types
        val numMetricTypes = readVarInt(true)
        // 4. Per metric type:
        repeat(numMetricTypes) {
            // 4.1. 1-based index of parent resource type
            val parentIndex = readVarInt(true)
            // 4.2. Name of metric type
            val name = readString()

            // Lookup metric type
            require(parentIndex in 1..numResourceTypes) { "Invalid parent index: $parentIndex" }
            require(name != "..") { "Invalid name: .." }
            val parent = resourceTypes[parentIndex - 1]
            val metricType = parent.resolveMetricType(Path.relative(name))
            require(metricType != null) {
                "No metric type found for parent \"${parent.path}\" and name \"$name\""
            }
            metricTypes.add(metricType!!)
        }

        this.resourceTypes = resourceTypes.toTypedArray()
        this.metricTypes = metricTypes.toTypedArray()
    }

    private fun readResourceModel(resourceModel: ResourceModel) {
        val resources = arrayListOf<Resource>()

        // 1. Number of resources
        val numResources = readVarInt(true)
        // 2. Per resource:
        repeat(numResources) { i ->
            // 2.1. 1-based index of parent (0 for no parent)
            val parentIndex = readVarInt(true)
            // 2.2. Short name of resource
            val name = readString()

            // Lookup resource
            require(parentIndex in 0..i) { "Invalid parent index: $parentIndex" }
            require(name != "..") { "Invalid name: .." }
            if (parentIndex == 0) {
                resources.add(resourceModel.rootResource)
            } else {
                val parent = resources[parentIndex - 1]
                val resource = parent.resolveResource(Path.relative(name))
                require(resource != null) {
                    "No resource found for parent \"${parent.path}\" and name \"$name\""
                }
                resources.add(resource!!)
            }
        }

        val metrics = arrayListOf<Metric>()

        // 3. Number of metrics
        val numMetrics = readVarInt(true)
        // 4. Per metric:
        repeat(numMetrics) {
            // 4.1. 1-based index of parent resource
            val parentIndex = readVarInt(true)
            // 4.2. Name of metric
            val name = readString()

            // Lookup metric
            require(parentIndex in 1..numResources) { "Invalid parent index: $parentIndex" }
            require(name != "..") { "Invalid name: .." }
            val parent = resources[parentIndex - 1]
            val metric = parent.resolveMetric(Path.relative(name))
            require(metric != null) {
                "No metric found for parent \"${parent.path}\" and name \"$name\""
            }
            metrics.add(metric!!)
        }

        this.resources = resources.toTypedArray()
        this.metrics = metrics.toTypedArray()
    }

    companion object {

        fun from(
                inputStream: InputStream,
                executionModel: ExecutionModel,
                resourceModel: ResourceModel
        ): Grade10Deserializer {
            return Grade10Deserializer(inputStream).apply {
                readExecutionModelSpecification(executionModel.specification)
                readExecutionModel(executionModel)
                readResourceModelSpecification(resourceModel.specification)
                readResourceModel(resourceModel)
            }
        }

    }

}
