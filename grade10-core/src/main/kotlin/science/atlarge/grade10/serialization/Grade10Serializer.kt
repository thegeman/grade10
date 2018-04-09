package science.atlarge.grade10.serialization

import com.esotericsoftware.kryo.io.Output
import science.atlarge.grade10.model.execution.ExecutionModel
import science.atlarge.grade10.model.execution.ExecutionModelSpecification
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.execution.PhaseType
import science.atlarge.grade10.model.resources.*
import science.atlarge.grade10.util.depthFirstSearch
import java.io.OutputStream

class Grade10Serializer private constructor(outputStream: OutputStream) : Output(outputStream) {

    private lateinit var phaseTypeToIndex: Map<PhaseType, Int>
    private lateinit var phaseToIndex: Map<Phase, Int>
    private lateinit var resourceTypeToIndex: Map<ResourceType, Int>
    private lateinit var resourceToIndex: Map<Resource, Int>
    private lateinit var metricTypeToIndex: Map<MetricType, Int>
    private lateinit var metricToIndex: Map<Metric, Int>

    init {
        writeVarInt(SERIALIZATION_VERSION, true)
    }

    fun write(phaseType: PhaseType) {
        writeVarInt(phaseTypeToIndex[phaseType]!!, true)
    }

    fun write(phase: Phase) {
        writeVarInt(phaseToIndex[phase]!!, true)
    }

    fun write(resourceType: ResourceType) {
        writeVarInt(resourceTypeToIndex[resourceType]!!, true)
    }

    fun write(resource: Resource) {
        writeVarInt(resourceToIndex[resource]!!, true)
    }

    fun write(metricType: MetricType) {
        writeVarInt(metricTypeToIndex[metricType]!!, true)
    }

    fun write(metric: Metric) {
        writeVarInt(metricToIndex[metric]!!, true)
    }

    private fun writeExecutionModelSpecification(executionModelSpecification: ExecutionModelSpecification) {
        val orderedPhaseTypes = arrayListOf<PhaseType>()
        depthFirstSearch(executionModelSpecification.rootPhaseType, { it.subphaseTypes.values }) { phaseType, _ ->
            orderedPhaseTypes.add(phaseType)
        }
        val phaseTypeToIndex = orderedPhaseTypes.mapIndexed { index, phaseType -> phaseType to index + 1 }.toMap()

        // 1. Number of phase types
        writeVarInt(orderedPhaseTypes.size, true)
        // 2. Per phase type:
        orderedPhaseTypes.forEachIndexed { _, phaseType ->
            // 2.1. 1-based index of parent (0 for no parent)
            if (phaseType.parent == null) {
                writeVarInt(0, true)
            } else {
                writeVarInt(phaseTypeToIndex[phaseType.parent]!!, true)
            }
            // 2.2. Name of phase type
            writeString(phaseType.name)
        }

        this.phaseTypeToIndex = phaseTypeToIndex
    }

    private fun writeExecutionModel(executionModel: ExecutionModel) {
        val orderedPhases = arrayListOf<Phase>()
        depthFirstSearch(executionModel.rootPhase, { it.subphases.values }) { phase, _ ->
            orderedPhases.add(phase)
        }
        val phaseToIndex = orderedPhases.mapIndexed { index, phase -> phase to index + 1 }.toMap()

        // 1. Number of phases
        writeVarInt(orderedPhases.size, true)
        // 2. Per phase:
        orderedPhases.forEachIndexed { _, phase ->
            // 2.1. 1-based index of parent (0 for no parent)
            if (phase.parent == null) {
                writeVarInt(0, true)
            } else {
                writeVarInt(phaseToIndex[phase.parent]!!, true)
            }
            // 2.2. Short name of phase
            writeString(phase.shortName)
        }

        this.phaseToIndex = phaseToIndex
    }

    private fun writeResourceModelSpecification(resourceModelSpecification: ResourceModelSpecification) {
        val orderedResourceTypes = arrayListOf<ResourceType>()
        depthFirstSearch(resourceModelSpecification.rootResourceType, { it.subresourceTypes.values },
                { resourceType, _ ->
                    orderedResourceTypes.add(resourceType)
                }
        )
        val resourceTypeToIndex = orderedResourceTypes
                .mapIndexed { index, resourceType -> resourceType to index + 1 }
                .toMap()

        // 1. Number of resource types
        writeVarInt(orderedResourceTypes.size, true)
        // 2. Per resource type:
        orderedResourceTypes.forEachIndexed { _, resourceType ->
            // 2.1. 1-based index of parent (0 for no parent)
            if (resourceType.parent == null) {
                writeVarInt(0, true)
            } else {
                writeVarInt(resourceTypeToIndex[resourceType.parent]!!, true)
            }
            // 2.2. Name of resource type
            writeString(resourceType.name)
        }

        val orderedMetricTypes = orderedResourceTypes.flatMap { it.metricTypes.values }
        val metricTypeToIndex = orderedMetricTypes.mapIndexed { index, metricType -> metricType to index + 1 }.toMap()

        // 3. Number of metric types
        writeVarInt(orderedMetricTypes.size, true)
        // 4. Per metric type:
        orderedMetricTypes.forEachIndexed { _, metricType ->
            // 4.1. 1-based index of parent resource type
            writeVarInt(resourceTypeToIndex[metricType.parent]!!, true)
            // 4.2. Name of metric type
            writeString(metricType.name)
        }

        this.resourceTypeToIndex = resourceTypeToIndex
        this.metricTypeToIndex = metricTypeToIndex
    }

    private fun writeResourceModel(resourceModel: ResourceModel) {
        val orderedResources = arrayListOf<Resource>()
        depthFirstSearch(resourceModel.rootResource, { it.subresources.values }) { resource, _ ->
            orderedResources.add(resource)
        }
        val resourceToIndex = orderedResources.mapIndexed { index, resource -> resource to index + 1 }.toMap()

        // 1. Number of resources
        writeVarInt(orderedResources.size, true)
        // 2. Per resource:
        orderedResources.forEachIndexed { _, resource ->
            // 2.1. 1-based index of parent (0 for no parent)
            if (resource.parent == null) {
                writeVarInt(0, true)
            } else {
                writeVarInt(resourceToIndex[resource.parent]!!, true)
            }
            // 2.2. Short name of resource
            writeString(resource.shortName)
        }

        val orderedMetrics = orderedResources.flatMap { it.metrics.values }
        val metricToIndex = orderedMetrics.mapIndexed { index, metric -> metric to index + 1 }.toMap()

        // 3. Number of metrics
        writeVarInt(orderedMetrics.size, true)
        // 4. Per metric:
        orderedMetrics.forEachIndexed { _, metricType ->
            // 4.1. 1-based index of parent resource
            writeVarInt(resourceToIndex[metricType.parent]!!, true)
            // 4.2. Name of metric
            writeString(metricType.name)
        }

        this.resourceToIndex = resourceToIndex
        this.metricToIndex = metricToIndex
    }

    companion object {

        const val SERIALIZATION_VERSION = 1

        fun from(
                outputStream: OutputStream,
                executionModel: ExecutionModel,
                resourceModel: ResourceModel
        ): Grade10Serializer {
            return Grade10Serializer(outputStream).apply {
                writeExecutionModelSpecification(executionModel.specification)
                writeExecutionModel(executionModel)
                writeResourceModelSpecification(resourceModel.specification)
                writeResourceModel(resourceModel)
            }
        }

    }

}
