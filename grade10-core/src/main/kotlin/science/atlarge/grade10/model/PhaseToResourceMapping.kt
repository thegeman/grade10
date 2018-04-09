package science.atlarge.grade10.model

import science.atlarge.grade10.model.execution.ExecutionModel
import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.resources.Metric
import science.atlarge.grade10.model.resources.Resource
import science.atlarge.grade10.model.resources.ResourceModel

class PhaseToResourceMapping(
        val executionModel: ExecutionModel,
        val resourceModel: ResourceModel,
        entries: Iterable<PhaseToResourceMappingEntry>
) {

    private val entries = (entries + rootEntry(executionModel, resourceModel)).associateBy { it.phase }

    init {
        validate()
    }

    operator fun get(phase: Phase): PhaseToResourceMappingEntry {
        return entries[phase] ?: phase.parent?.let(this::get)
        ?: throw IllegalArgumentException("Phase does not belong to execution model mapped by this mapping")
    }

    fun isPhaseMappedToMetric(phase: Phase, metric: Metric): Boolean = metric in this[phase]

    private fun validate() {
        // Sanity check: all phases, resources, and metrics in entries must belong to the same execution/resource model
        entries.forEach { phase, mapping ->
            require(phase.root === executionModel.rootPhase) {
                "All entries must map phases that belong to the same execution model"
            }
            mapping.resources.forEach { resource ->
                require(resource.root === resourceModel.rootResource) {
                    "All entries must map resources that belong to the same resource model"
                }
            }
            mapping.metrics.forEach { metric ->
                require(metric.root === resourceModel.rootResource) {
                    "All entries must map resources that belong to the same resource model"
                }
            }
        }

        // Validity check: subphase mappings must be contained in their parent's mapping
        entries.forEach { phase, mapping ->
            if (!phase.isRoot) {
                require(mapping in this[phase.parent!!]) {
                    "The mapping of phase \"${phase.path}\" is not a subset of its parent's mapping"
                }
            }
        }
    }

    companion object {

        private fun rootEntry(
                executionModel: ExecutionModel,
                resourceModel: ResourceModel
        ): PhaseToResourceMappingEntry {
            return PhaseToResourceMappingEntry(
                    phase = executionModel.rootPhase,
                    resources = listOf(resourceModel.rootResource)
            )
        }

    }

}

data class PhaseToResourceMappingEntry(
        val phase: Phase,
        val resources: List<Resource> = emptyList(),
        val metrics: List<Metric> = emptyList()
) {

    private val mappedResourcePaths = (resources + metrics).map { it.path }

    operator fun contains(resource: Resource): Boolean {
        return resource.path in this
    }

    operator fun contains(metric: Metric): Boolean {
        return metric.path in this
    }

    operator fun contains(other: PhaseToResourceMappingEntry): Boolean {
        return other.mappedResourcePaths.all { it in this }
    }

    private operator fun contains(path: Path): Boolean {
        return mappedResourcePaths.any { path in it }
    }

}
