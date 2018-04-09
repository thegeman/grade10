package science.atlarge.grade10.model.resources

import science.atlarge.grade10.model.HierarchyComponent
import science.atlarge.grade10.model.Path
import science.atlarge.grade10.model.PathComponent

class ResourceModelSpecification(
        val rootResourceType: ResourceType
)

sealed class ResourceModelSpecificationComponent(
        override val parent: ResourceType?,
        name: PathComponent,
        val description: String
) : HierarchyComponent<ResourceModelSpecificationComponent>(parent, name) {

    abstract override val root: ResourceType

    fun resolveResourceType(path: Path): ResourceType? = resolve(path) as? ResourceType
    fun resolveMetricType(path: Path): MetricType? = resolve(path) as? MetricType

}

class ResourceType(
        parent: ResourceType?,
        name: String,
        description: String = "",
        val repeatability: ResourceTypeRepeatability
) : ResourceModelSpecificationComponent(parent, name, description) {

    /**
     * @return a non-empty name uniquely identifying this resource type among all resource and metric types with the
     *         same [parent], or the empty string for the root of the hierarchy.
     */
    val name: String
        get() = componentId

    private val _metricTypes = mutableMapOf<String, MetricType>()
    /**
     * @return all metric types of this type, keyed by their [name].
     */
    val metricTypes: Map<String, MetricType>
        get() = _metricTypes

    private val _subresourceTypes = mutableMapOf<String, ResourceType>()
    /**
     * @return all subresource types of this type, keyed by their [name].
     */
    val subresourceTypes: Map<String, ResourceType>
        get() = _subresourceTypes

    override val root: ResourceType
        get() {
            var pointer = this
            while (true) {
                pointer = pointer.parent ?: return pointer
            }
        }

    init {
        if (parent == null) {
            require(!repeatability.isRepeatable) { "The root of the hierarchy cannot be repeatable" }
        }

        parent?.addSubresourceType(this)
    }

    override fun lookupChild(childId: PathComponent): ResourceModelSpecificationComponent? {
        return _metricTypes[childId] ?: _subresourceTypes[childId]
    }

    internal fun addMetricType(metricType: MetricType) {
        checkNameFree(metricType.name)
        _metricTypes[metricType.name] = metricType
    }

    private fun addSubresourceType(subresourceType: ResourceType) {
        checkNameFree(subresourceType.name)
        _subresourceTypes[subresourceType.name] = subresourceType
    }

    private fun checkNameFree(name: String) {
        require(name !in _metricTypes) { "A metric type with the name \"$name\" already exists" }
        require(name !in _subresourceTypes) { "A subresource type with the name \"$name\" already exists" }
    }

}

class MetricType(
        override val parent: ResourceType,
        name: String,
        description: String = "",
        val metricClass: MetricClass
) : ResourceModelSpecificationComponent(parent, name, description) {

    init {
        parent.addMetricType(this)
    }

    /**
     * @return a non-empty name uniquely identifying this metric type among all resource and metric types with the
     *         same [parent].
     */
    val name: String
        get() = componentId

    override val root: ResourceType
        get() = parent.root

    override fun lookupChild(childId: PathComponent): ResourceModelSpecificationComponent? {
        return null
    }

}

enum class MetricClass {
    CONSUMABLE,
    BLOCKING
}

sealed class ResourceTypeRepeatability(val validOccurrenceCount: IntRange, val instanceKey: String = "") {

    object OptionalOnce : ResourceTypeRepeatability(0..1)

    object RequiredOnce : ResourceTypeRepeatability(1..1)

    class Many(instanceKey: String, validOccurrenceCount: IntRange = 0..Int.MAX_VALUE) :
            ResourceTypeRepeatability(validOccurrenceCount, instanceKey) {

        init {
            require(!validOccurrenceCount.isEmpty()) {
                "There must exist a valid number of occurrences for any resource type"
            }
            require(isRepeatable) { "Repeatability of 'Many' requires an upper bound of at least 2 occurrences" }
            require(instanceKey.isNotBlank()) {
                "Instance key must be set for repeatable resource types"
            }
        }

    }

    val isRepeatable: Boolean
        get() = validOccurrenceCount.endInclusive > 1

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ResourceTypeRepeatability

        if (validOccurrenceCount != other.validOccurrenceCount) return false
        if (instanceKey != other.instanceKey) return false

        return true
    }

    override fun hashCode(): Int {
        var result = validOccurrenceCount.hashCode()
        result = 31 * result + instanceKey.hashCode()
        return result
    }


}
