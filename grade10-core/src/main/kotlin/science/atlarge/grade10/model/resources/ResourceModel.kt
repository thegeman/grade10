package science.atlarge.grade10.model.resources

import science.atlarge.grade10.metrics.RateObservations
import science.atlarge.grade10.metrics.TimePeriodList
import science.atlarge.grade10.metrics.TimeSlicePeriodList
import science.atlarge.grade10.model.HierarchyComponent
import science.atlarge.grade10.model.Path
import science.atlarge.grade10.model.PathComponent

class ResourceModel(
        val specification: ResourceModelSpecification,
        val rootResource: Resource
) {

    init {
        require(specification.rootResourceType === rootResource.type) {
            "The type of the execution model's root must be the execution model specification's root"
        }
    }

    fun resolveResource(path: Path): Resource? = rootResource.resolveResource(path)
    fun resolveMetric(path: Path): Metric? = rootResource.resolveMetric(path)

}

sealed class ResourceModelComponent(
        override val parent: Resource?,
        name: String
) : HierarchyComponent<ResourceModelComponent>(parent, name) {

    abstract override val root: Resource

    fun resolveResource(path: Path): Resource? = resolve(path) as? Resource
    fun resolveMetric(path: Path): Metric? = resolve(path) as? Metric

}

class Resource(
        val type: ResourceType,
        parent: Resource?,
        val instanceId: String = ""
) : ResourceModelComponent(parent, deriveName(type, instanceId)) {

    /**
     * @return a non-empty name uniquely identifying this resource among all resources and metrics with the same
     *         [parent], or the empty string for the root of the hierarchy.
     */
    val name: String
        get() = componentId
    /**
     * @return a shortened [name] of this resource without the name of the instance key.
     */
    val shortName: String = deriveName(type, instanceId, includeKey = false)

    private val _metrics = mutableMapOf<String, Metric>()
    /**
     * @return all metrics of this resource, keyed by their [name].
     */
    val metrics: Map<String, Metric>
        get() = _metrics

    private val _subresources = mutableMapOf<String, Resource>()
    /**
     * @return all subresources of this resource, keyed by their [name].
     */
    val subresources: Map<String, Resource>
        get() = _subresources

    private val _subresourcesByShortName = mutableMapOf<String, Resource>()
    /**
     * @return all subresources of this resource, keyed by their [shortName].
     */
    val subresourcesByShortName: Map<String, Resource>
        get() = _subresourcesByShortName

    override val root: Resource
        get() {
            var pointer = this
            while (true) {
                pointer = pointer.parent ?: return pointer
            }
        }

    init {
        if (parent == null) {
            require(type.isRoot) {
                "The type of a resource model's root must be the root of a resource model specification."
            }
        } else {
            require(type.parent === parent.type) {
                "The type of this resource's parent and the parent of this resource's type must match."
            }
            require(!type.repeatability.isRepeatable || isValidInstanceId(instanceId)) {
                "Instance ID must be valid for an instance of a repeatable resource type"
            }
        }

        parent?.addSubresource(this)
    }

    override fun lookupChild(childId: PathComponent): ResourceModelComponent? {
        return if ('[' in childId && '=' !in childId) {
            _subresourcesByShortName[childId]
        } else {
            _metrics[childId] ?: _subresources[childId]
        }
    }

    internal fun addMetric(metric: Metric) {
        checkNameFree(metric.name)
        _metrics[metric.name] = metric
    }

    private fun addSubresource(subdomain: Resource) {
        checkNameFree(subdomain.name)
        checkShortNameFree(subdomain.shortName)
        _subresources[subdomain.name] = subdomain
        _subresourcesByShortName[subdomain.shortName] = subdomain
    }

    private fun checkNameFree(name: String) {
        require(name !in _metrics) { "A metric with the name \"$name\" already exists" }
        require(name !in _subresources) { "A subresource with the name \"$name\" already exists" }
    }

    private fun checkShortNameFree(name: String) {
        require(name !in _metrics) { "A metric with the name \"$name\" already exists" }
        require(name !in _subresourcesByShortName) { "A subresource with the short name \"$name\" already exists" }
    }

    companion object {

        /**
         * Checks if the given string is a valid instance ID, i.e., it contains no whitespace, square brackets,
         * equality signs, or forward slashes.
         *
         * @return true iff the input is a valid instance ID.
         */
        fun isValidInstanceId(instanceId: String): Boolean {
            return instanceId.all { c ->
                !c.isWhitespace() && c != '=' && c != '[' && c != ']' && c != '/'
            }
        }

        private fun deriveName(type: ResourceType, instanceId: String, includeKey: Boolean = true): String {
            return if (type.repeatability.isRepeatable) {
                "${type.name}[${if (includeKey) "${type.repeatability.instanceKey}=" else ""}$instanceId]"
            } else {
                type.name
            }
        }

    }

}

sealed class Metric(
        val type: MetricType,
        final override val parent: Resource
) : ResourceModelComponent(parent, type.name) {

    /**
     * @return a non-empty name uniquely identifying this metric among all resources and metrics with the same
     *         [parent], or the empty string for the root of the hierarchy.
     */
    val name: String
        get() = componentId

    override val root: Resource
        get() = parent.root

    init {
        parent.addMetric(this)
    }

    override fun lookupChild(childId: PathComponent): ResourceModelComponent? {
        return null
    }

    class Consumable(
            type: MetricType,
            parent: Resource,
            val observedUsage: RateObservations,
            val capacity: Double
    ) : Metric(type, parent) {

        init {
            require(type.metricClass == MetricClass.CONSUMABLE) {
                "A metric must be of the same class as its type"
            }
        }

    }

    class Blocking(type: MetricType, parent: Resource, val blockedTimePeriods: TimePeriodList) : Metric(type, parent) {

        val blockedTimeSlices: TimeSlicePeriodList = TimeSlicePeriodList.fromTimePeriodList(blockedTimePeriods)

        init {
            require(type.metricClass == MetricClass.BLOCKING) {
                "A metric must be of the same class as its type"
            }
        }

    }

}
