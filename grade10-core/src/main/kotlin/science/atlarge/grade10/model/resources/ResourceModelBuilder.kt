package science.atlarge.grade10.model.resources

import science.atlarge.grade10.metrics.RateObservations
import science.atlarge.grade10.metrics.TimePeriodList

/**
 * TODO: Document
 */
@DslMarker
annotation class ResourceModelDsl

/**
 * TODO: Document
 */
@ResourceModelDsl
interface ResourceBuilder {

    val type: ResourceType

    val instanceId: String

    fun newBlockingMetric(
            name: String,
            blockedTimePeriods: TimePeriodList
    ): BlockingMetricBuilder

    fun newConsumableMetric(
            name: String,
            observedUsage: RateObservations,
            capacity: Double
    ): ConsumableMetricBuilder

    fun newSubresource(
            name: String,
            instanceId: String = "",
            init: ResourceBuilder.() -> Unit = {}
    ): ResourceBuilder

    fun blockingMetric(name: String): BlockingMetricBuilder

    fun consumableMetric(name: String): ConsumableMetricBuilder

    fun subresource(name: String, instanceId: String = ""): ResourceBuilder

}

/**
 * TODO: Document
 */
@ResourceModelDsl
interface BlockingMetricBuilder {

    val type: MetricType

    val blockedTimePeriods: TimePeriodList

}

/**
 * TODO: Document
 */
@ResourceModelDsl
interface ConsumableMetricBuilder {

    val type: MetricType

    val observedUsage: RateObservations

    val capacity: Double

}

/**
 * TODO: Document
 */
class ResourceBuilderImpl(
        override val type: ResourceType,
        override val instanceId: String
) : ResourceBuilder {

    private val singleSubresourceBuilders = mutableMapOf<ResourceType, ResourceBuilderImpl>()
    private val repeatableSubresourceBuilders = mutableMapOf<ResourceType, MutableMap<String, ResourceBuilderImpl>>()
    private val blockingMetricBuilders = mutableMapOf<MetricType, BlockingMetricBuilderImpl>()
    private val consumableMetricBuilders = mutableMapOf<MetricType, ConsumableMetricBuilderImpl>()

    override fun newBlockingMetric(name: String, blockedTimePeriods: TimePeriodList): BlockingMetricBuilder {
        val type = lookupBlockingMetricType(name)
        require(type !in blockingMetricBuilders) { "Metric with type \"${type.path}\" already exists" }
        return BlockingMetricBuilderImpl(type, blockedTimePeriods)
                .also { blockingMetricBuilders[type] = it }
    }

    override fun newConsumableMetric(
            name: String,
            observedUsage: RateObservations,
            capacity: Double
    ): ConsumableMetricBuilder {
        val type = lookupConsumableMetricType(name)
        require(type !in consumableMetricBuilders) { "Metric with type \"${type.path}\" already exists" }
        return ConsumableMetricBuilderImpl(type, observedUsage, capacity)
                .also { consumableMetricBuilders[type] = it }
    }

    override fun newSubresource(
            name: String,
            instanceId: String,
            init: ResourceBuilder.() -> Unit
    ): ResourceBuilder {
        val type = lookupResourceType(name)
        return if (type.repeatability.isRepeatable) {
            val instances = repeatableSubresourceBuilders.getOrPut(type) { mutableMapOf() }
            require(instanceId !in instances) {
                "Resource with type \"${type.path}\" and instance ID \"$instanceId\" already exists"
            }
            ResourceBuilderImpl(type, instanceId)
                    .also { instances[instanceId] = it }
                    .also(init)
        } else {
            require(type !in singleSubresourceBuilders) { "Resource with type \"${type.path}\" already exists" }
            ResourceBuilderImpl(type, "")
                    .also { singleSubresourceBuilders[type] = it }
                    .also(init)
        }
    }

    override fun blockingMetric(name: String): BlockingMetricBuilder {
        val type = lookupBlockingMetricType(name)
        return blockingMetricBuilders[type]
                ?: throw IllegalArgumentException("No blocking metric for type \"${type.path}\" exists")
    }

    override fun consumableMetric(name: String): ConsumableMetricBuilder {
        val type = lookupConsumableMetricType(name)
        return consumableMetricBuilders[type]
                ?: throw IllegalArgumentException("No consumable metric for type \"${type.path}\" exists")
    }

    override fun subresource(name: String, instanceId: String): ResourceBuilder {
        val type = lookupResourceType(name)
        return if (type.repeatability.isRepeatable) {
            repeatableSubresourceBuilders[type]?.get(instanceId) ?: throw IllegalArgumentException(
                    "No subresource for type \"${type.path}\" and instance ID \"$instanceId\" exists")
        } else {
            singleSubresourceBuilders[type]
                    ?: throw IllegalArgumentException("No subresource for type \"${type.path}\" exists")
        }
    }

    fun build(): Resource {
        require(type.isRoot) { "Resource hierarchy must be built from the root" }
        return build(null)
    }

    private fun build(parent: Resource?): Resource {
        val resource = Resource(type, parent, instanceId)

        blockingMetricBuilders.forEach { _, builder -> builder.build(resource) }
        consumableMetricBuilders.forEach { _, builder -> builder.build(resource) }
        singleSubresourceBuilders.forEach { _, builder -> builder.build(resource) }
        repeatableSubresourceBuilders.forEach { _, builders ->
            builders.forEach { _, builder -> builder.build(resource) }
        }

        return resource
    }

    private fun lookupResourceType(name: String): ResourceType {
        return type.subresourceTypes[name]
                ?: throw IllegalArgumentException("No resource type with the name \"$name\" exists")
    }

    private fun lookupConsumableMetricType(name: String): MetricType {
        val metricType = type.metricTypes[name]
                ?: throw IllegalArgumentException("No metric type with the name \"$name\" exists")
        require(metricType.metricClass == MetricClass.CONSUMABLE) {
            "Metric type \"${metricType.path} is not consumable"
        }
        return metricType
    }

    private fun lookupBlockingMetricType(name: String): MetricType {
        val metricType = type.metricTypes[name]
                ?: throw IllegalArgumentException("No metric type with the name \"$name\" exists")
        require(metricType.metricClass == MetricClass.BLOCKING) {
            "Metric type \"${metricType.path} is not blocking"
        }
        return metricType
    }

}

/**
 * TODO: Document
 */
class BlockingMetricBuilderImpl(
        override val type: MetricType,
        override val blockedTimePeriods: TimePeriodList
) : BlockingMetricBuilder {

    fun build(parent: Resource): Metric.Blocking {
        return Metric.Blocking(type, parent, blockedTimePeriods)
    }

}

/**
 * TODO: Document
 */
class ConsumableMetricBuilderImpl(
        override val type: MetricType,
        override val observedUsage: RateObservations,
        override val capacity: Double
) : ConsumableMetricBuilder {

    fun build(parent: Resource): Metric.Consumable {
        return Metric.Consumable(type, parent, observedUsage, capacity)
    }

}

/**
 * TODO: Document
 */
fun buildResourceModel(
        resourceModelSpecification: ResourceModelSpecification,
        init: ResourceBuilder.() -> Unit = {}
): ResourceModel {
    val rootResource =
            ResourceBuilderImpl(resourceModelSpecification.rootResourceType, "")
                    .also(init)
                    .build()
    return ResourceModel(resourceModelSpecification, rootResource)
}

/**
 * TODO: Document
 */
fun mergeResourceModels(resourceModels: List<ResourceModel>): ResourceModel {
    fun ResourceBuilder.copy(resources: List<Resource>) {
        resources.flatMap { it.subresources.toList() }
                .groupBy({ it.first }, { it.second })
                .forEach { _, subresources ->
                    val name = subresources[0].type.name
                    val instanceId = subresources[0].instanceId
                    newSubresource(name, instanceId) {
                        copy(subresources)
                    }
                }
        resources.flatMap { it.metrics.values }
                .forEach { metric ->
                    when (metric) {
                        is Metric.Blocking -> newBlockingMetric(metric.name, metric.blockedTimePeriods)
                        is Metric.Consumable -> newConsumableMetric(metric.name, metric.observedUsage, metric.capacity)
                    }
                }
    }

    val mergedModelSpecification = mergeResourceModelSpecifications(resourceModels.map { it.specification })
    return buildResourceModel(mergedModelSpecification) {
        copy(resourceModels.map { it.rootResource })
    }
}
