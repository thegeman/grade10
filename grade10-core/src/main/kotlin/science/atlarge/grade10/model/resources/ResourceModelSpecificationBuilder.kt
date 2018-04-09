package science.atlarge.grade10.model.resources

/**
 * TODO: Document
 */
@DslMarker
annotation class ResourceModelSpecificationDsl

/**
 * TODO: Document
 */
@ResourceModelSpecificationDsl
interface ResourceTypeBuilder {

    val name: String

    val repeatability: ResourceTypeRepeatability

    var description: String

    fun newMetricType(
            name: String,
            type: MetricClass,
            init: MetricTypeBuilder.() -> Unit = {}
    ): MetricTypeBuilder

    fun newSubresourceType(
            name: String,
            repeatability: ResourceTypeRepeatability = ResourceTypeRepeatability.RequiredOnce,
            init: ResourceTypeBuilder.() -> Unit = {}
    ): ResourceTypeBuilder

    fun metricType(name: String): MetricTypeBuilder

    fun subresourceType(name: String): ResourceTypeBuilder

}

/**
 * TODO: Document
 */
@ResourceModelSpecificationDsl
interface MetricTypeBuilder {

    val name: String

    val type: MetricClass

    var description: String

}

/**
 * TODO: Document
 */
class ResourceTypeBuilderImpl(
        private val parentBuilder: ResourceTypeBuilderImpl?,
        override val name: String,
        override val repeatability: ResourceTypeRepeatability
) : ResourceTypeBuilder {

    override var description: String = ""

    private val subresourceBuilders = mutableMapOf<String, ResourceTypeBuilderImpl>()

    private val metricBuilders = mutableMapOf<String, MetricTypeBuilderImpl>()

    override fun newMetricType(
            name: String,
            type: MetricClass,
            init: MetricTypeBuilder.() -> Unit
    ): MetricTypeBuilder {
        checkNameFree(name)
        return MetricTypeBuilderImpl(name, type)
                .also { metricBuilders[name] = it }
                .also(init)
    }

    override fun newSubresourceType(
            name: String,
            repeatability: ResourceTypeRepeatability,
            init: ResourceTypeBuilder.() -> Unit
    ): ResourceTypeBuilder {
        checkNameFree(name)
        return ResourceTypeBuilderImpl(this, name, repeatability)
                .also { subresourceBuilders[name] = it }
                .also(init)
    }

    override fun metricType(name: String): MetricTypeBuilder {
        return metricBuilders[name] ?: throw IllegalArgumentException("No metric type with name \"$name\" exists")
    }

    override fun subresourceType(name: String): ResourceTypeBuilder {
        return subresourceBuilders[name]
                ?: throw IllegalArgumentException("No subresource type with name \"$name\" exists")
    }


    fun build(): ResourceType {
        require(parentBuilder == null) { "Resource type hierarchy must be built from the root" }
        return build(null)
    }

    private fun build(parent: ResourceType?): ResourceType {
        val type = ResourceType(parent, name, description, repeatability)

        metricBuilders.forEach { _, builder -> builder.build(type) }
        subresourceBuilders.forEach { _, builder -> builder.build(type) }

        return type
    }

    private fun checkNameFree(name: String) {
        require(name !in metricBuilders) { "A metric type with the name \"$name\" already exists" }
        require(name !in subresourceBuilders) { "A subresource type with the name \"$name\" already exists" }
    }

}

/**
 * TODO: Document
 */
class MetricTypeBuilderImpl(
        override val name: String,
        override val type: MetricClass
) : MetricTypeBuilder {

    override var description: String = ""

    fun build(parent: ResourceType): MetricType {
        return MetricType(parent, name, description, type)
    }

}

/**
 * TODO: Document
 */
fun buildResourceModelSpecification(init: ResourceTypeBuilder.() -> Unit = {}): ResourceModelSpecification {
    val rootResourceType =
            ResourceTypeBuilderImpl(null, "", ResourceTypeRepeatability.RequiredOnce)
                    .also(init)
                    .build()
    return ResourceModelSpecification(rootResourceType)
}

/**
 * TODO: Document
 */
fun mergeResourceModelSpecifications(resourceModelSpecifications: List<ResourceModelSpecification>):
        ResourceModelSpecification {
    fun ResourceTypeBuilder.copy(resourceTypes: List<ResourceType>) {
        resourceTypes.flatMap { it.subresourceTypes.toList() }
                .groupBy({ it.first }, { it.second })
                .forEach { subresourceTypeName, subresourceTypes ->
                    val repeatability = subresourceTypes[0].repeatability
                    val description = subresourceTypes[0].description
                    require(subresourceTypes.all {
                        it.repeatability == repeatability &&
                                it.description == description
                    }) {
                        "Conflicting definitions of resource type \"${subresourceTypes[0].path}\""
                    }
                    newSubresourceType(subresourceTypeName, repeatability) {
                        this.description = description
                        copy(subresourceTypes)
                    }
                }
        resourceTypes.flatMap { it.metricTypes.toList() }
                .groupBy({ it.first }, { it.second })
                .forEach { metricTypeName, metricTypes ->
                    val metricClass = metricTypes[0].metricClass
                    val description = metricTypes[0].description
                    require(metricTypes.all { it.metricClass == metricClass && it.description == description }) {
                        "Conflicting definitions of metric type \"${metricTypes[0].path}\""
                    }
                    newMetricType(metricTypeName, metricClass) {
                        this.description = description
                    }
                }
    }
    return buildResourceModelSpecification {
        copy(resourceModelSpecifications.map { it.rootResourceType })
    }
}
