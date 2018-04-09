package science.atlarge.grade10.model.execution

import science.atlarge.grade10.util.topologicalSort

/**
 * TODO: Document
 */
@DslMarker
annotation class ExecutionModelSpecificationDsl

/**
 * TODO: Document
 */
@ExecutionModelSpecificationDsl
interface PhaseTypeBuilder {

    val name: String

    val repeatability: PhaseTypeRepeatability

    var description: String

    fun newSubphaseType(
            name: String,
            repeatability: PhaseTypeRepeatability = PhaseTypeRepeatability.NonRepeated,
            init: PhaseTypeBuilder.() -> Unit = {}
    ): PhaseTypeBuilder

    fun subphaseType(name: String): PhaseTypeBuilder

    infix fun before(otherPhaseType: PhaseTypeBuilder)

    infix fun after(otherPhaseType: PhaseTypeBuilder)

}

/**
 * TODO: Document
 */
class PhaseTypeBuilderImpl(
        private val parentBuilder: PhaseTypeBuilderImpl?,
        override val name: String,
        override val repeatability: PhaseTypeRepeatability
) : PhaseTypeBuilder {

    override var description: String = ""

    private val subphaseTypes = mutableMapOf<String, PhaseTypeBuilderImpl>()

    private val subphaseDependencies = mutableMapOf<String, MutableSet<String>>()

    init {
        require(parentBuilder == null || name.isNotBlank()) {
            "Non-root phase types must have a non-blank name"
        }
    }

    override fun newSubphaseType(
            name: String,
            repeatability: PhaseTypeRepeatability,
            init: PhaseTypeBuilder.() -> Unit
    ): PhaseTypeBuilder {
        require(name !in subphaseTypes) { "A subphase type with the name \"$name\" already exists" }
        return PhaseTypeBuilderImpl(this, name, repeatability)
                .also { subphaseTypes[name] = it }
                .also(init)
    }

    override fun subphaseType(name: String): PhaseTypeBuilder {
        return subphaseTypes[name] ?: throw IllegalArgumentException("No subphase type with name \"$name\" exists")
    }

    override fun before(otherPhaseType: PhaseTypeBuilder) {
        otherPhaseType after this
    }

    override fun after(otherPhaseType: PhaseTypeBuilder) {
        otherPhaseType as PhaseTypeBuilderImpl
        require(parentBuilder != null) { "Root phase type cannot have a dependency" }
        require(otherPhaseType.parentBuilder != null) { "Root phase type cannot be a dependency" }
        require(parentBuilder === otherPhaseType.parentBuilder) {
            "Dependencies can only be created between phase types with the same parent"
        }

        parentBuilder!!.addDependency(this.name, otherPhaseType.name)
    }

    private fun addDependency(type: String, dependency: String) {
        subphaseDependencies
                .getOrPut(type) { mutableSetOf() }
                .add(dependency)
    }

    fun build(): PhaseType {
        require(parentBuilder == null) { "Phase type hierarchy must be built from the root" }
        return build(null, emptyList())
    }

    private fun build(parent: PhaseType?, dependencies: List<PhaseType>): PhaseType {
        val phaseType = PhaseType(parent, name, dependencies, description, repeatability)

        val dependantsPerPhaseType = computeReverseDependencies()
        val buildOrder = topologicalSort(subphaseTypes.keys.toList(), dependantsPerPhaseType)
                ?: throw IllegalArgumentException("Detected cyclic dependencies between subphase types of " +
                        "\"${phaseType.path}\"")

        val builtSubphaseTypes = mutableMapOf<String, PhaseType>()
        buildOrder.forEach { t ->
            builtSubphaseTypes[t] = subphaseTypes[t]!!.build(
                    parent = phaseType,
                    dependencies = subphaseDependencies[t]?.map { builtSubphaseTypes[it]!! } ?: emptyList()
            )
        }

        return phaseType
    }

    private fun computeReverseDependencies(): Map<String, List<String>> {
        val dependantsPerPhaseType = mutableMapOf<String, MutableList<String>>()
        subphaseTypes.keys.forEach { dependantsPerPhaseType[it] = mutableListOf() }
        subphaseDependencies.forEach { dependant, dependencies ->
            dependencies.forEach {
                dependantsPerPhaseType[it]?.add(dependant)
            }
        }
        return dependantsPerPhaseType
    }

}

/**
 * TODO: Document
 */
fun buildExecutionModelSpecification(init: PhaseTypeBuilder.() -> Unit = {}): ExecutionModelSpecification {
    val rootPhaseType = PhaseTypeBuilderImpl(null, "", PhaseTypeRepeatability.NonRepeated)
            .also(init)
            .build()
    return ExecutionModelSpecification(rootPhaseType)
}
