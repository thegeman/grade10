package science.atlarge.grade10.model

@Suppress("UNCHECKED_CAST")
abstract class HierarchyComponent<This : HierarchyComponent<This>>(
        parent: This?,
        componentId: PathComponent
) {

    @Suppress("CanBePrimaryConstructorProperty")
    open val parent: This? = parent

    val componentId: PathComponent = if (parent == null) "" else componentId

    val path: Path by lazy {
        this.parent?.path?.resolve(componentId) ?: Path.ROOT
    }

    val isRoot: Boolean
        get() = parent == null

    open val root: This
        get() {
            var pointer = this as This
            while (true) {
                pointer = pointer.parent ?: return pointer
            }
        }

    init {
        require(parent == null || componentId.isNotBlank()) {
            "Component ID must not be blank for child components"
        }
    }

    fun resolve(path: Path): This? {
        return if (path.isRelative) {
            resolveComponents(path.pathComponents)
        } else {
            root.resolveComponents(path.pathComponents)
        }
    }

    protected abstract fun lookupChild(childId: PathComponent): This?

    private fun resolveComponents(pathComponents: List<PathComponent>): This? {
        return pathComponents.fold(this as This?) { currentComponent, nextId ->
            when (nextId) {
                "." -> currentComponent
                ".." -> currentComponent?.parent
                else -> currentComponent?.lookupChild(nextId)
            }
        }
    }

}
