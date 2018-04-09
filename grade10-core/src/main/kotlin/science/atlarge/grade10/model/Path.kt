package science.atlarge.grade10.model

/**
 * Single component of a [Path].
 */
typealias PathComponent = String

/**
 * Representation of an arbitrary path expression. [Paths][Path] are used to uniquely identify entries in a hierarchical
 * data structure, e.g., phases in an execution systemModel.
 *
 * @property[pathComponents] ordered list of [PathComponents][PathComponent] that comprise the path.
 * @property[isRelative] true if the path is relative, false if the path is absolute.
 */
data class Path(
        val pathComponents: List<PathComponent>,
        val isRelative: Boolean
) : Comparable<Path> {

    /**
     * @return true if the path is absolute, false if the path is relative.
     */
    val isAbsolute: Boolean
        get() = !isRelative

    /**
     * @return a new path obtained by appending [component] to this path's components.
     */
    fun resolve(component: PathComponent): Path {
        return Path(pathComponents + component, isRelative)
    }

    /**
     * Resolves [otherPath] with respect to this path. If [otherPath] is relative, the resolved path is the result of
     * appending [otherPath]'s components to this path's components. Otherwise, the result is equal to [otherPath].
     *
     * @return the resolved path.
     */
    fun resolve(otherPath: Path): Path {
        return if (otherPath.isRelative) {
            Path(pathComponents + otherPath.pathComponents, isRelative)
        } else {
            otherPath
        }
    }

    /**
     * @return true iff [other] is equal to or a subpath of this path.
     */
    operator fun contains(other: Path): Boolean {
        if (isRelative != other.isRelative) {
            return false
        }

        val thisComponents = this.toCanonicalPath().pathComponents
        val otherComponents = other.toCanonicalPath().pathComponents

        if (thisComponents.size > otherComponents.size) {
            return false
        }

        for (i in thisComponents.indices) {
            if (thisComponents[i] != otherComponents[i]) {
                return false
            }
        }

        return true
    }

    /**
     * Checks if the path is canonical. A canonical path contains no "." components and, if absolute, no ".."
     * components. Relative paths may contain ".." components, but not after any component of a different kind.
     *
     * @return true iff the path is canonical.
     */
    val isCanonical: Boolean
        get() = if (isRelative) {
            pathComponents.none { it == "." } &&
                    pathComponents.asSequence().dropWhile { it == ".." }.none { it == ".." }
        } else {
            pathComponents.none { it == "." || it == ".." }
        }

    /**
     * Derives a canonical representation of this path by resolving any "." and ".." path components.
     *
     * @throws[InvalidPathException] if the path is absolute and attempts to reference the parent of the root path.
     * @return The canonical representation of this path.
     */
    fun toCanonicalPath(): Path {
        if (isCanonical) {
            return this
        }

        val newPathComponents = mutableListOf<PathComponent>()
        pathComponents.forEach { pathComponent ->
            when (pathComponent) {
                "." -> {
                }
                ".." -> {
                    when {
                        newPathComponents.isNotEmpty() -> newPathComponents.removeAt(newPathComponents.size - 1)
                        isRelative -> newPathComponents.add("..")
                        else -> throw InvalidPathException(this, "${SEPARATOR}..")
                    }
                }
                else -> newPathComponents.add(pathComponent)
            }
        }
        return Path(newPathComponents, isRelative)
    }

    override fun compareTo(other: Path): Int {
        if (isAbsolute && !other.isAbsolute) {
            return -1
        }
        if (!isAbsolute && other.isAbsolute) {
            return 1
        }

        val len = minOf(pathComponents.size, other.pathComponents.size)
        for (i in 0 until len) {
            val compareComponents = pathComponents[i].compareTo(other.pathComponents[i])
            if (compareComponents != 0) {
                return compareComponents
            }
        }

        return pathComponents.size - other.pathComponents.size
    }

    override fun toString(): String {
        val separatorString = SEPARATOR.toString()
        return pathComponents.joinToString(separator = separatorString, prefix = if (isRelative) "" else separatorString)
    }

    companion object {
        /**
         * Character used as delimiter between path components.
         */
        const val SEPARATOR = '/'

        /**
         * Root of any absolute path.
         */
        val ROOT = Path(emptyList(), false)

        /**
         * @param[pathExpression] the String representation of a path.
         * @return the parsed [Path].
         */
        fun parse(pathExpression: String): Path {
            val isRelative: Boolean
            var remainingPath = pathExpression
            if (pathExpression.startsWith(SEPARATOR)) {
                isRelative = false
                remainingPath = pathExpression.trimStart(SEPARATOR)
            } else {
                isRelative = true
            }

            val pathComponents = mutableListOf<PathComponent>()
            while (remainingPath.isNotEmpty()) {
                val nextSeparator = remainingPath.indexOf(SEPARATOR)
                if (nextSeparator == -1) {
                    pathComponents.add(remainingPath)
                    remainingPath = ""
                } else {
                    pathComponents.add(remainingPath.substring(0, nextSeparator))
                    remainingPath = remainingPath.substring(nextSeparator + 1).trimStart(SEPARATOR)
                }
            }

            return Path(pathComponents, isRelative)
        }

        /**
         * @return a new relative path with the given [components].
         */
        fun relative(vararg components: PathComponent): Path {
            return Path(components.toList(), true)
        }

        /**
         * @return a new absolute path with the given [components].
         */
        fun absolute(vararg components: PathComponent): Path {
            return Path(components.toList(), false)
        }

    }
}

/**
 * TODO: Document
 */
class InvalidPathException(
        val path: Path,
        val invalidPartiallyResolvedPath: String
) : Exception()
