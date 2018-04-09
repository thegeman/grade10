package science.atlarge.grade10.util

/**
 * Creates a topological ordering of the vertices in a graph with a given list of [vertices] and [edges]. If the graph
 * contains a cycle, no topological ordering exists, and null is returned.
 *
 * @param[vertices] The vertices of the graph
 * @param[edges] The edges of the graph, stored as a map of a source vertex to a list of destination vertices
 * @return A topological ordering of the vertices in the graph, or null if no such ordering exists
 */
fun <V> topologicalSort(vertices: List<V>, edges: Map<V, Iterable<V>>): List<V>? {
    val reverseEdges = reverseEdgeCount(edges)
    val ordering = mutableListOf<V>()
    vertices.filterTo(ordering, { it !in reverseEdges })

    var vertexToVisit = 0
    while (vertexToVisit < ordering.size && ordering.size < vertices.size) {
        val nextVertex = ordering[vertexToVisit]
        edges[nextVertex]?.forEach { dst ->
            if (reverseEdges[dst] == 1) {
                ordering.add(dst)
            } else {
                reverseEdges[dst] = reverseEdges[dst]!! - 1
            }
        }
        vertexToVisit++
    }

    return if (ordering.size < vertices.size) {
        null
    } else {
        ordering
    }
}

private fun <V> reverseEdgeCount(edges: Map<V, Iterable<V>>): MutableMap<V, Int> {
    val result = mutableMapOf<V, Int>()
    edges.forEach { e ->
        e.value.forEach { dst ->
            result[dst] = result.getOrElse(dst, { 0 }) + 1
        }
    }
    return result
}

fun <T> depthFirstSearch(root: T, childrenSelector: (parent: T) -> Iterable<T>, callback: (node: T, depth: Int) -> Unit) {
    fun dfs(node: T, depth: Int) {
        callback(node, depth)
        childrenSelector(node).forEach { dfs(it, depth + 1) }
    }
    dfs(root, 0)
}

fun <T> collectTreeNodes(root: T, childrenSelector: (parent: T) -> Iterable<T>): Set<T> {
    val nodes = mutableSetOf<T>()
    depthFirstSearch(root, childrenSelector) { n, _ -> nodes.add(n) }
    return nodes
}
