package science.atlarge.grade10.monitor.util

/*
 * Append-only ArrayLists optimized for primitive types
 */

class LongArrayBuilder(initialSize: Int = 16) {

    var size = 0
        private set
    private var maxSize: Int = initialSize
    private var array: LongArray

    init {
        require(initialSize >= 0) { "Array size must be non-negative" }
        array = LongArray(maxSize)
    }

    fun append(value: Long) {
        if (size >= maxSize)
            extendArray()
        array[size] = value
        size++
    }

    fun dropLast(count: Int = 1) {
        require(count >= 0) { "Cannot drop a negative number of elements" }
        size = (size - count).coerceAtLeast(0)
    }

    private fun extendArray() {
        maxSize += maxSize / 2
        array = array.copyOf(maxSize)
    }

    fun toArray(): LongArray = array.copyOfRange(0, size)

}

class DoubleArrayBuilder(initialSize: Int = 16) {

    var size = 0
        private set
    private var maxSize: Int = initialSize
    private var array: DoubleArray

    init {
        require(initialSize >= 0) { "Array size must be non-negative" }
        array = DoubleArray(maxSize)
    }

    fun append(value: Double) {
        if (size >= maxSize)
            extendArray()
        array[size] = value
        size++
    }

    fun dropLast(count: Int = 1) {
        require(count >= 0) { "Cannot drop a negative number of elements" }
        size = (size - count).coerceAtLeast(0)
    }

    private fun extendArray() {
        maxSize += maxSize / 2
        array = array.copyOf(maxSize)
    }

    fun toArray(): DoubleArray = array.copyOfRange(0, size)

}
