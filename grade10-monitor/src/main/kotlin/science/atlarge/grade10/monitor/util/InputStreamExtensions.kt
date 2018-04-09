package science.atlarge.grade10.monitor.util

import java.io.InputStream

fun InputStream.tryReadLELong(): Long? {
    var value = 0L
    for (i in 0 until 8) {
        val nextByte = read()
        if (nextByte < 0)
            return null
        value = value or (nextByte.toLong() shl i * 8)
    }
    return value
}

fun InputStream.readLELong(): Long {
    return tryReadLELong()
            ?: throw IllegalStateException("Reached end-of-stream before reaching the end of the Long value")
}

fun InputStream.readLEB128Int(): Int {
    var value = 0
    var index = 0
    var nextByte: Int
    do {
        nextByte = read()
        if (nextByte < 0)
            throw IllegalStateException("Reached end-of-stream before reaching the end of the Long value")
        value = value or ((nextByte and 0x7F) shl index)
        index += 7
    } while ((nextByte and 0x80) != 0)
    return value
}

fun InputStream.readLEB128Long(): Long {
    var value = 0L
    var index = 0
    var nextByte: Int
    do {
        nextByte = read()
        if (nextByte < 0)
            throw IllegalStateException("Reached end-of-stream before reaching the end of the Long value")
        value = value or ((nextByte.toLong() and 0x7F) shl index)
        index += 7
    } while ((nextByte and 0x80) != 0)
    return value
}

fun InputStream.readString(): String {
    val str = StringBuilder()
    while (true) {
        val nextByte = read()
        if (nextByte == 0)
            return str.toString()
        str.append(nextByte.toChar())
    }
}
