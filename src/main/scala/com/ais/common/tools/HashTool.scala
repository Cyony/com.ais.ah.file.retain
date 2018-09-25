package com.ais.common.tools

import java.nio.{ByteBuffer, ByteOrder}

/** Created by ShiGZ on 2017/2/15. */
object HashTool {
  /**
    * MurMurHash算法，是非加密HASH算法，性能很高，
    * 比传统的CRC32,MD5，SHA-1（这两个算法都是加密HASH算法，复杂度本身就很高，带来的性能上的损害也不可避免）
    * 等HASH算法要快很多，而且据说这个算法的碰撞率很低.
    * http://murmurhash.googlepages.com/
    */
  def hash(key:String):Long = {
    val buf: ByteBuffer = ByteBuffer.wrap(key.getBytes)
    val seed: Int = 0x1234ABCD

    val byteOrder: ByteOrder = buf.order
    buf.order(ByteOrder.LITTLE_ENDIAN)

    val m: Long = 0xc6a4a7935bd1e995L
    val r: Int = 47

    var h: Long = seed ^ (buf.remaining * m)

    var k: Long = 0L
    while (buf.remaining >= 8) {
      k = buf.getLong
      k *= m
      k ^= k >>> r
      k *= m
      h ^= k
      h *= m
    }

    if (buf.remaining > 0) {
      val finish: ByteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
      // for big-endian version, do this first:
      // finish.position(8-buf.remaining());
      finish.put(buf).rewind
      h ^= finish.getLong
      h *= m
    }

    h ^= h >>> r
    h *= m
    h ^= h >>> r

    buf.order(byteOrder)
     Math.abs(h)
  }

}
