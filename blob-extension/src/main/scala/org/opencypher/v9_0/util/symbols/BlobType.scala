package org.opencypher.v9_0.util.symbols

/**
 * using fading instead of source module recompilation
 * because of lack of v9.0 source project
 */

object BlobType {
  val instance = new BlobType() {
    override val parentType = CTAny
    override val toString = "Blob"
    override val toNeoTypeString = "BLOB?"
  }
}

sealed trait BlobType extends CypherType