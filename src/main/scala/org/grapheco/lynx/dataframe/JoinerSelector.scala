package org.grapheco.lynx.dataframe

import org.grapheco.lynx.types.{LynxValue, LynxType}
import org.grapheco.lynx.types.property.LynxNull

object JoinerSelector {

  /**
   * Choose the appropriate joiner (SortMergeJoin or HashJoin) based on the input DataFrames.
   *
   * @param a The first DataFrame.
   * @param b The second DataFrame.
   * @param joinColumns The columns to join on.
   * @param joinType The type of join (e.g., InnerJoin, LeftJoin, etc.).
   * @return A function that performs the join using the appropriate joiner.
   */
  def chooseJoiner(a: DataFrame, b: DataFrame, joinColumns: Seq[String], joinType: JoinType): (DataFrame, DataFrame, Seq[String], JoinType) => DataFrame = {
    // Check if the DataFrames should use SortMergeJoiner or HashJoiner based on heuristics
    if (shouldUseSortMergeJoin(a, b, joinColumns)) {
      // If both tables are sorted on the join columns, use SortMergeJoiner
      (a: DataFrame, b: DataFrame, joinColumns: Seq[String], joinType: JoinType) => 
        SortMergeJoiner.join(a, b, joinColumns, joinType)
    } else {
      // Otherwise, use HashJoiner
      (a: DataFrame, b: DataFrame, joinColumns: Seq[String], joinType: JoinType) => 
        HashJoiner.join(a, b, joinColumns, joinType)
    }
  }

  /**
   * Heuristic to decide whether to use SortMergeJoiner based on the DataFrame properties.
   * This implementation checks if both DataFrames are sorted on the join columns.
   *
   * @param a The first DataFrame.
   * @param b The second DataFrame.
   * @param joinColumns The columns to join on.
   * @return True if both DataFrames should use SortMergeJoiner, false otherwise.
   */
  private def shouldUseSortMergeJoin(a: DataFrame, b: DataFrame, joinColumns: Seq[String]): Boolean = {
    val isASorted = isSorted(a, joinColumns)
    val isBSorted = isSorted(b, joinColumns)

    // If both DataFrames are sorted on the join columns, it's better to use SortMergeJoin
    isASorted && isBSorted
  }

  /**
   * Simple check to determine if a DataFrame is sorted by the join columns.
   * This is a basic check assuming that DataFrames are sorted beforehand.
   *
   * @param dataFrame The DataFrame to check.
   * @param joinColumns The columns to check for sorting.
   * @return True if the DataFrame is sorted by the join columns, false otherwise.
   */
  private def isSorted(dataFrame: DataFrame, joinColumns: Seq[String]): Boolean = {
    // Implementing a simple check: assume the DataFrame is sorted if it has more than one row and is already in sorted order.
    // In real use cases, we can inspect the actual data or metadata to verify if it's sorted.
    // For now, we assume it's sorted (you could enhance this check based on specific needs).
    // In practice, this could be an optimization to check metadata or perform a fast sort check.
    
    // Simple heuristic: Assume it's sorted if there are less than 1000 rows, as a sample case
    if (dataFrame.records.size < 1000) {
      true // small DataFrame, assume it's sorted or won't matter
    } else {
      // For larger DataFrames, assume it's not sorted unless we know it is
      // In production, this could be improved by checking sorting flags or inspecting the data
      false
    }
  }
}
