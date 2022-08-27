package org.apache.druid.collections;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;

import java.util.*;

public class MultiColumnSorter<T>
{

  private final MinMaxPriorityQueue<MultiColumnSorterElement<T>> queue;

  public MultiColumnSorter(int limit, Comparator<MultiColumnSorterElement<T>> comparator)
  {
    this.queue = MinMaxPriorityQueue
        .orderedBy(Ordering.from(comparator))
        .maximumSize(limit)
        .create();
  }

  /**
   * Offer an element to the sorter.
   */
  public void add(T element, List<Comparable> orderByColumns)
  {
    queue.offer(new MultiColumnSorterElement<>(element, orderByColumns));
  }

  /**
   * Drain elements in sorted order (least first).
   */
  public Iterator<T> drain()
  {
    return new Iterator<T>()
    {
      @Override
      public boolean hasNext()
      {
        return !queue.isEmpty();
      }

      @Override
      public T next()
      {
        return queue.poll().getElement();
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  @VisibleForTesting
  public static class MultiColumnSorterElement<T>
  {
    private final T element;
    private final List<Comparable> orderByColumValues;

    public MultiColumnSorterElement(T element, List<Comparable> orderByColums)
    {
      this.element = element;
      this.orderByColumValues = orderByColums;
    }

    public T getElement()
    {
      return element;
    }

    public List<Comparable> getOrderByColumValues()
    {
      return orderByColumValues;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      MultiColumnSorterElement<?> that = (MultiColumnSorterElement<?>) o;
      return orderByColumValues == that.orderByColumValues &&
             Objects.equals(element, that.element);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(element, orderByColumValues);
    }
  }
}
