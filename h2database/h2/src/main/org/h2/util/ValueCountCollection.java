package org.h2.util;

import org.h2.value.CompareMode;
import org.h2.value.Value;

import java.util.*;

/**
 * Allows compact storage of intermediary selection and projection results
 * that are necessary to be able to support certain aggregation functions.
 *
 * This class has two main "phases," first of which is a modification phase
 * where data may be added or removed, and all Value added and their Count
 * are kept track in a TreeMap for ordered retrieval later.
 *
 * Once the modification phase is complete, finalizeCollection() call must
 * be made before extrapolation and array-like value retrieval is enabled.
 * Once extrapolation is completed, this class is locked into read-only phase.
 *
 * @author Mike Ryu doryu@calpoly.edu
 * @version 2017-12-06
 */
public class ValueCountCollection implements Collection {

    /** size of the whole data structure*/
    private int size;

    /** tree map to compactly keep track of count of each occurrence of the value*/
    private TreeMap<Value, Integer> countMap;

    /** flags indicating state of this collection*/
    private boolean collectionFinalized;
    private boolean extrapolateReady;

    /** shared object between inner class and Extrapolator for extrapolation on a separate thread*/
    private final Object extrapolationSyncer = new Object();

    /** mapping from a natural index to an actual Value and count */
    private HashMap<Integer, ValueCountNode> extrapolatedIndexer;

    /** sequential (in-order) list of Value-count pairs as the result of extrapolation */
    private LinkedList<ValueCountNode> extrapolatedList;

    /** reference to a node that is the mode */
    private ValueCountNode modeNode;

    /**
     * Node in a compact LinkedList storage for extrapolation.
     */
    private class ValueCountNode {

        /**
         * Default constructor. Simply assigns Value and count.
         * @param value Value for the node.
         * @param count initial count for the node.
         */
        ValueCountNode(Value value, int count) {
            this.value = value;
            this.count = count;
        }

        Value value;
        int count;
    }

    /**
     * Runnable implementation to allow extrapolation in a separate thread.
     */
    private class Extrapolator implements Runnable {
        @Override
        public void run() {
            checkCollectionFinalizedPriorToExtrapolation();
            extrapolatedIndexer = new HashMap<>();
            extrapolatedList = new LinkedList<>();
            Set<Map.Entry<Value, Integer>> valueSet = countMap.entrySet();

            int maxCount = 0;
            for (Map.Entry<Value, Integer> entry: valueSet) {
                Value value = entry.getKey();
                int count = entry.getValue();
                int beginIndex = extrapolatedIndexer.size();
                int endIndex = beginIndex + count;

                ValueCountNode vcNode = new ValueCountNode(value, count);
                extrapolatedList.add(vcNode);

                for (int i = beginIndex; i < endIndex; i++) {
                    extrapolatedIndexer.put(i, vcNode);
                }

                if (count > maxCount) {
                    maxCount = count;
                    modeNode = vcNode;
                }
            }

            synchronized (extrapolationSyncer) {
                extrapolateReady = true;
                extrapolationSyncer.notifyAll();
            }
        }
    }

    /**
     * Iterator that allows extrapolated reads from this class.
     */
    private class ValueCountExtrapolatedIterator implements Iterator<Value> {

        int cursor;
        Value removalCache;

        /**
         * Default constructor. Checks extrapolation completion before proceeding to initialize members.
         */
        ValueCountExtrapolatedIterator() {
            checkCollectionExtrapolationPriorToGet();
            this.cursor = 0;
            removalCache = null;
        }

        @Override
        public boolean hasNext() {
            return cursor < size();
        }

        @Override
        public Value next() {
            if (hasNext()) {
                removalCache = extrapolatedGet(cursor++);
                return removalCache;
            } else {
                throw new NoSuchElementException("There is no next Extrapolated Value object available.");
            }
        }

        @Override
        public void remove() {
            if (removalCache != null) {
                ValueCountCollection.this.remove(removalCache);
            }
        }
    }

    /**
     * Default ValueCountCollection constructor.
     * TreeMap is initialized with a default comparator for Values for ordering.
     */
    public ValueCountCollection() {
        this.size = 0;
        this.countMap = new TreeMap<>(new Comparator<Value>() {
            @Override
            public int compare(Value o1, Value o2) {
                return o1.compareTypeSafe(o2, CompareMode.getInstance(null, 0));
            }
        });
        clearExtrapolationDataMembers();
    }
    /**
     * Use this method to signal the end of modification phase and finalize the content of this collection;
     * this method MUST be called before accessing collection with extrapolatedGet(naturalIndex) or extrapolatedMode().
     *
     * Once this method is called, no additional modification (addition or removal) or values will be permitted,
     * and extrapolation to reconstruct the intermediary table for aggregation begins on a separate thread.
     *
     * Once this method returns, calls to extrapolatedGet(naturalIndex) and extrapolatedMode() will be permitted.
     *
     * @throws InterruptedException if the extrapolation thread was interrupted unexpectedly.
     */
    public void finalizeCollection() throws InterruptedException {
        checkAndPreventDuplicateFinalization();
        collectionFinalized = true;
        extrapolate();
    }

    /**
     * Use this method as a fail-safe check in the client methods to ensure that
     * the extrapolation has been completed before accessing the Value members.
     *
     * @return true if extrapolation is complete and this object is in read-only mode, false otherwise.
     */
    public boolean isExtrapolateReady() {
        return extrapolateReady;
    }

    /**
     * Returns the desired item at a given array-like natural index [0, size).
     *
     * IndexOutOfBoundsException gets through if the index given is negative or out of bounds.
     *
     * @param naturalIndex array-like sequential index into this Collection.
     * @return Value at the given index, if the index is within bounds.
     */
    public Value extrapolatedGet(int naturalIndex) {
        checkCollectionExtrapolationPriorToGet();
        ValueCountNode desiredNode = extrapolatedIndexer.get(naturalIndex);

        if (desiredNode == null) {
            throw new IndexOutOfBoundsException("index " + naturalIndex + " does not occur in this collection.");
        }

        return desiredNode.value;
    }

    /**
     * Returns the Value that occurs most frequently in this collection.
     *
     * If there is a tie, Value that was most recently encountered during the modification phase is selected.
     *
     * @return the mode (most frequent) Value in this collection.
     */
    public Value extrapolatedMode() {
        return modeNode == null ? null : modeNode.value;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Object o) {
        return (o instanceof Value) && countMap.containsKey(o);
    }

    @Override
    public Iterator iterator() {
        // constructor checks for finalization and extrapolator readiness
        return new ValueCountExtrapolatedIterator();
    }

    @Override
    public boolean add(Object o) {
        checkCollectionFinalizedPriorToModification();
        try {
            if (o instanceof Value) {
                countMap.put((Value) o, contains(o) ? 1 + countMap.get(o) : 1);
            }
        } catch (RuntimeException re) { // probably not the best thing to do
            return false;
        }
        size++;
        return true;
    }

    @Override
    public boolean remove(Object o) {
        checkCollectionFinalizedPriorToModification();
        try {
            if (!(o instanceof Value) || (size == 0) || !contains(o)) {
                return false;
            }

            int newCount = countMap.get(o) - 1;

            if (newCount > 0) {
                countMap.put((Value) o, countMap.get(o) - 1);
            } else {
                countMap.remove(o);
            }

        } catch (RuntimeException re) { // probably not the best thing to do
            return false;
        }
        size--;
        return true;
    }

    @Override
    public boolean addAll(Collection c) {
        checkCollectionFinalizedPriorToModification();
        boolean result = true;
        for (Object o: c) {
            result &= add(o);
        }
        return result;
    }

    @Override
    public void clear() {
        this.size = 0;
        this.countMap.clear();
        clearExtrapolationDataMembers();
    }

    @Override
    public boolean removeAll(Collection c) {
        checkCollectionFinalizedPriorToModification();
        boolean result = true;
        for (Object o: c) {
            result &= remove(o);
        }
        return result;
    }

    @Override
    public boolean containsAll(Collection c) {
        if (c.isEmpty()) {
            return true;
        }
        else {
            for (Object o: c) {
                if (!contains(o)) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public Object[] toArray() {
        checkCollectionFinalizedPriorToExtrapolation();
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray(Object[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection c) {
        throw new UnsupportedOperationException();
        // TODO as necessary
    }

    /* PRIVATE HELPER METHODS BELOW */

    private void extrapolate() throws InterruptedException {
        Thread extrapolationThread = new Thread(new Extrapolator());
        extrapolationThread.start();

        synchronized (extrapolationSyncer) {
            while (!extrapolateReady) {
                extrapolationSyncer.wait();
            }
        }
    }

    private void clearExtrapolationDataMembers() {
        this.collectionFinalized = false;
        this.extrapolateReady = false;

        this.extrapolatedIndexer = null;
        this.extrapolatedList = null;

        this.modeNode = null;
    }

    private void checkAndPreventDuplicateFinalization() {
        if (collectionFinalized || extrapolateReady) {
            throw new IllegalStateException(
                    "Duplicate finalization attempt detected; collection has already been finalized.");
        }
    }

    private void checkCollectionExtrapolationPriorToGet() {
        if (!extrapolateReady) {
            throw new IllegalStateException(
                    "Cannot get elements prior to extrapolation; use isExtrapolateReady() to check its completion state.");
        }
    }

    private void checkCollectionFinalizedPriorToExtrapolation() {
        if (!collectionFinalized) {
            throw new IllegalStateException(
                    "Collection must be finalized before extrapolation can proceed.");
        }
    }

    private void checkCollectionFinalizedPriorToModification() {
        if (collectionFinalized) {
            throw new IllegalStateException(
                    "Modifying a finalized collection is not permitted.");
        }
    }
}
