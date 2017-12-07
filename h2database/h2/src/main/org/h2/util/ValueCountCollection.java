package org.h2.util;

import org.h2.value.CompareMode;
import org.h2.value.Value;

import java.util.*;

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

    // sequential (in-order) list of Value-count pairs as the result of extrapolation
    private LinkedList<ValueCountNode> extrapolatedList;

    // reference to a node that is the mode
    private ValueCountNode modeNode;

    /* ValueCountNode Constructor */
    /**
     * Returns the number of rows in the result set.
     *
     */
    private class ValueCountNode {
        /** @param value the key
         * @param count  the value */
        ValueCountNode(Value value, int count) {
            this.value = value;
            this.count = count;
        }

        Value value;
        int count;
    }

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

    private class ValueCountExtrapolatedIterator implements Iterator<Value> {

        int cursor;
        Value removalCache;

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
     * ValueCountCollection Constructor
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
     * Must be called before accessing collection.
     * @throws InterruptedException if synchronization fails
     */
    public void finalizeCollection() throws InterruptedException {
        checkAndPreventDuplicateFinalization();
        collectionFinalized = true;
        extrapolate();
    }

    /**
     * @return true if calls to extrapolated*() methods may be called
     */
    public boolean isExtrapolateReady() {
        return extrapolateReady;
    }

    /**
     * Returns the desired item
     * @param naturalIndex logical index into collection
     * @return the desired value
     */
    public Value extrapolatedGet(int naturalIndex) {
        checkCollectionExtrapolationPriorToGet();
        ValueCountNode desiredNode = extrapolatedIndexer.get(naturalIndex);
        return desiredNode.value;
    }

    /**
     * @return the mode
     */
    public Value extrapolatedMode() {
        return modeNode == null ? null : modeNode.value;
    }

    private void extrapolate() throws InterruptedException {
        Thread extrapolationThread = new Thread(new Extrapolator());
        extrapolationThread.start();

        synchronized (extrapolationSyncer) {
            while (!extrapolateReady) {
                extrapolationSyncer.wait();
            }
        }
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

    private void clearExtrapolationDataMembers() {
        this.collectionFinalized = false;
        this.extrapolateReady = false;

        this.extrapolatedIndexer = null;
        this.extrapolatedList = null;

        this.modeNode = null;
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
