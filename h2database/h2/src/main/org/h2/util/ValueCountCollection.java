package org.h2.util;

import org.h2.value.CompareMode;
import org.h2.value.Value;

import java.util.*;

public class ValueCountCollection implements Collection {

    // size of the whole data structure
    private int size;

    // tree map to compactly keep track of count of each occurrence of the value
    private TreeMap<Value, Integer> countMap;

    // flags indicating state of this collection
    private boolean collectionFinalized;
    private boolean extrapolateReady;

    // mapping from a natural index to an actual Value and count
    private HashMap<Integer, ValueCountNode> extrapolatedIndexer;

    // sequential (in-order) list of Value-count pairs as the result of extrapolation
    private LinkedList<ValueCountNode> extrapolatedList;

    // reference to a node that is the mode
    private ValueCountNode modeNode;

    private class ValueCountNode {
        ValueCountNode(Value value, int count) {
            this.value = value;
            this.count = count;
        }

        Value value;
        int count;
    }

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

    public void finalizeCollection() {
        checkAndPreventDuplicateFinalizatioon();
        extrapolate();
    }

    public boolean isExtrapolateReady() {
        return extrapolateReady;
    }

    public Value extrapolatedGet(int naturalIndex) {
        checkCollectionExtrapolationPriorToGet();
        ValueCountNode desiredNode = extrapolatedIndexer.get(naturalIndex);
        return desiredNode.value;
    }

    public Value extrapolatedMode() {
        return modeNode == null ? null : modeNode.value;
    }

    private void extrapolate() {
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

        extrapolateReady = true;
    }

    private void checkAndPreventDuplicateFinalizatioon() {
        if (collectionFinalized || extrapolateReady) {
            throw new IllegalStateException("Collection has already been finalized.");
        }
    }

    private void checkCollectionExtrapolationPriorToGet() {
        if (!extrapolateReady) {
            throw new IllegalStateException("Cannot get elements prior to extrapolation.");
        }
    }

    private void checkCollectionFinalizedPriorToExtrapolation() {
        if (!collectionFinalized) {
            throw new IllegalStateException("Collection must be finalized before extrapolation can proceed.");
        }
    }

    private void checkCollectionFinalizedPriorToModification() {
        if (collectionFinalized) {
            throw new IllegalStateException("Cannot add elements to finalized collection.");
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
        return countMap.containsKey(o);
    }

    @Override
    public Iterator iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public boolean add(Object o) {
        checkCollectionFinalizedPriorToModification();
        int count = 0;
        try {
            if (contains(o)) {
                count = 1 + countMap.get(o);
            }
            countMap.put((Value) o, count);
        } catch (RuntimeException re) {
            return false;
        }
        size++;
        return true;
    }

    @Override
    public boolean remove(Object o) {
        checkCollectionFinalizedPriorToModification();
        try {
            if ((size == 0) || !contains(o)) {
                return false;
            }
            countMap.put((Value) o, countMap.get(o) - 1);
        } catch (RuntimeException re) {
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
    public boolean retainAll(Collection c) {
        throw new UnsupportedOperationException();
        // TODO as necessary
    }

    @Override
    public boolean removeAll(Collection c) {
        throw new UnsupportedOperationException();
        // TODO as necessary
    }

    @Override
    public boolean containsAll(Collection c) {
        if (c.isEmpty()) {
            return true;
        }
        else {
            for (Object o: c) {
                if (contains(o)) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public Object[] toArray(Object[] a) {
        throw new UnsupportedOperationException();
    }

    private void clearExtrapolationDataMembers() {
        this.collectionFinalized = false;
        this.extrapolateReady = false;

        this.extrapolatedIndexer = null;
        this.extrapolatedList = null;

        this.modeNode = null;
    }
}
