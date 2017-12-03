package org.h2.util;

import org.h2.value.ValueInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ValueCountCollectionTest {

    private ValueCountCollection vcc1;
    ValueInt[] testInts;

    @Before
    public void setUp() throws Exception {
        this.vcc1 = new ValueCountCollection();
        this.testInts = new ValueInt[100];

        for (int i = 0; i < 100; i++) {
            testInts[i] = ValueInt.get(i);
        }
    }

    @After
    public void tearDown() throws Exception {
        this.vcc1 = null;
    }

    @Test
    public void finalizeCollection() {
    }

    @Test
    public void isExtrapolateReady() {
    }

    @Test
    public void extrapolatedGet() {
    }

    @Test
    public void extrapolatedMode() {
    }

    @Test
    public void size() {
    }

    @Test
    public void isEmpty() {
    }

    @Test
    public void contains() {
    }

    @Test
    public void iterator() {
    }

    @Test
    public void toArray() {
    }

    @Test
    public void add() {
        vcc1.add(testInts[0]);
        assertEquals(true, vcc1.contains(testInts[0]));
    }

    @Test
    public void remove() {
    }

    @Test
    public void addAll() {
    }

    @Test
    public void clear() {
    }

    @Test
    public void retainAll() {
    }

    @Test
    public void removeAll() {
    }

    @Test
    public void containsAll() {
    }

    @Test
    public void toArray1() {
    }
}