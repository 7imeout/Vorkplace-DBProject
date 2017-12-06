package org.h2.util;

import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

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
        this.testInts = null;
    }

    @Test
    public void testExtrapolatedGet() throws InterruptedException {
        vcc1.addAll(Arrays.asList(testInts));
        vcc1.finalizeCollection();

        for (int i = 0; i < testInts.length; i++) {
            assertEquals(i, vcc1.extrapolatedGet(i).getInt());
        }
    }

    @Test
    public void testExtrapolatedModeSmall() throws InterruptedException {
        testAdd();
        vcc1.finalizeCollection();
        boolean isReady = vcc1.isExtrapolateReady();
        assertTrue(isReady);

        Value modeVal = vcc1.extrapolatedMode();
        assertEquals(0, modeVal.getInt());
    }

    @Test
    public void testExtrapolatedModeLarge() throws InterruptedException {
        testAddAll();
        vcc1.add(testInts[88]);
        vcc1.add(testInts[88]);
        vcc1.add(testInts[88]);
        vcc1.add(testInts[88]);
        vcc1.add(testInts[88]);
        vcc1.finalizeCollection();

        boolean isReady = vcc1.isExtrapolateReady();
        assertTrue(isReady);

        Value modeVal = vcc1.extrapolatedMode();
        assertEquals(88, modeVal.getInt());
    }

    @Test
    public void testIsEmpty() {
        assertTrue(vcc1.isEmpty());

        testAdd();
        assertFalse(vcc1.isEmpty());
    }

    @Test
    public void testContainsAndSize() {
        int sizeOracle = 0;

        for (Value val : testInts) {
            vcc1.add(val);
            assertTrue(vcc1.contains(val));
            assertEquals(++sizeOracle, vcc1.size());
        }

        for (Value val : testInts) {
            vcc1.remove(val);
            assertFalse(vcc1.contains(val));
            assertEquals(--sizeOracle, vcc1.size());
        }
    }

    @Test
    public void testIterator() throws InterruptedException {
        testAddAll();
        vcc1.finalizeCollection();
        Iterator iter = vcc1.iterator();

        int i = 0;
        while (iter.hasNext()) {
            assertEquals(iter.next(), testInts[i++]);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testToArrayNoArg() throws InterruptedException {
        vcc1.finalizeCollection();
        vcc1.toArray();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testToArrayObjectArrayArg() {
        vcc1.toArray(new Object[1]);
    }

    @Test
    public void testAdd() {
        vcc1.add(testInts[0]);
        assertTrue(vcc1.contains(testInts[0]));
        vcc1.add(testInts[1]);
        assertTrue(vcc1.contains(testInts[0]));
        assertTrue(vcc1.contains(testInts[1]));
    }

    @Test
    public void testRemove() {
        testAdd();

        assertTrue(vcc1.remove(testInts[0]));
        assertFalse(vcc1.contains(testInts[0]));

        assertTrue(vcc1.remove(testInts[1]));
        assertFalse(vcc1.contains(testInts[1]));
    }

    @Test
    public void testAddAll() {
        vcc1.addAll(Arrays.asList(testInts));
        assertEquals(100, vcc1.size());
        for (Value val : testInts) {
            assertTrue(vcc1.contains(val));
        }
    }

    @Test
    public void testClear() throws InterruptedException {
        vcc1.addAll(Arrays.asList(testInts));
        assertTrue(vcc1.contains(testInts[0]));
        assertTrue(vcc1.contains(testInts[55]));
        assertTrue(vcc1.contains(testInts[99]));
        vcc1.finalizeCollection();

        assertEquals(100, vcc1.size());
        assertEquals(1, vcc1.extrapolatedGet(1).getInt());

        try {
            vcc1.add(testInts[99]);
            fail("Expected IllegalStateException to be thrown.");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Modifying a finalized collection is not permitted.");
        }

        vcc1.clear();
        assertEquals(0, vcc1.size());
        assertFalse(vcc1.contains(testInts[99]));

        vcc1.add(testInts[99]);
        assertEquals(1, vcc1.size());
        assertTrue(vcc1.contains(testInts[99]));

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRetainAll() {
        vcc1.retainAll(null);
    }

    @Test
    public void testRemoveAll() {
        testAddAll();
        assertTrue(vcc1.contains(testInts[0]));
        assertTrue(vcc1.contains(testInts[55]));
        assertTrue(vcc1.contains(testInts[99]));

        vcc1.removeAll(Arrays.asList(testInts));
        assertFalse(vcc1.contains(testInts[0]));
        assertFalse(vcc1.contains(testInts[55]));
        assertFalse(vcc1.contains(testInts[99]));

        assertTrue(vcc1.isEmpty());
    }

    @Test
    public void testContainsAll() {
        assertTrue(vcc1.containsAll(new ArrayList<Value>()));
        testAddAll();
        assertTrue(vcc1.containsAll(Arrays.asList(testInts)));
    }
}