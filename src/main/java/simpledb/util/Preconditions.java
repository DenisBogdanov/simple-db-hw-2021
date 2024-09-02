package simpledb.util;

import java.util.NoSuchElementException;

public final class Preconditions {
    private Preconditions() {
    }

    public static void checkIndex(int index, int containerSize) {
        if (index < 0) throw new NoSuchElementException("Index is negative");
        if (index >= containerSize) throw new NoSuchElementException("Index is out of bounds");
    }

    public static <T> void checkValueExists(T value) {
        if (value == null) throw new NoSuchElementException();
    }
}
