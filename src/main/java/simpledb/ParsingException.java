package simpledb;

import java.io.Serial;

public class ParsingException extends Exception {
    @Serial
    private static final long serialVersionUID = 1L;

    public ParsingException(String string) {
        super(string);
    }

    public ParsingException(Exception e) {
        super(e);
    }
}
