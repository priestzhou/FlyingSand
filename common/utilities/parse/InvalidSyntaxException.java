package utilities.parse;

import java.util.Formatter;

public class InvalidSyntaxException extends Exception {
    public InvalidSyntaxException(String msg) {
        super(msg);
    }
}
