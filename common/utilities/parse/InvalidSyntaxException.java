package utilities.parse;

import java.util.Formatter;

public class InvalidSyntaxException extends Exception {
    private String message = null;
    private long line = 0;
    private long row = 0;

    public InvalidSyntaxException(String msg) {
        super(new Formatter().format("%s at eof", msg).toString());
        this.message = msg;
    }

    public InvalidSyntaxException(String msg, long line, long row) {
        super(new Formatter().format("%s at %d:%d", msg, line, row).toString());
        this.message = msg;
        this.line = line;
        this.row = row;
    }

    public String getRawMessage() {
        return message;
    }

    public long getLine() {
        return line;
    }

    public long getRow() {
        return row;
    }
}
