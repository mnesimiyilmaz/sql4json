package io.github.mnesimiyilmaz.sql4json.types;

/**
 * Represents SQL NULL as a sealed record type.
 * <p>Use {@link #INSTANCE} — do not call {@code new SqlNull()} directly.
 * The singleton property is a convention; the Java record type system does not
 * enforce private construction.
 */
public record SqlNull() implements SqlValue {
    /**
     * The singleton {@code NULL} instance. Prefer this over the record constructor.
     */
    public static final SqlNull INSTANCE = new SqlNull();

    @Override
    public Object rawValue() {
        return null;
    }
}
