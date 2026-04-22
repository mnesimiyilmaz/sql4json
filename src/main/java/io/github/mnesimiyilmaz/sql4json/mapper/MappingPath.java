package io.github.mnesimiyilmaz.sql4json.mapper;

/**
 * Immutable, copy-on-descend path accumulator used for error messages
 * (e.g. {@code $.orders[2].amount}). Each {@code field} or {@code index}
 * call returns a new node, so recursion can share prefix nodes without
 * mutating shared state.
 */
final class MappingPath {

    private static final MappingPath ROOT = new MappingPath(null, "$");

    private final MappingPath parent;
    private final String      segment;

    private MappingPath(MappingPath parent, String segment) {
        this.parent = parent;
        this.segment = segment;
    }

    static MappingPath root() {
        return ROOT;
    }

    MappingPath field(String name) {
        return new MappingPath(this, "." + name);
    }

    MappingPath index(int i) {
        return new MappingPath(this, "[" + i + "]");
    }

    MappingPath key(String jsonKey) {
        return new MappingPath(this, "." + jsonKey);
    }

    @Override
    public String toString() {
        if (parent == null) return segment;
        StringBuilder sb = new StringBuilder();
        build(sb);
        return sb.toString();
    }

    private void build(StringBuilder sb) {
        if (parent != null) parent.build(sb);
        sb.append(segment);
    }
}
