package io.github.mnesimiyilmaz.sql4json;

final class TestDataGenerator {

    private static final String[] NAMES = {
            "Alice", "Bob", "Carol", "David", "Eve", "Frank",
            "Grace", "Henry", "Ivy", "Jack"
    };

    private static final String[] DEPARTMENTS = {
            "Engineering", "Marketing", "Sales", "HR", "Finance",
            "Legal", "Support", "Product", "Design", "Operations"
    };

    private TestDataGenerator() {
    }

    static String generateLargeArray(int rowCount, int fieldCount) {
        var sb = new StringBuilder(rowCount * 80);
        sb.append('[');
        for (int i = 0; i < rowCount; i++) {
            if (i > 0) sb.append(',');
            sb.append('{');
            sb.append("\"id\":").append(i);
            sb.append(",\"name\":\"").append(NAMES[i % NAMES.length]).append(i).append('"');
            sb.append(",\"age\":").append(i % 100);
            sb.append(",\"dept\":\"").append(DEPARTMENTS[i % DEPARTMENTS.length]).append('"');
            for (int f = 4; f < fieldCount; f++) {
                sb.append(",\"col_").append(f).append("\":").append(i + f);
            }
            sb.append('}');
        }
        sb.append(']');
        return sb.toString();
    }

    static String generateWideArray(int rowCount, int fieldCount) {
        var sb = new StringBuilder(rowCount * fieldCount * 12);
        sb.append('[');
        for (int i = 0; i < rowCount; i++) {
            if (i > 0) sb.append(',');
            sb.append('{');
            for (int f = 0; f < fieldCount; f++) {
                if (f > 0) sb.append(',');
                sb.append("\"col_").append(f).append("\":").append(i + f);
            }
            sb.append('}');
        }
        sb.append(']');
        return sb.toString();
    }

    static String generateDeeplyNested(int depth, int leafCount) {
        var sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < leafCount; i++) {
            if (i > 0) sb.append(',');
            sb.append('{');
            var current = new StringBuilder();
            for (int d = 0; d < depth; d++) {
                current.append("\"l").append(d).append("\":{");
            }
            current.append("\"value\":").append(i);
            for (int d = 0; d < depth; d++) {
                current.append('}');
            }
            sb.append("\"data\":{").append(current).append('}');
            sb.append('}');
        }
        sb.append(']');
        return sb.toString();
    }
}
