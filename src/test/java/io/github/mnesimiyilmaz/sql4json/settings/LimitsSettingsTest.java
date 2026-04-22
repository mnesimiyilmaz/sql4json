package io.github.mnesimiyilmaz.sql4json.settings;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LimitsSettingsTest {

    @Test
    void when_defaults_then_max_parameters_is_1024() {
        assertEquals(1024, LimitsSettings.defaults().maxParameters());
    }

    @Test
    void when_builder_sets_max_parameters_then_value_reflected() {
        LimitsSettings l = LimitsSettings.builder().maxParameters(16).build();
        assertEquals(16, l.maxParameters());
    }

    @Test
    void when_builder_rejects_non_positive_max_parameters() {
        assertThrows(IllegalArgumentException.class,
                () -> LimitsSettings.builder().maxParameters(0));
        assertThrows(IllegalArgumentException.class,
                () -> LimitsSettings.builder().maxParameters(-1));
    }
}
