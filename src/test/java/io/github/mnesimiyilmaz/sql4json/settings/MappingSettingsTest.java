// SPDX-License-Identifier: Apache-2.0
package io.github.mnesimiyilmaz.sql4json.settings;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class MappingSettingsTest {

    @Test
    void when_defaults_then_policy_is_ignore() {
        assertEquals(MissingFieldPolicy.IGNORE, MappingSettings.defaults().missingFieldPolicy());
    }

    @Test
    void when_builder_sets_policy_fail_then_built_value_reflects_it() {
        MappingSettings s = MappingSettings.builder()
                .missingFieldPolicy(MissingFieldPolicy.FAIL)
                .build();
        assertEquals(MissingFieldPolicy.FAIL, s.missingFieldPolicy());
    }

    @Test
    void when_builder_rejects_null_policy() {
        assertThrows(NullPointerException.class, () -> MappingSettings.builder().missingFieldPolicy(null));
    }

    @Test
    void when_mapping_subsection_is_accessible_from_sql4json_settings() {
        Sql4jsonSettings s = Sql4jsonSettings.builder()
                .mapping(m -> m.missingFieldPolicy(MissingFieldPolicy.FAIL))
                .build();
        assertEquals(MissingFieldPolicy.FAIL, s.mapping().missingFieldPolicy());
    }

    @Test
    void when_sql4json_defaults_then_mapping_is_defaults() {
        assertSame(MappingSettings.defaults(), Sql4jsonSettings.defaults().mapping());
    }
}
