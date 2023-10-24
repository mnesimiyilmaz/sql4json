package io.github.mnesimiyilmaz.sql4json.dataclasses;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author mnesimiyilmaz
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SomeLoginData {
    private String    data;
    private Integer[] intData;
}
