package io.github.mnesimiyilmaz.sql4json.dataclasses;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * @author mnesimiyilmaz
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    private String        name;
    private String        surname;
    private Integer       age;
    private Account       account;
    private LocalDateTime lastLoginDateTime;
    private LocalDate     dateOfBirth;
}
