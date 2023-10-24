package io.github.mnesimiyilmaz.sql4json.dataclasses;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author mnesimiyilmaz
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Account {
    private String             username;
    private String[]           nicknames;
    private Boolean            active;
    private List<LoginHistory> loginHistoryList;
}