package com.robustdb.server.model.metadata;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@Builder
@ToString
public class ColumnDef {
    private String dataType;
    private String table;
    private String length;
    private String name;
    private String fullName;
}
