package com.robustdb.server.model.metadata;

import com.robustdb.server.enums.ConstraintType;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter
@Getter
@Builder
@ToString
public class ColumnDef {
    private String dataType;
    private String table;
    private String length;
    private String name;
    private List<ConstraintType> constraintTypes;
    private String fullName;
}
