package com.robustdb.server.model.metadata;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter
@Getter
@Builder
@ToString
public class IndexDef {
    private List<ColumnDef> indexColumns;
    private boolean isUnique;
}
