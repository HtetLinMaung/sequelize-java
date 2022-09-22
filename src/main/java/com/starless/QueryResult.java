package com.starless;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class QueryResult {
    @Builder.Default
    private List<?> mapData = new ArrayList<>();
    @Builder.Default
    private List<Map<String, Object>> data = new ArrayList<>();
    @Builder.Default
    private int affectedRows = 0;
}
