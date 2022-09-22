package com.starless;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FindOptions {
    @Builder.Default
    private Map<String, Object> where = new HashMap<>();
    @Builder.Default
    private Map<String, Object> defaults = new HashMap<>();
    @Builder.Default
    private List<String> select = new ArrayList<>();
    @Builder.Default
    private boolean truncate = false;
    @Builder.Default
    private Map<String, String> order = new HashMap<>();
    @Builder.Default
    private List<String> group = new ArrayList<>();
    @Builder.Default
    private int limit = 0;
    @Builder.Default
    private int offset = 0;
}
