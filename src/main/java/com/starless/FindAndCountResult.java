package com.starless;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FindAndCountResult {
    @Builder.Default
    private int count = 0;
    @Builder.Default
    private List<Map<String, Object>> rows = new ArrayList<>();
}
