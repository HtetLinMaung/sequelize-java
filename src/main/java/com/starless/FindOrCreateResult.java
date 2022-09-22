package com.starless;

import java.util.Map;
import java.util.Optional;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FindOrCreateResult {
    private Optional<Map<String, Object>> oldData;
    private Optional<Map<String, Object>> newData;
}
