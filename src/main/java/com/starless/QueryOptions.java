package com.starless;

import java.util.ArrayList;
import java.util.List;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class QueryOptions {
    private String type;
    @Builder.Default
    private List<Object> bind = new ArrayList<>();
}
