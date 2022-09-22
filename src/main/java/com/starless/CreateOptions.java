package com.starless;

import java.util.ArrayList;
import java.util.List;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CreateOptions {
    @Builder.Default
    private List<String> fields = new ArrayList<>();
    @Builder.Default
    private boolean unorder = true;
}
