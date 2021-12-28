package com.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class tag {
    private Integer userId;
    private Integer movieId;
    private String tag;
    private long timestamp;
}
