package com.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class score implements Serializable {
    private Integer userId;
    private Integer movieId;
    private Double observe;
    private Double predict;
}
