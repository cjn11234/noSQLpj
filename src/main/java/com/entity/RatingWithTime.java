package com.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class RatingWithTime implements Comparable<RatingWithTime> , Serializable {
    private Integer movieId;
    private Double rating;
    private Long timestamp;
    @Override
    public int compareTo(RatingWithTime o) {
        if(o.getTimestamp()>this.getTimestamp())
            return 1;
        else if(o.getTimestamp()<this.getTimestamp()){
            return -1;
        }
        else
            return 0;
    }
}
