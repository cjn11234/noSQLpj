package com.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import scala.Serializable;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor

public class Recommendation implements Comparable<Recommendation> , Serializable {

    private Integer movieId;
    private Double avg;



    @Override
    public int compareTo(Recommendation o) {
        Double i=o.avg - this.getAvg();
        if(o.avg>this.getAvg())
            return 1;
        else if(o.avg<this.getAvg()){
            return -1;
        }
        else
            return 0;

    }
}
