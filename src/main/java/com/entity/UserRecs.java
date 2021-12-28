package com.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import scala.collection.Seq;
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class UserRecs {
    Integer userId;
    Recommendation[] recs;
}
