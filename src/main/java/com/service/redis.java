package com.service;

import redis.clients.jedis.Jedis;

public class redis {
    static public void main(String[] args){
        Jedis jedis =new Jedis("localhost");
        System.out.println("OK");
        System.out.println(jedis.ping());

    }
}
