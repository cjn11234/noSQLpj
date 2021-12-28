package com.entity;

public class juzhen {
    private Integer filename1;
    private Integer filename2;

    juzhen() {

    }

    public juzhen(Integer filename1, Integer filename2) {

        this.filename1 = filename1;
        this.filename2 = filename2;
    }

    public Integer getName1() {
        return filename1;
    }

    public void setName1(Integer filename1) {
        this.filename1 = filename1;
    }

    public Integer getName2() {
        return filename2;
    }

    public void setName2(Integer filename2) {
        this.filename2 = filename2;
    }
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof juzhen) {
            juzhen p = (juzhen) obj;
            return this.filename1.equals(p.filename1) && this.filename2.equals(p.filename2);
        } else {
            return false;
        }
    }



}



