package com.lyg.data_collect.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FirstController {

    @RequestMapping("/testDemo")
    public String test(String name,int age){
        System.out.println("name:"+name+",age:"+age);
        return "success";
    }
}
