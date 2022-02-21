package com.atguigu.bigdata.spark.test;
import java.util.*;
public class Test {
    public static void main(String[] args) {

        User user = new User();
        user.name = "zhangsan";

        //List<User> userList = new ArrayList<User>();
        //userList.clone();
        ArrayList<User> userList1 = new ArrayList<User>();
        userList1.add(user);

        ArrayList<User> userList2 = (ArrayList<User>)userList1.clone();

        System.out.println(userList1 == userList2);
        final User user1 = userList2.get(0);
        user1.name = "lisi";

        System.out.println(userList1);
        System.out.println(userList2);

    }
}
class User {
    public String name;

    @Override
    public String toString() {
        return "User["+name+"]";
    }
}
