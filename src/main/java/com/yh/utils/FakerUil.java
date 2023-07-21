package com.yh.utils;

import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Random;

public class FakerUil {
    private static Logger logger = LoggerFactory.getLogger(FakerUil.class);

    /**
     * language
     */
    private static final String LANGUAGE = "zh-CN";

    /**
     * 创建一个 Faker 对象
     * Faker 方法调用时会随机访问底层数据，因此不需要每次都创建一个实例
     * Java Faker 通过 /src/main/resources 中的 .yml 获取数据
     * <p>
     * language - default EN  英文
     * de 德语
     * </p>
     */
    private static Faker faker = new Faker(new Locale(FakerUil.LANGUAGE));

    // ====================== random num ===================================

    public static Integer randomNum() {
        //logger.info("min : [{}], max : [{}]", Long.MIN_VALUE, Long.MAX_VALUE);
        return randomNum(Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public static Integer randomNum(Integer min, Integer max) {
        return faker.number().numberBetween(min, max);
    }

    // ====================== random name ===================================

    public static String fullName() {
        return faker.name().fullName();
    }

    public static String name() {
        return faker.name().name();
    }

    public static String appName() {
        return faker.app().name();
    }

    public static String food() {
        return faker.food().ingredient();
    }

    public static String title() {
        return faker.name().title();
    }

    public static String streetAddress() {
        return faker.address().streetAddress();
    }

    public static String cityName() {
        return faker.address().cityName();
    }

    public static String country() {
        return faker.address().country();
    }

    public static String phoneNumber(){
        return faker.phoneNumber().cellPhone();
    }

    public static String randomTime(String startTime,String endTime) throws Exception {
        //结束开始时间
        long startT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(startTime).getTime();;
        //设置结束时间
        long endT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(endTime).getTime();
        // 设置一个随机数
        Random random = new Random();
        //产生long类型指定范围随机数
        long randomDate = startT + (long) (random.nextFloat() * (endT - startT + 1));
        // 转化为日期格式
        String format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(randomDate));
        return String.valueOf(randomDate);
    }

    public static void main(String[] args) throws Exception {


        Date parse = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2021-12-01 00:00:00");
        System.out.println(parse);


        for (int i = 0; i < 10; i++) {
            System.out.println(randomTime("2021-12-01 00:00:00","2021-12-10 23:59:59"));
        }



//        System.out.println(FakerUil.randomNum());
//        System.out.println(FakerUil.randomNum(1,20));
//        System.out.println(FakerUil.fullName());
//        System.out.println(FakerUil.name());
//        System.out.println(FakerUil.appName());
//        System.out.println(FakerUil.food());
//        System.out.println(FakerUil.title());
//        System.out.println(FakerUil.streetAddress());
//        System.out.println(FakerUil.cityName());
//        System.out.println(FakerUil.country());


    }

}
