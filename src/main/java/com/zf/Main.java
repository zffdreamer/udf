package com.zf;

import org.apache.hadoop.io.Text;

/**
 * @author 郑锋
 * @version 1.0
 */
public class Main {
    public static void main(String[] args) throws Exception {
        GetWorkdays getWorkdays = new GetWorkdays();
        //String date1 = "20/03/2023";
        //String date2 = "10/04/2023";
        String date1 = "2023-04-01";
        String date2 = "2023-04-13";
        System.out.println(getWorkdays.evaluate(new Text(date1), new Text(date2)));
    }
}