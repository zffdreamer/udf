package com.zf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * @author 郑锋
 * @version 1.0
 */
public class GetWorkdays extends UDF {
    public LongWritable evaluate(Text dateString, Text dateString1) throws ParseException { //takes the two columns as inputs
        //SimpleDateFormat date = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
/*  String date1 = "20/07/2016";
    String date2 = "28/07/2016";
*/  int count=0;

        List<Date> dates = new ArrayList<Date>();

        Date  startDate = (Date)date.parse(dateString.toString());
        Date  endDate = (Date)date.parse(dateString1.toString());
        long interval = 24*1000 * 60 * 60; // 1 hour in millis
        long endTime =endDate.getTime() ; // create your endtime here, possibly using Calendar or Date
        long curTime = startDate.getTime();
        while (curTime <= endTime) {
            dates.add(new Date(curTime));
            curTime += interval;
        }
        for(int i=0;i<dates.size();i++){
            Date lDate =(Date)dates.get(i);
            if(lDate.getDay()==0||lDate.getDay()==6){
                count+=1;  //counts the number of sundays in between
            }
        }

        long days_diff = (endDate.getTime()-startDate.getTime())/(24 * 60 * 60 * 1000)-count; //displays the days difference excluding sundays
        return new LongWritable(days_diff);

    }
}
