package com.example.influxdbdemo.controller;

import java.util.ArrayList;
import java.util.List;

import com.example.influxdbdemo.entity.Sensor;
import com.example.influxdbdemo.util.InfluxdbUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class IndexController {

    @Autowired
    private InfluxdbUtils influxdbUtils;
    
    /**
     * 插入单挑记录
     */
    @GetMapping("/index21")
    public void insert(){
//      Sensor sensor = Sensor.builder().deviceId("0002")
//              .A1(10).A2(10).temp(10L).voltage(10).build();
//      influxdbUtils.insertOne(sensor);
    }
    
    /**
     * 批量插入第一种方式
     */
    @GetMapping("/index22")
    public void batchInsert(){  
        List<Sensor> sensorList = new ArrayList<Sensor>();
        Sensor sensor = null;
//      for(int i=0; i<50; i++){
//          sensor = Sensor.builder().deviceId("000"+i)
//                  .A1(10).A2(10).temp(10).voltage(10).build();
//          sensorList.add(sensor);
//      }
        for(int i=0; i<50; i++){
            sensor = new Sensor();
            sensor.setA1(2);
            sensor.setA2(12);
            sensor.setTemp(9);
            sensor.setVoltage(12);
            sensor.setDeviceId("sensor4545-"+i);
            sensorList.add(sensor);
        }
        influxdbUtils.insertBatchByRecords(sensorList);
    }
    
    /**
     * 批量插入第二种方式
     */
    @GetMapping("/index23")
    public void batchInsert1()  throws Exception{
        List<Sensor> sensorList = new ArrayList<Sensor>();
        Sensor sensor = null;
//      for(int i=0; i<50; i++){
//          sensor = Sensor.builder().deviceId("000"+i)
//                  .A1(10).A2(10).temp(10).voltage(10).build();
//          sensorList.add(sensor);
//      }
        for (int i = 0; i < 10; i++) {
            for(int j=0; j<10; j++){
                sensor = new Sensor();
                sensor.setA1(2);
                sensor.setA2(12);
                sensor.setTemp(9);
                sensor.setVoltage(12);
                sensor.setDeviceId("sensor4545-"+j);
                sensorList.add(sensor);
            }
            influxdbUtils.insertBatchByPoints(sensorList);
            Thread.sleep(1000L);
        }


    }
        
    /**
     * 获取数据
     */
    @GetMapping("/datas2")
    public List<Object> datas(@RequestParam Integer page){
        int pageSize = 10;
        // InfluxDB支持分页查询,因此可以设置分页查询条件
        String pageQuery = " LIMIT " + pageSize + " OFFSET " + (page - 1) * pageSize;
        
        String queryCondition = "";  //查询条件暂且为空
        // 此处查询所有内容,如果
        String queryCmd = "SELECT * FROM sensor"
            // 查询指定设备下的日志信息
            // 要指定从 RetentionPolicyName.measurement中查询指定数据,默认的策略可以不加；
            // + 策略name + "." + measurement
            // 添加查询条件(注意查询条件选择tag值,选择field数值会严重拖慢查询速度)
            + queryCondition
            // 查询结果需要按照时间排序
            + " ORDER BY time DESC"
            // 添加分页查询条件
            + pageQuery;
        
        List<Object> sensorList = influxdbUtils.fetchRecords(queryCmd);
        log.info("query result => {}", sensorList);

        return sensorList;
    }
    
    /**
     * 获取数据
     */
    @GetMapping("/datas21")
    public List<Sensor> datas1(@RequestParam Integer page){
        int pageSize = 10;
        // InfluxDB支持分页查询,因此可以设置分页查询条件
        String pageQuery = " LIMIT " + pageSize + " OFFSET " + (page - 1) * pageSize;
        
        String queryCondition = "";  //查询条件暂且为空
        // 此处查询所有内容,如果
        String queryCmd = "SELECT * FROM sensor"
            // 查询指定设备下的日志信息
            // 要指定从 RetentionPolicyName.measurement中查询指定数据,默认的策略可以不加；
            // + 策略name + "." + measurement
            // 添加查询条件(注意查询条件选择tag值,选择field数值会严重拖慢查询速度)
            + queryCondition
            // 查询结果需要按照时间排序
            + " ORDER BY time DESC"
            // 添加分页查询条件
            + pageQuery;
        List<Sensor> sensorList = influxdbUtils.fetchResults(queryCmd, Sensor.class);
        //List<Sensor> sensorList = influxdbUtils.fetchResults("*", "sensor", Sensor.class);
        sensorList.forEach(sensor->{
            log.info("query sensor => {}", sensor);
        });

        return sensorList;
    }
}