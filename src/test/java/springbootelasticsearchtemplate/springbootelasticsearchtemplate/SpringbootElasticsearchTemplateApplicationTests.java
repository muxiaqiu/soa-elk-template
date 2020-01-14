package springbootelasticsearchtemplate.springbootelasticsearchtemplate;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import springbootelasticsearchtemplate.springbootelasticsearchtemplate.service.ElasticsearchService;

import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootElasticsearchTemplateApplicationTests {

	@Autowired
	ElasticsearchService elasticsearchService;

	@Test
	public void contextLoads() {

		List<Map> mapList = Lists.newArrayList();
		Map<String,Object> objectMap = Maps.newHashMap();
		objectMap.put("@timestamp","2020-01-14T13:55:35.748+08:00");
		objectMap.put("@version","1");
		objectMap.put("message","Starting DataSourceTest on DESKTOP-423MI6P with PID 2624 (started by qmx in D:\\caifuyun_project\\sms\\qcloud\\tal-pac-service\\micro-pac-restapi)");
		objectMap.put("logger_name","com.tal.pac.mapper.DataSourceTest");
		objectMap.put("thread_name","main");
		objectMap.put("level","INFO");
		objectMap.put("level_value","20000");
		objectMap.put("HOSTNAME","DESKTOP-423MI6P");
		objectMap.put("appName","tal-pac");

		Map<String,Object> objectMap1 = Maps.newHashMap();
		objectMap1.put("@timestamp","2020-01-14T13:55:35.748+08:00");
		objectMap1.put("@version","1");
		objectMap1.put("message","Starting DataSourceTest on DESKTOP-423MI6P with PID 2624 (started by qmx in D:\\caifuyun_project\\sms\\qcloud\\tal-pac-service\\micro-pac-restapi)");
		objectMap1.put("logger_name","com.tal.pac.mapper.DataSourceTest");
		objectMap1.put("thread_name","main");
		objectMap1.put("level","INFO");
		objectMap1.put("level_value","20000");
		objectMap1.put("HOSTNAME","DESKTOP-423MI6P");
		objectMap1.put("appName","tal-pac");

		mapList.add(objectMap);
		mapList.add(objectMap1);
		elasticsearchService.insertBulkAsync("data_linke1","unkown_1",mapList);
	}

}
