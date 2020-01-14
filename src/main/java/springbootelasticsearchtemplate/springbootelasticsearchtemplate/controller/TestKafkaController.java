package springbootelasticsearchtemplate.springbootelasticsearchtemplate.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import springbootelasticsearchtemplate.springbootelasticsearchtemplate.kafka.producer.SimpleProducer;
import springbootelasticsearchtemplate.springbootelasticsearchtemplate.pojo.DemoObj;

import javax.annotation.Resource;

@RestController
@RequestMapping("/kafka")
public class TestKafkaController {
	@Resource(name="simpleProducer")
	private SimpleProducer producer;
	
	private final String TOPIC = "first"; //测试使用topic
	private final String TOPIC2 = "topic-test2"; //测试使用topic
	
	@RequestMapping("/send")
	public String send(String data){
		producer.sendMessage(TOPIC, data);
		
		return "发送数据【" + data + "】成功！";
	}
	
	@RequestMapping("/send2")
	public String send2(DemoObj demoObj){
		producer.sendObjectMessage(TOPIC2, demoObj);
		
		return "发送数据【" + demoObj + "】成功！";
	}
	
}
