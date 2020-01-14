package springbootelasticsearchtemplate.springbootelasticsearchtemplate.kafka.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

/**
 * 消息生产者的第一个示例
 * @author zifangsky
 */
@Component("simpleProducer")
public class SimpleProducer {
	private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
	
	@Autowired
	private KafkaTemplate<Object, Object> kafkaTemplate;
	
	/**
	 * 使用KafkaTemplate向Kafka推送数据
	 * @param topicName topic
	 * @param data
	 */
	public void sendMessage(String topicName,String data){
		logger.info(MessageFormat.format("开始向Kafka推送数据：{0}", data));
		
		try {
			kafkaTemplate.send(topicName, data);
			logger.info("推送数据成功！");
		} catch (Exception e) {
			logger.error(MessageFormat.format("推送数据出错，topic:{0},data:{1}"
					,topicName,data));
		}
	}

	/**
	 * 使用KafkaTemplate向Kafka推送数据
	 * @param topicName topic
	 * @param data
	 */
	public void sendObjectMessage(String topicName,Object data){
		logger.info(MessageFormat.format("开始向Kafka推送数据：{0}", data));
		
		try {
			kafkaTemplate.send(topicName, data);
			logger.info("推送数据成功！");
		} catch (Exception e) {
			logger.error(MessageFormat.format("推送数据出错，topic:{0},data:{1}"
					,topicName,data));
		}
	}
	
}
