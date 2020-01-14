package springbootelasticsearchtemplate.springbootelasticsearchtemplate.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

@Configuration
public class WebMvcConfig extends WebMvcConfigurerAdapter {

	@Override
	public void addViewControllers(ViewControllerRegistry registry) {
		//这里会按照Thymeleaf模板解析，默认是classpath:/templates/ 目录
		registry.addViewController("/index2").setViewName("index2");
	}

}
