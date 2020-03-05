package reciter.connect.main;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
@PropertySource("classpath:application.properties")
@ComponentScan("edu.wcmc.cbt")
public class Application {
	
}


