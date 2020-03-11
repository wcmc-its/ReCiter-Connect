package reciter.connect.main;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import reciter.connect.database.ldap.LDAPConnectionFactory;
import reciter.connect.database.mssql.MssqlConnectionFactory;
import reciter.connect.database.mysql.MysqlConnectionFactory;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;

@SpringBootApplication
@PropertySource("classpath:application.properties")
@ComponentScan({"reciter.connect", "org.vivoweb.harvester"})
public class Application {
	public static void main(String[] args) {
        //ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
        new SpringApplicationBuilder(Application.class).web(WebApplicationType.NONE).run(args);
        ApplicationContext context = new AnnotationConfigApplicationContext(Application.class);
        LDAPConnectionFactory ldapConnectionFactory = context.getBean(LDAPConnectionFactory.class);
		MysqlConnectionFactory mysqlConnectionFactory = context.getBean(MysqlConnectionFactory.class);
        MssqlConnectionFactory mssqlConnectionFactory = context.getBean(MssqlConnectionFactory.class);
        JenaConnectionFactory jenaConnectionFactory = context.getBean(JenaConnectionFactory.class);

        if(ldapConnectionFactory != null)
            ldapConnectionFactory.destroyConnectionPool();

        if(mysqlConnectionFactory != null)
            mysqlConnectionFactory.destroyConnectionPool();
        
        if(mssqlConnectionFactory != null)
            mssqlConnectionFactory.destroyConnectionPool();

        if(jenaConnectionFactory != null)
            jenaConnectionFactory.destroyConnectionPool();
        
        ((ConfigurableApplicationContext)context).close();
    }
}


