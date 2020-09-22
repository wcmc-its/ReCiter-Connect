package reciter.connect.main;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.net.ssl.SSLException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import org.vivoweb.harvester.ingest.AcademicFetchFromED;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import lombok.extern.slf4j.Slf4j;
import reactor.netty.http.client.HttpClient;
import reciter.connect.api.client.ReCiterClient;
import reciter.connect.api.client.model.ArticleRetrievalModel;
import reciter.connect.api.client.model.exception.ApiException;
import reciter.connect.database.ldap.LDAPConnectionFactory;
import reciter.connect.database.mssql.MssqlConnectionFactory;
import reciter.connect.database.mysql.MysqlConnectionFactory;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
import reciter.connect.vivo.api.client.VivoClient;
import reciter.connect.vivo.sdb.publications.service.VivoPublicationsService;
import reciter.connect.vivo.sdb.query.SDBQueryInterface;

@SpringBootApplication
@PropertySource("classpath:application.properties")
@ComponentScan({ "reciter.connect", "org.vivoweb.harvester" })
@Slf4j
public class Application implements ApplicationRunner {

    private WebClient webClient;

    @Value("${reciter.api.base.url}")
    private String reciterBaseUrl;

    private String consumerApiKey = System.getenv("CONSUMER_API_KEY");

    @Autowired
    private ApplicationContext context;

    @Autowired
    private VivoPublicationsService vivoPublicationsService;

    @Autowired
    private VivoClient vivoClient;

    @Bean
    public WebClient getWebClient() {
        try {
            SslContext sslContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE)
                    .build();
            HttpClient httpClient = HttpClient.create().secure(t -> t.sslContext(sslContext));
            this.webClient = WebClient.builder().baseUrl(this.reciterBaseUrl)
                    .clientConnector(new ReactorClientHttpConnector(httpClient))
                    .exchangeStrategies(ExchangeStrategies.builder()
                            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(25 * 1024 * 1024)).build())
                    .defaultHeader("api-key", consumerApiKey).build();
        } catch (SSLException e) {
            log.error("SSLException", e);
        }
        return this.webClient;
    }

    public static void main(String[] args) {
        // ConfigurableApplicationContext context =
        // SpringApplication.run(Application.class, args);
        new SpringApplicationBuilder(Application.class).web(WebApplicationType.NONE).run(args);
        // ApplicationContext context = new
        // AnnotationConfigApplicationContext(Application.class);

    }

    /* @PostConstruct
    public void run() {
        
    }*/
 
    @Override
    public void run(ApplicationArguments args) throws Exception {
        LDAPConnectionFactory ldapConnectionFactory = context.getBean(LDAPConnectionFactory.class);
        MysqlConnectionFactory mysqlConnectionFactory = context.getBean(MysqlConnectionFactory.class);
        MssqlConnectionFactory mssqlConnectionFactory = context.getBean(MssqlConnectionFactory.class);
        JenaConnectionFactory jenaConnectionFactory = context.getBean(JenaConnectionFactory.class);
        SDBQueryInterface sdbQueryInterface = context.getBean(SDBQueryInterface.class);
        AcademicFetchFromED academicFetchFromED = context.getBean(AcademicFetchFromED.class);


        /* try {
            academicFetchFromED.execute();
        } catch (IOException e) {
            log.error("IOException", e);
        }

        ReCiterClient reCiterClient = context.getBean(ReCiterClient.class);
        // ArticleRetrievalModel pubs =
        // reCiterClient.getPublicationsByUid("paa2013").block();
        
         try{ List<ArticleRetrievalModel> allPubs = reCiterClient
            .getPublicationsByGroup(Arrays.asList("rak2007"))
            .collectList()
            .block();
            log.info("pubs" + allPubs.size()); 
            vivoPublicationsService.importPublications(allPubs);
        }   catch(Exception e) {
         log.error("Exception", e); 
        } */
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT DATA { \n");
        sb.append("GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-2> { \n");
        sb.append("<http://test.domain/ns#book1> <http://purl.org/dc/elements/1.1/title> \"DataStructure and algorithm\" . \n");
        sb.append("}}");

        log.info(vivoClient.vivoUpdateApi(sb.toString()));
        
         
        if (ldapConnectionFactory != null)
            ldapConnectionFactory.destroyConnectionPool();

        if (mysqlConnectionFactory != null)
            mysqlConnectionFactory.destroyConnectionPool();

        if (mssqlConnectionFactory != null)
            mssqlConnectionFactory.destroyConnectionPool();

        if (jenaConnectionFactory != null)
            jenaConnectionFactory.destroyConnectionPool();
    }
}


