package reciter.connect.main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.net.ssl.SSLException;

import com.google.common.collect.Lists;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.util.StopWatch;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import org.vivoweb.harvester.ingest.AcademicFetchFromED;
import org.vivoweb.harvester.ingest.AppointmentsFetchFromED;
import org.vivoweb.harvester.ingest.EdDataInterface;
import org.vivoweb.harvester.ingest.GrantsFetchFromED;
import org.vivoweb.harvester.operations.DeleteProfile;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import lombok.extern.slf4j.Slf4j;
import reactor.netty.http.client.HttpClient;
import reciter.connect.api.client.ReCiterClient;
import reciter.connect.api.client.model.ArticleRetrievalModel;
import reciter.connect.beans.vivo.PeopleBean;
import reciter.connect.database.ldap.LDAPConnectionFactory;
import reciter.connect.database.mssql.MssqlConnectionFactory;
import reciter.connect.database.mysql.MysqlConnectionFactory;
import reciter.connect.database.mysql.jena.JenaConnectionFactory;
import reciter.connect.database.tdb.TDBConnectionFactory;
import reciter.connect.vivo.api.client.VivoClient;
import reciter.connect.vivo.sdb.publications.service.VivoPublicationsService;

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

    @Autowired
    private EdDataInterface edDataInterface;

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
        new SpringApplicationBuilder(Application.class).web(WebApplicationType.NONE).run(args);
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
        //TDBConnectionFactory tdbConnectionFactory = context.getBean(TDBConnectionFactory.class);
        AcademicFetchFromED academicFetchFromED = context.getBean(AcademicFetchFromED.class);
        GrantsFetchFromED grantsFetchFromED = context.getBean(GrantsFetchFromED.class);
        AppointmentsFetchFromED appointmentsFetchFromED = context.getBean(AppointmentsFetchFromED.class);
        ReCiterClient reCiterClient = context.getBean(ReCiterClient.class);
        DeleteProfile deleteProfile = context.getBean(DeleteProfile.class);
        mssqlConnectionFactory.createC3PODatasourceForASMS();
        mssqlConnectionFactory.createC3PODatasourceForInfoEd();
        Connection asmsCon = null;
        Connection infoEdCon = null;
        try {
            asmsCon = MssqlConnectionFactory.getASMSDataSource().getConnection();
            infoEdCon = MssqlConnectionFactory.getInfoedDataSource().getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}

        academicFetchFromED.getCOIData();

        ExecutorService executor = Executors.newFixedThreadPool(25);

        try {
            List<PeopleBean> people = academicFetchFromED.getActivePeopleFromED();
            //deleteProfile.execute();
            List<List<PeopleBean>> peopleSubSets = Lists.partition(people, 10);
            Iterator<List<PeopleBean>> subSetsIteratorPeople = peopleSubSets.iterator();
            while (subSetsIteratorPeople.hasNext()) {
                List<PeopleBean> subsetPeoples = subSetsIteratorPeople.next();
                List<Callable<String>> callables = new ArrayList<>();
                for(PeopleBean peopleSubset: subsetPeoples) {
                    callables.add(academicFetchFromED.getCallable(Arrays.asList(peopleSubset)));
                }
                log.info("People fetch will run for " + subsetPeoples.toString());
                try {
                    executor.invokeAll(callables)
                    .stream()
                    .map(future -> {
                        try {
                            return future.get();
                        }
                        catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }).forEach(System.out::println);
                } catch (InterruptedException e) {
                    log.error("Unable to invoke callable.", e);
                }
                callables.clear();
            }
            List<String> peopleCwids = people.stream().map(PeopleBean::getCwid).collect(Collectors.toList());
            
            List<List<String>> peopleCwidsSubSets = Lists.partition(peopleCwids, 5);
            Iterator<List<String>> subSetsIteratorPeopleCwids = peopleCwidsSubSets.iterator();
		    while (subSetsIteratorPeopleCwids.hasNext()) {
                List<String> subsetPeoples = subSetsIteratorPeopleCwids.next();
                List<Callable<String>> callables = new ArrayList<>();
                for(String cwid: subsetPeoples) {
                    callables.add(appointmentsFetchFromED.getCallable(Arrays.asList(cwid), asmsCon));
                    callables.add(grantsFetchFromED.getCallable(Arrays.asList(cwid), asmsCon, infoEdCon));
                }
                log.info("Appointment and Grants fetch will run for " + subsetPeoples.toString());
                try {
                    executor.invokeAll(callables)
                    .stream()
                    .map(future -> {
                        try {
                            return future.get();
                        }
                        catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }).forEach(System.out::println);
                } catch (InterruptedException e) {
                    log.error("Unable to invoke callable.", e);
                }
                callables.clear();
            }

            //Close Connections
            if (ldapConnectionFactory != null)
            ldapConnectionFactory.destroyConnectionPool();

            if (mysqlConnectionFactory != null)
                mysqlConnectionFactory.destroyConnectionPool();

            if(asmsCon != null && infoEdCon != null) {
                try {
                    asmsCon.close();
                    infoEdCon.close();
                } catch (SQLException e) {
                    log.error("SQLException", e);
                }
            }

            MssqlConnectionFactory.dataSourceCleanup(MssqlConnectionFactory.getASMSDataSource());
            MssqlConnectionFactory.dataSourceCleanup(MssqlConnectionFactory.getInfoedDataSource());

            for(List<String> subsetPeoples: peopleCwidsSubSets) {
                List<Callable<String>> callables = new ArrayList<>();
                try{
                    StopWatch stopWatch = new StopWatch("Getting Publications from ReCiter");
                    stopWatch.start("Getting Publications from ReCiter");
                    log.info("Getting publications for group : " + subsetPeoples.toString());
                    List<ArticleRetrievalModel> allPubs = reCiterClient
                                .getPublicationsByGroup(subsetPeoples)
                                .onErrorReturn(new ArticleRetrievalModel())
                                .collectList()
                                .block();
                    stopWatch.stop();
                    if(!allPubs.isEmpty()) {
                        log.info("Publications fetch Time taken: " + stopWatch.getTotalTimeSeconds() + "s");
                        callables.add(vivoPublicationsService.getCallable(allPubs));
                    }
                } catch(Exception e) {
                    log.error("Bulk Retrieval Exception", e);
                }
                if(!callables.isEmpty()) {
                    log.info("Publications fetch will run for " + subsetPeoples.toString());
                    try {
                        executor.invokeAll(callables)
                        .stream()
                        .map(future -> {
                            try {
                                return future.get();
                            }
                            catch (Exception e) {
                                throw new IllegalStateException(e);
                            }
                        }).forEach(System.out::println);
                    } catch (InterruptedException e) {
                        log.error("Unable to invoke callable.", e);
                    }
                }
                callables.clear();
            }
            

        } catch (Exception e) {
            log.error("Exception in application", e);
        }

        if (jenaConnectionFactory != null)
            jenaConnectionFactory.destroyConnectionPool();

        System.exit(0);
    }

    public static <T> CompletableFuture<List<T>> allOf(List<CompletableFuture<T>> futuresList) {
	    CompletableFuture<Void> allFuturesResult =
	    	    CompletableFuture.allOf(futuresList.toArray(new CompletableFuture[futuresList.size()]));
	    	    return allFuturesResult.thenApply(v ->
	    	    	futuresList.stream().map(CompletableFuture::join).collect(Collectors.toList())
	    	    );
	 }
}


