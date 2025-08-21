package com.example.flinkreplication;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Slf4j
@RequiredArgsConstructor
@EnableJpaRepositories
@SpringBootApplication
@EnableScheduling
public class FlinkReplicationApplication {
//    private final SvoiCustomLogger svoiCustomLogger;
//    private final LogPartitionRepository logPartitionRepository;
//    private final LogRepository logRepository;
//    private final ConfigurableEnvironment configurableEnvironment;
//    private static ConfigurableApplicationContext applicationContext;

//    @PostConstruct
//    public void startupApplication() {
//        logPartitionRepository.createTodayPartition();
//        svoiCustomLogger.send("startService", "Start Service", "Started service", SvoiSeverityEnum.ONE);
//        String props = Utils.getSources(configurableEnvironment.getPropertySources());
//        String propsHash = Utils.getHash(props, "SHA-256");
//        String localHostName = "";
//        try {
//            localHostName = InetAddress.getLocalHost().getHostName();
//        } catch (UnknownHostException e) {
//            localHostName = InetAddress.getLoopbackAddress().getHostName();
//        }
//        Log logEntity = logRepository.findLatestByType("checkConfig", localHostName);
//        if (logEntity == null) {
//            svoiCustomLogger.send("checkConfig", "Check Config", propsHash, SvoiSeverityEnum.ONE);
//        } else {
//            String prevHash = StringUtils.trim(StringUtils.substringBetween(logEntity.getLog(), "msg=", "deviceProcessName="));
//            if (!StringUtils.equals(prevHash, propsHash)) {
//                svoiCustomLogger.send("checkConfig", "Check Config", propsHash, SvoiSeverityEnum.ONE);
//            }
//        }
//    }

    public static void main(String[] args) {
         SpringApplication.run(FlinkReplicationApplication.class, args);
    }
//    @PreDestroy
//    public void shutdownApplication() {
//        svoiCustomLogger.send("stopService", "Stop Service", "Stopped service", SvoiSeverityEnum.ONE);
//    }
//    public static void restart() {
//        ApplicationArguments args = applicationContext.getBean(ApplicationArguments.class);
//        Thread thread = new Thread(() -> {
//            applicationContext.close();
//            applicationContext = SpringApplication.run(FlinkReplicationApplication.class, args.getSourceArgs());
//        });
//        thread.setDaemon(false);
//        thread.start();
//    }
}