package com.gpb.replication.postgres.log;

import com.gpb.replication.postgres.dto.ReplicationRequestDto;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.gpb.replication.postgres.logrepository.Log;
import com.gpb.replication.postgres.logrepository.LogRepository;
import com.gpb.replication.postgres.properties.LogsDatabaseProperties;
import com.gpb.replication.postgres.properties.SysProperties;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
@Slf4j
public class SvoiCustomLogger {
    private final SysProperties sysProperties;
    private final LogsDatabaseProperties logsDatabaseProperties;
    private final LogRepository logRepository;
    private final SvoiJournalFactory svoiJournalFactory = new SvoiJournalFactory();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");


    @Autowired
    public SvoiCustomLogger(SysProperties sysProperties,
                            LogsDatabaseProperties logsDatabaseProperties,
                            LogRepository logRepository) {
        this.sysProperties = sysProperties;
        this.logsDatabaseProperties = logsDatabaseProperties;
        this.logRepository = logRepository;
    }
    public void logConnectToSource(String sourceHost,
                                   int sourcePort,
                                   String dbType,
                                   String duser) {
        try {
            SvoiJournal journal = svoiJournalFactory.getJournalSource();
            String Dhost;
            String Dst;
            if (isIpAddress(sourceHost)) {
                Dst = sourceHost;
                Dhost = resolveToOpposite(sourceHost);
            } else {
                Dst = resolveToOpposite(sourceHost);
                Dhost = sourceHost;
            }
            journal.setDhost(Dhost);
            journal.setDst(Dst);
            journal.setDvchost(Dhost);
            journal.setDpt(sourcePort);
            journal.setDuser(duser);

            String message = String.format("connectTo%s dns=%s ip=%s port=%d",
                    dbType, Dhost, Dst, sourcePort);

            sendInternal("connectToSource",
                    "Database Connection",
                    message,
                    SvoiSeverityEnum.ONE,
                    journal);

        } catch (Exception e) {
            log.error("Ошибка при логировании подключения к источнику {}", dbType, e);
        }
    }

    public void logAuthError(String ip, String dns, int port, String dbType, String username, Exception e) {
        try {
            SvoiJournal journal = svoiJournalFactory.getJournalSource();
            String localHostName;
            String localHostAddress;
            try {
                localHostName = InetAddress.getLocalHost().getHostName();
                localHostAddress = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException ex) {
                localHostName = InetAddress.getLoopbackAddress().getHostName();
                localHostAddress = InetAddress.getLoopbackAddress().getHostAddress();
            }

            journal.setShost(localHostName);
            journal.setSrc(localHostAddress);
            journal.setSpt(0);

            String message = String.format(
                    "authError connectTo%s user=%s dns=%s ip=%s port=%d error=%s",
                    dbType,
                    username,
                    dns,
                    ip,
                    port,
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()
            );

            sendInternal(
                    "authError",
                    "Authorization Error",
                    message,
                    SvoiSeverityEnum.FIVE,
                    journal
            );

        } catch (Exception ex) {
            log.error("Ошибка при логировании ошибки авторизации", ex);
        }
    }


    public void logApiCall(HttpServletRequest request, String message, ReplicationRequestDto dto) {
        try {
            String clientIp = request.getRemoteAddr();
            String clientHost = request.getRemoteHost();
            int clientPort = request.getRemotePort();

            SvoiJournal journal = svoiJournalFactory.getJournalSource();
            journal.setShost(clientHost);
            journal.setSrc(clientIp);
            journal.setSpt(clientPort);

            String extendedMessage = message;
            if (dto != null && dto.getServiceName() != null) {
                extendedMessage = String.format("%s serviceName=%s", message, dto.getServiceName());
            }

            sendInternal(
                    "metadataSyncApi",
                    "Metadata Synchronization Request",
                    extendedMessage,
                    SvoiSeverityEnum.ONE,
                    journal
            );

        } catch (Exception e) {
            String contextInfo = (dto != null && dto.getServiceName() != null)
                    ? String.format(" Ошибка при логировании вызова API (serverName=%s)", dto.getServiceName())
                    : " Ошибка при логировании вызова API";
            log.error(contextInfo, e);
        }
    }

    public void send(String deviceEventClassID, String name, String message, SvoiSeverityEnum severity) {
        sendInternal(deviceEventClassID, name, message, severity, svoiJournalFactory.getJournalSource());
    }

    private void sendInternal(String deviceEventClassID, String name, String message,
                              SvoiSeverityEnum severity, SvoiJournal journal) {
        String localHostName;
        String localHostAddress;

        try {
            localHostName = InetAddress.getLocalHost().getHostName();
            localHostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            localHostName = InetAddress.getLoopbackAddress().getHostName();
            localHostAddress = InetAddress.getLoopbackAddress().getHostAddress();
        }

        journal.setDeviceProduct(sysProperties.getName());
        journal.setDeviceVersion(sysProperties.getVersion());
        journal.setDntdom(sysProperties.getDntdom());
        journal.setDeviceEventClassID(deviceEventClassID);
        journal.setName(name);
        journal.setMessage(message);
        journal.setDuser(sysProperties.getUser());
        journal.setSuser(sysProperties.getUser());
        journal.setApp("");
        journal.setDmac(getMacAddress());
        journal.setSeverity(severity);

        if (isBlank(journal.getSrc()) || isBlank(journal.getShost())) {
            journal.setSrc(localHostAddress);
            journal.setShost(localHostName);
        }

        if (journal.getDpt() == null || journal.getDpt() == 0) {
            journal.setDpt(sysProperties.getDpt());
        }

        if (isBlank(journal.getDhost()) || isBlank(journal.getDst())) {
            journal.setDhost(localHostName);
            journal.setDst(localHostAddress);
            journal.setDvchost(localHostName);
        }

        try (
                MDC.MDCCloseable hostClosable = MDC.putCloseable("host", journal.getHostForSvoi());
                MDC.MDCCloseable logTypeClosable = MDC.putCloseable("log_type", "audit_log")
        ) {
            log.info(StringUtils.replace(journal.toString(), "OmniPlatform", "ORD"));
        }

        if (!logsDatabaseProperties.isEnabled()) return;

        try {
            LocalDateTime created = LocalDateTime.parse(journal.getStart(), formatter);
            logRepository.save(new Log(
                    created,
                    StringUtils.replace(journal.toString(), "OmniPlatform", "ORD"),
                    deviceEventClassID
            ));
        } catch (Exception e) {
            log.error("Ошибка при сохранении лога в БД", e);
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }


    private String getMacAddress() {
        List<String> addresses = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();
                byte[] mac = ni.getHardwareAddress();
                if (mac == null) continue;
                for (byte b : mac) {
                    addresses.add(String.format("%02X", b));
                }
            }
        } catch (SocketException e) {
            log.error(e.getMessage(), e);
        }
        return String.join(":", addresses);
    }

    private static boolean isIpAddress(String input) {
        if (input == null) return false;

        boolean hasDots = input.chars().filter(ch -> ch == '.').count() == 3;
        boolean hasColons = input.contains(":");

        return hasDots || hasColons;
    }

    private static String resolveToOpposite(String input) {
        try {
            InetAddress inetAddress = InetAddress.getByName(input);

            if (isIpAddress(input)) {
                return inetAddress.getHostName();
            } else {
                return inetAddress.getHostAddress();
            }
        } catch (UnknownHostException e) {
            return "Unable to resolve: " + input;
        }
    }
}
