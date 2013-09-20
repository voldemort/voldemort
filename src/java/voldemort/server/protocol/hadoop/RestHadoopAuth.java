package voldemort.server.protocol.hadoop;

import java.util.Properties;

import org.apache.hadoop.fs.rest.client.RestAuthService;
import org.apache.hadoop.fs.rest.client.RestFSException;
import org.apache.log4j.Logger;

import voldemort.common.service.AbstractService;
import voldemort.common.service.ServiceType;

public class RestHadoopAuth extends AbstractService {

    private final static Logger logger = Logger.getLogger(RestHadoopAuth.class);

    public static String SERVICE_NAME = "rest-hdfs-auth-service";
    public static String KERBEROS_REALM_PROPERTY = "java.security.krb5.realm";
    public static String KERBEROS_KDC_PROPERTY = "java.security.krb5.kdc";
    public static long RETRY_INTERVAL_MS = 60000; // one minute retry interval

    private static SynchronizedAuth syncAuth = null;

    public RestHadoopAuth(String kerberosRealm,
                          String kerberosKdc,
                          String kerberosPrincipal,
                          String kerberosKeytabPath) {
        super(ServiceType.HDFSAuth);

        // TODO: fix this
        if(null == syncAuth) {
            syncAuth = new SynchronizedAuth(kerberosRealm,
                                            kerberosKdc,
                                            kerberosPrincipal,
                                            kerberosKeytabPath);
        }
    }

    @Override
    protected void startInner() {
        syncAuth.loginIfNot();
    }

    @Override
    protected void stopInner() {
        syncAuth.logoutIfNot();
    }

    public static void loginIfNot() {
        syncAuth.loginIfNot();
    }

    class SynchronizedAuth {

        String kerberosPrincipal;
        String kerberosKeytabPath;
        private Properties props;
        private boolean isLoggedIn = false;

        public SynchronizedAuth(String kerberosRealm,
                                String kerberosKdc,
                                String kerberosPrincipal,
                                String kerberosKeytabPath) {
            this.kerberosPrincipal = kerberosPrincipal;
            this.kerberosKeytabPath = kerberosKeytabPath;
            props = new Properties();
            props.setProperty(KERBEROS_REALM_PROPERTY, kerberosRealm);
            props.setProperty(KERBEROS_KDC_PROPERTY, kerberosKdc);
        }

        public synchronized void loginIfNot() {
            if(!isLoggedIn) {
                try {
                    RestAuthService.loginUserFromKeytab(this.kerberosPrincipal,
                                                        this.kerberosKeytabPath,
                                                        props);
                    isLoggedIn = true;
                    logger.info("Successfully logged in HDFS");
                } catch(RestFSException e) {
                    logger.warn("Failed to authenticate to HDFS: " + e);
                }
            } else {
                if(logger.isDebugEnabled()) {
                    logger.debug("Already logged in.");
                }
            }
        }

        public synchronized void logoutIfNot() {
            try {
                if(isLoggedIn) {
                    RestAuthService.logout();
                    logger.info("Successfully logged out HDFS");
                } else {
                    logger.info("Auth service stopped before logged into HDFS.");
                }
            } catch(RestFSException e) {
                isLoggedIn = false;
                logger.warn("Encounted error while logging out hadoop: " + e.getMessage());
            }
        }
    }

}
