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

    private static String kerberosPrincipal;
    private static String kerberosKeytabPath;
    private static Properties props;

    public RestHadoopAuth(String kerberosRealm,
                          String kerberosKdc,
                          String kerberosPrincipal,
                          String kerberosKeytabPath) {
        super(ServiceType.HDFSAuth);

        // initialize all the secured hadoop properties for login
        init(kerberosRealm, kerberosKdc, kerberosPrincipal, kerberosKeytabPath);
    }

    /**
     * Invoked when start() method is called.
     */
    @Override
    protected void startInner() {
        try {
            loginSecuredHdfs();
        } catch(RestFSException e) {
            logger.warn("Failed to authenticate to HDFS: " + e);
            logger.warn("Will try authentication to HDFS again later.");
        }
    }

    /**
     * Invoked when stop() method is called.
     */
    @Override
    protected void stopInner() {
        try {
            logoutSecuredHdfs();
        } catch(RestFSException e) {
            logger.warn("Encounted error while logging out hadoop: " + e);
        }
    }

    /**
     * This method is not thread-safe, and is not expected to be called multiple
     * times because RestHadoopAuth service is only created once at the
     * beginning when voldemort server starts.
     * 
     * @param realm Kerberos realm
     * @param kdc Kerberos kdc
     * @param principal Kerberos user
     * @param keytabPath Kerberos key tab path
     */
    private static void init(String realm, String kdc, String principal, String keytabPath) {
        kerberosPrincipal = principal;
        kerberosKeytabPath = keytabPath;
        props = new Properties();
        props.setProperty(KERBEROS_REALM_PROPERTY, realm);
        props.setProperty(KERBEROS_KDC_PROPERTY, kdc);

        logger.info("Initializing secured hadoop properties: ");
        logger.info("  Kerberos realm: " + realm);
        logger.info("  Kerberos kdc: " + kdc);
        logger.info("  Kerberos user: " + principal);
        logger.info("  Kerberos keytab path: " + keytabPath);
    }

    /**
     * This method is a wrapper of RestAuthService.loginUserFromKeytab(), which
     * is thread-safe and can be called multiple times, even after the session
     * is already logged in.
     * 
     * It is currently called once when a Voldemort server starts up all its
     * services; and it is also called when starting each build-and-push job in
     * case the last login wasn't successful.
     * 
     * @throws RestFSException
     */
    public static void loginSecuredHdfs() throws RestFSException {
        RestAuthService.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabPath, props);
        logger.info("Successfully logged in HDFS");
    }

    /**
     * This method is a wrapper of RestAuthService.logout(), which is NOT
     * thread-safe and shall only be called when the voldemort server is
     * shutting down, after all build-and-push jobs are terminated on the
     * voldemort server.
     * 
     * It is currently called once when a Voldemort server stops all its
     * services
     * 
     * @throws RestFSException
     */
    public static void logoutSecuredHdfs() throws RestFSException {
        if(RestAuthService.logout()) {
            logger.info("Successfully logged out HDFS");
        }
    }
}
