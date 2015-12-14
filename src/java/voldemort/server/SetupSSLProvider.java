package voldemort.server;

import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

public class SetupSSLProvider {
    public static void useBouncyCastle() {
        Security.insertProviderAt(new BouncyCastleProvider(), 1);
    }
}
