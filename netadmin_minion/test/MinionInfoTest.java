import javax.net.ssl.*;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;

/**
 * @author Georgiy Korneev (kgeorgiy@kgeorgiy.info)
 */
public final class MinionInfoTest {
    private MinionInfoTest() {
    }

    public static void main(final String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: java MinionInfoTest.java JKS HOST PORT");
            System.err.println("Where");
            System.err.println("    JKS        - Path to .jks key store");
            System.err.println("    HOST       - NetAdmin Minion host");
            System.err.println("    PORT       - NetAdmin Minion port");
            System.exit(1);
        }
        final String jksFile = args[0];
        final String host = args[1];
        final int port = Integer.parseInt(args[2]);

        System.out.println("Java version: " + System.getProperty("java.version"));

        final char[] password = "vc/iIcg1R/Zbuf55a/Yu7d35EvCX7rNPYgarD5KK8UAlzh7KZRYz5LQ1wxmSo8IZ36X7kytSrHQ6".toCharArray();
        final KeyStore jks = KeyStore.getInstance("JKS");
        try (final FileInputStream is = new FileInputStream(jksFile)) {
            jks.load(is, password);
        }

        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(jks, password);
        final KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
        trustManagerFactory.init(jks);
        final TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();

        final SSLContext context = SSLContext.getInstance("TLSv1.2");
        context.init(keyManagers, trustManagers, null);

        final SSLSocketFactory socketFactory = context.getSocketFactory();
        for (int i = 0; i < 10; i++) {
            try (
                    final SSLSocket socket = (SSLSocket) socketFactory.createSocket(host, port);
                    final DataInputStream in = new DataInputStream(socket.getInputStream());
                    final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            ) {
                final byte[] bytes = "{\"request_id\": \"hello\"}".getBytes(StandardCharsets.UTF_8);
                out.writeInt(bytes.length);
                out.writeInt(0x49_4E_46_4F);
                out.write(bytes);
                out.flush();
                socket.shutdownOutput();

                final int size = in.readInt();
                in.readInt();
                final byte[] buffer = in.readNBytes(size);
                System.out.println("Pass " + (i + 1) + ": " + new String(buffer, StandardCharsets.UTF_8));
            }
        }
    }
}
