import javax.net.ssl.*;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;

/**
 * @author Georgiy Korneev (kgeorgiy@kgeorgiy.info)
 */
public final class TestMinion {
    private TestMinion() {
    }

    public static void main(final String[] args) throws Exception {
        System.out.println("Version: " + System.getProperty("java.version"));

        final char[] password = "vc/iIcg1R/Zbuf55a/Yu7d35EvCX7rNPYgarD5KK8UAlzh7KZRYz5LQ1wxmSo8IZ36X7kytSrHQ6".toCharArray();
        final KeyStore jks = KeyStore.getInstance("JKS");
        try (final FileInputStream is = new FileInputStream("__keys/client.netadmin.test.jks")) {
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
                    final SSLSocket socket = (SSLSocket) socketFactory.createSocket("127.0.0.1", 6236);
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
