import java.io._;
import java.net._;
import javax.net.ssl._
import java.nio.charset.StandardCharsets;
import java.security.{KeyStore => JKeyStore}

object LegacyExecTest {
  /** Packet type for process output. */
  val PROCESS_OUTPUT    = 0x12345678

  /** Packet type for process exit code. */
  val PROCESS_EXIT_CODE = 0x00000000

  def main(args: Array[String]) {
    println(new File(".").getAbsolutePath)
    if (args.length < 2) {
        println("Usage: scala LegacyExecTest [host] [port] [command]")
        System.exit(1)
    }

    val host = args(0)
    val port = args(1).toInt
    val command = args.slice(2, args.length).mkString(" ")

    try {
      val keyStore = new KeyStore(
        "__keys/client.netadmin.test.jks",
        "vc/iIcg1R/Zbuf55a/Yu7d35EvCX7rNPYgarD5KK8UAlzh7KZRYz5LQ1wxmSo8IZ36X7kytSrHQ6"
      )
      keyStore.check()
      val factory = keyStore.trustingAuthContext.getSocketFactory
      val bytes = command.getBytes(StandardCharsets.UTF_8)

      val socket = factory.createSocket(host, port)
      try {
        val os = new DataOutputStream(socket.getOutputStream)
        os.writeInt(bytes.length)
        os.write(bytes)
        os.flush()

        val is = new DataInputStream(socket.getInputStream)
        var exitCode = 0
        while (true) {
          val head = is.readInt()
          if (head == PROCESS_OUTPUT) {
            val size = is.readInt()
            val buffer = Array.ofDim[Byte](size)
            is.readFully(buffer);
            print("OUT: " + new String(buffer, StandardCharsets.UTF_8));
          } else if (head == PROCESS_EXIT_CODE) {
            exitCode = is.readInt()
            println("Exit code: " + exitCode)
            return
          } else {
            throw new AssertionError("Unknown packet type " + head)
          }
        }
      } finally {
        socket.close()
      }
    } catch {
      case e: Exception => {
        e.printStackTrace
        println("Error: " + e.getMessage)
      }
    }
  }
}


/**
 * Private key store.
 *
 * @author Georgiy Korneev (kgeorgiy@kgeorgiy.info)
 */
case class KeyStore(path: String, password: String) {
  log(s"Creating key store $this")

  /**
   * Checks that this key store is valid.
   * @throws IllegalStateException if this key store is invalid
   */
  def check() {
    try java() catch { case e: Exception =>
      throw new IllegalStateException(s"Invalid $this: ${e.getMessage}", e)
    }
  }

  /** Creates Java [[java.security.KeyStore]]. */
  def java(): JKeyStore = {
    log(s"Creating Java key store")
    val keyStore = JKeyStore.getInstance("JKS")
    val is = new FileInputStream(path)
    try {
      keyStore.load(is, password.toCharArray)
      keyStore
    } finally {
      is.close()
    }
  }

  /** [[javax.net.ssl.SSLContext]] trusting certificates contained in this key store. */
  lazy val trustingContext: SSLContext = context("trusting", null, trustManagers)

  /** [[javax.net.ssl.SSLContext]] authenticating using certificates contained in this key store. */
  lazy val authContext: SSLContext = context("authenticating", keyManagers, null)

  /** [[javax.net.ssl.SSLContext]] trusting and authenticates using certificates contained in this key store. */
  lazy val trustingAuthContext: SSLContext = context("trusting and authenticating", keyManagers, trustManagers)

  private def context(message: String, keyManagers: Array[KeyManager], trustManagers: Array[TrustManager]) = {
    log(s"Creating $message SSL context")
    val context = SSLContext.getInstance("TLS")
    context.init(keyManagers, trustManagers, null)
    context
  }

  private lazy val trustManagers = {
    log("Creating trust managers")
    val trustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    trustManagerFactory.init(java())
    trustManagerFactory.getTrustManagers
  }

  private lazy val keyManagers = {
    log(s"Creating key managers")
    val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(java(), password.toCharArray)
    keyManagerFactory.getKeyManagers
  }

  private def log(message: String) = println(s"$this: $message")

  override def toString = s"KeyStore(path=$path, password-hash=${Integer.toHexString(password.hashCode)}})"
}
