# NetAdmin Minion

NetAdmin remote info and command execution service.

## Build and Installation

### Ubuntu/Debian

* [Install Rust](https://www.rust-lang.org/tools/install)
    ```
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```
* Install build tools
    ```
    sudo apt install build-essential
    ```
* Build minion
    ```
    cargo build --release
    ```
* Generate TLS keys
    ```
    cargo run --release --bin netadmin-certgen
    ```
* Install ipexec as a service
    ```
    sudo ./scripts/deb-install.sh
    ```

* Start ipexec service
    ```
    sudo service netadmin-minion start
    ```

* Check whether ipexec is running
    ```
    ps aux | grep netadmin-minion
    ```
* Continue to [setup NetAdmin Scala Server](#nass)

## <a name="nass">NetAdmin Scala Server integration</a>

* Test generated keys before transferring them to NASS.
  Here `HOST` is a NetAdmin Minion host.
  To run test scripts on Windows use `sh` bundled with Git.
    * Run `test/run-MinionInfoTest.sh HOST` to test info service
        * Requres Java 8+
        * JSON with basic minion info should be printed ten times
    * Run `test/run-LegacyExecTest.sh HOST` to test legacy exec service
        * Requires Scala 2.10-2.13
        * Output should end with
        ```
        OUT: hello world
        Exit code: 0
        ```
* Copy `__keys/netadmin-server.jks` to `$NetAdmin/keys/`.
* Edit `NetAdmin/linux-*.xml as` necessary.
* Try to execute "Echo" task via NetAdmin
