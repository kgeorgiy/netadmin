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
* Continue to [setup NetAdmin](#keys)
    