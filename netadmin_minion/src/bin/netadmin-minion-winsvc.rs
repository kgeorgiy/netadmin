#[macro_use]
extern crate windows_service;

use core::time::Duration;
use std::env;
use std::ffi::OsString;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::Result;
use clap::Parser;
use tokio::{runtime::Runtime, task::{AbortHandle, JoinHandle}};
use tracing::debug;
use windows_service::{
    service::{ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType},
    service_control_handler::{self, ServiceControlHandlerResult, ServiceStatusHandle},
    service_dispatcher
};

use netadmin_minion::{log::Log, minion_main::Cli};
use netadmin_minion::log::{LevelFilterConfig, LogConfig};

define_windows_service!(ffi_service_main, service_main);

#[allow(clippy::needless_pass_by_value)]
fn service_main(arguments: Vec<OsString>) {
    let service_name = arguments.get(0)
        .and_then(|s| s.to_str())
        .unwrap_or("unknown-service-name")
        .to_owned();

    let mut log = Log::local();
    let log_file = Path::new(&env::args().next().expect("file name")).with_extension("log");
    let _ignore = log.set_local(&LogConfig::new(&log_file, LevelFilterConfig::All));

    debug!("Registering service control handler for {service_name}");
    let minion_handles: Arc<Mutex<Vec<AbortHandle>>> = Arc::new(Mutex::new(vec![]));
    let status_handle = register_handler(
        service_name.clone(),
        Arc::clone(&minion_handles)
    );

    Runtime::new().expect("runtime created").block_on(async move {
        match start(&mut log) {
            Ok(handles) => {
                as_mut(&minion_handles).extend(handles.iter().map(JoinHandle::abort_handle));
                debug!("Setting service status as running for {service_name}");
                set_running(&status_handle);
                let exit_code = Cli::wait(handles).await.map_or(1, |_| 0);
                set_stopped(&status_handle, exit_code);
            }
            Err(error) => {
                debug!("Setting service status as stopped for {service_name}");
                Log::log_error(&error);
                set_stopped(&status_handle, 1);
            }
        }
    });
}

#[allow(clippy::unwrap_used)]
fn as_mut<T>(minion_handles: &Arc<Mutex<T>>) -> MutexGuard<T> {
    minion_handles.lock().unwrap()
}

#[allow(clippy::wildcard_enum_match_arm)]
fn register_handler(service_name: String, minion_handles: Arc<Mutex<Vec<AbortHandle>>>) -> Option<ServiceStatusHandle> {
    let mut status_handle: Option<ServiceStatusHandle> = None;
    status_handle = Some(service_control_handler::register(service_name.clone(), {
        move |control_event| -> ServiceControlHandlerResult {
            match control_event {
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                ServiceControl::Stop | ServiceControl::Shutdown => {
                    for handle in as_mut(&minion_handles).iter() {
                        handle.abort();
                    }
                    debug!("Setting service status as stopped for {service_name}");
                    set_stopped(&status_handle, 0);
                    ServiceControlHandlerResult::NoError
                }
                _ => ServiceControlHandlerResult::NotImplemented,
            }
        }
    }).expect("service handler registered"));
    status_handle
}

fn start(log: &mut Log) -> Result<Vec<JoinHandle<()>>> {
    Cli::parse().create_and_serve(log)
}

fn set_stopped(handle: &Option<ServiceStatusHandle>, exit_code: u32) {
    set_status(handle, ServiceState::Stopped, exit_code, ServiceControlAccept::empty());
}

fn set_running(handle: &Option<ServiceStatusHandle>) {
    let accept = ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN;
    set_status(handle, ServiceState::Running, 0, accept);
}

fn set_status(handle: &Option<ServiceStatusHandle>, state: ServiceState, exit_code: u32, controls: ServiceControlAccept) {
    if let Some(ref handle) = *handle {
        let _ignore = handle.set_service_status(ServiceStatus {
            service_type: ServiceType::OWN_PROCESS,
            current_state: state,
            controls_accepted: controls,
            exit_code: ServiceExitCode::ServiceSpecific(exit_code),
            checkpoint: 0,
            wait_hint: Duration::from_millis(0),
            process_id: None,
        });
    }
}

fn main() -> Result<(), windows_service::Error> {
    service_dispatcher::start("netadmin-minion", ffi_service_main)
}
