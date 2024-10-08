use std::{
    collections::{hash_map::DefaultHasher, HashSet},
    env,
    hash::{Hash, Hasher},
};

use anyhow::{Context, Result};
use containerd_shim_wasm::{
    container::{Engine, RuntimeContext, Stdio},
    sandbox::WasmLayer,
    version,
};
use futures::future;
use log::info;
use spin_app::locked::LockedApp;
use spin_trigger::TriggerExecutor;
use spin_trigger_http::HttpTrigger;
use spin_trigger_redis::RedisTrigger;
use tokio::runtime::Runtime;
use trigger_command::CommandTrigger;
use trigger_mqtt::MqttTrigger;
use trigger_sqs::SqsTrigger;

use crate::{
    constants,
    source::Source,
    trigger::{build_trigger, get_supported_triggers},
    utils::{
        configure_application_variables_from_environment_variables, initialize_cache,
        is_wasm_content, parse_addr,
    },
};

#[derive(Clone)]
pub struct SpinEngine {
    pub(crate) wasmtime_engine: wasmtime::Engine,
}

impl Default for SpinEngine {
    fn default() -> Self {
        // the host expects epoch interruption to be enabled, so this has to be
        // turned on for the components we compile.
        let mut config = wasmtime::Config::default();
        config.epoch_interruption(true);
        Self {
            wasmtime_engine: wasmtime::Engine::new(&config).unwrap(),
        }
    }
}

impl Engine for SpinEngine {
    fn name() -> &'static str {
        "spin"
    }

    fn run_wasi(&self, ctx: &impl RuntimeContext, stdio: Stdio) -> Result<i32> {
        stdio.redirect()?;
        info!("setting up wasi");
        let rt = Runtime::new().context("failed to create runtime")?;

        let (abortable, abort_handle) = futures::future::abortable(self.wasm_exec_async(ctx));
        ctrlc::set_handler(move || abort_handle.abort())?;

        match rt.block_on(abortable) {
            Ok(Ok(())) => {
                info!("run_wasi shut down: exiting");
                Ok(0)
            }
            Ok(Err(err)) => {
                log::error!("run_wasi ERROR >>>  failed: {:?}", err);
                Err(err)
            }
            Err(aborted) => {
                info!("Received signal to abort: {:?}", aborted);
                Ok(0)
            }
        }
    }

    fn can_handle(&self, _ctx: &impl RuntimeContext) -> Result<()> {
        Ok(())
    }

    fn supported_layers_types() -> &'static [&'static str] {
        &[
            constants::OCI_LAYER_MEDIA_TYPE_WASM,
            spin_oci::client::ARCHIVE_MEDIATYPE,
            spin_oci::client::DATA_MEDIATYPE,
            spin_oci::client::SPIN_APPLICATION_MEDIA_TYPE,
        ]
    }

    fn precompile(&self, layers: &[WasmLayer]) -> Result<Vec<Option<Vec<u8>>>> {
        // Runwasi expects layers to be returned in the same order, so wrap each layer in an option, setting non Wasm layers to None
        let precompiled_layers = layers
            .iter()
            .map(|layer| match is_wasm_content(layer) {
                Some(wasm_layer) => {
                    log::info!(
                        "Precompile called for wasm layer {:?}",
                        wasm_layer.config.digest()
                    );
                    if self
                        .wasmtime_engine
                        .detect_precompiled(&wasm_layer.layer)
                        .is_some()
                    {
                        log::info!("Layer already precompiled {:?}", wasm_layer.config.digest());
                        Ok(Some(wasm_layer.layer))
                    } else {
                        let component =
                            spin_componentize::componentize_if_necessary(&wasm_layer.layer)?;
                        let precompiled = self.wasmtime_engine.precompile_component(&component)?;
                        Ok(Some(precompiled))
                    }
                }
                None => Ok(None),
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(precompiled_layers)
    }

    fn can_precompile(&self) -> Option<String> {
        let mut hasher = DefaultHasher::new();
        self.wasmtime_engine
            .precompile_compatibility_hash()
            .hash(&mut hasher);
        Some(hasher.finish().to_string())
    }
}

impl SpinEngine {
    async fn wasm_exec_async(&self, ctx: &impl RuntimeContext) -> Result<()> {
        let cache = initialize_cache().await?;
        let app_source = Source::from_ctx(ctx, &cache).await?;
        let locked_app = app_source.to_locked_app(&cache).await?;
        configure_application_variables_from_environment_variables(&locked_app)?;
        let trigger_cmds = get_supported_triggers(&locked_app)
            .with_context(|| format!("Couldn't find trigger executor for {app_source:?}"))?;
        let _telemetry_guard = spin_telemetry::init(version!().to_string())?;

        self.run_trigger(ctx, &trigger_cmds, locked_app, app_source)
            .await
    }

    async fn run_trigger(
        &self,
        ctx: &impl RuntimeContext,
        trigger_types: &HashSet<String>,
        app: LockedApp,
        app_source: Source,
    ) -> Result<()> {
        let mut futures_list = Vec::new();
        let mut trigger_type_map = Vec::new();

        for trigger_type in trigger_types.iter() {
            let f = match trigger_type.as_str() {
                HttpTrigger::TRIGGER_TYPE => {
                    let http_trigger =
                        build_trigger::<HttpTrigger>(app.clone(), app_source.clone()).await?;
                    info!(" >>> running spin http trigger");
                    let address_str = env::var(constants::SPIN_HTTP_LISTEN_ADDR_ENV)
                        .unwrap_or_else(|_| constants::SPIN_ADDR_DEFAULT.to_string());
                    let address = parse_addr(&address_str)?;
                    http_trigger.run(spin_trigger_http::CliArgs {
                        address,
                        tls_cert: None,
                        tls_key: None,
                    })
                }
                RedisTrigger::TRIGGER_TYPE => {
                    let redis_trigger =
                        build_trigger::<RedisTrigger>(app.clone(), app_source.clone()).await?;
                    info!(" >>> running spin redis trigger");
                    redis_trigger.run(spin_trigger::cli::NoArgs)
                }
                SqsTrigger::TRIGGER_TYPE => {
                    let sqs_trigger =
                        build_trigger::<SqsTrigger>(app.clone(), app_source.clone()).await?;
                    info!(" >>> running spin sqs trigger");
                    sqs_trigger.run(spin_trigger::cli::NoArgs)
                }
                CommandTrigger::TRIGGER_TYPE => {
                    let command_trigger =
                        build_trigger::<CommandTrigger>(app.clone(), app_source.clone()).await?;
                    info!(" >>> running spin command trigger");
                    command_trigger.run(trigger_command::CliArgs {
                        guest_args: ctx.args().to_vec(),
                    })
                }
                MqttTrigger::TRIGGER_TYPE => {
                    let mqtt_trigger =
                        build_trigger::<MqttTrigger>(app.clone(), app_source.clone()).await?;
                    info!(" >>> running spin mqtt trigger");
                    mqtt_trigger.run(trigger_mqtt::CliArgs { test: false })
                }
                _ => {
                    // This should never happen as we check for supported triggers in get_supported_triggers
                    unreachable!()
                }
            };

            trigger_type_map.push(trigger_type.clone());
            futures_list.push(f);
        }

        info!(" >>> notifying main thread we are about to start");

        // exit as soon as any of the trigger completes/exits
        let (result, index, rest) = future::select_all(futures_list).await;
        let trigger_type = &trigger_type_map[index];

        info!(" >>> trigger type '{trigger_type}' exited");

        drop(rest);

        result
    }
}

#[cfg(test)]
mod tests {
    use oci_spec::image::MediaType;

    use super::*;

    #[test]
    fn precompile() {
        let module = wat::parse_str("(module)").unwrap();
        let wasmtime_engine = wasmtime::Engine::default();
        let component = wasmtime::component::Component::new(&wasmtime_engine, "(component)")
            .unwrap()
            .serialize()
            .unwrap();
        let wasm_layers: Vec<WasmLayer> = vec![
            // Needs to be precompiled
            WasmLayer {
                layer: module.clone(),
                config: oci_spec::image::Descriptor::new(
                    MediaType::Other(constants::OCI_LAYER_MEDIA_TYPE_WASM.to_string()),
                    1024,
                    "sha256:1234",
                ),
            },
            // Precompiled
            WasmLayer {
                layer: component.to_owned(),
                config: oci_spec::image::Descriptor::new(
                    MediaType::Other(constants::OCI_LAYER_MEDIA_TYPE_WASM.to_string()),
                    1024,
                    "sha256:1234",
                ),
            },
            // Content that should be skipped
            WasmLayer {
                layer: vec![],
                config: oci_spec::image::Descriptor::new(
                    MediaType::Other(spin_oci::client::DATA_MEDIATYPE.to_string()),
                    1024,
                    "sha256:1234",
                ),
            },
        ];
        let spin_engine = SpinEngine::default();
        let precompiled = spin_engine
            .precompile(&wasm_layers)
            .expect("precompile failed");
        assert_eq!(precompiled.len(), 3);
        assert_ne!(precompiled[0].as_deref().expect("no first entry"), module);
        assert_eq!(
            precompiled[1].as_deref().expect("no second entry"),
            component
        );
        assert!(precompiled[2].is_none());
    }
}
