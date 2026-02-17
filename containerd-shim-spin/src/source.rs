use std::{fs::File, io::Write, path::PathBuf};

use anyhow::{Context, Result};
use containerd_shim_wasm::sandbox::context::RuntimeContext;
use log::info;
use oci_spec::image::MediaType;
use spin_app::locked::LockedApp;
use spin_loader::{cache::Cache, FilesMountStrategy};

use crate::{constants, loader, utils::handle_archive_layer};

#[derive(Clone)]
pub enum Source {
    File(PathBuf),
    Oci, // Rename to OciSpin
    // TODO: support OciWasm to support BareWasm (wkg) app source with media type "application/wasm";
}

impl std::fmt::Debug for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::File(path) => write!(f, "File({})", path.display()),
            Source::Oci => write!(f, "Oci"),
        }
    }
}

impl Source {
    pub(crate) async fn from_ctx(ctx: &impl RuntimeContext, cache: &Cache) -> Result<Self> {
        match ctx.entrypoint().source {
            containerd_shim_wasm::sandbox::context::Source::File(_) => {
                Ok(Source::File(constants::SPIN_MANIFEST_FILE_PATH.into()))
            }
            containerd_shim_wasm::sandbox::context::Source::Oci(layers) => {
                info!(" >>> configuring spin oci application {}", layers.len());

                for layer in layers {
                    log::debug!("<<< layer config: {:?}", layer.config);
                }

                for artifact in layers {
                    match artifact.config.media_type() {
                        MediaType::Other(name)
                            if name == spin_oci::client::SPIN_APPLICATION_MEDIA_TYPE =>
                        {
                            let path = PathBuf::from("/spin.json");
                            log::info!("writing spin oci config to {path:?}");
                            File::create(&path)
                                .context("failed to create spin.json")?
                                .write_all(&artifact.layer)
                                .context("failed to write spin.json")?;
                        }
                        MediaType::Other(name) if name == constants::OCI_LAYER_MEDIA_TYPE_WASM => {
                            log::info!(
                                "<<< writing wasm artifact with length {:?} config to cache, near {:?}",
                                artifact.layer.len(),
                                cache.manifests_dir()
                            );
                            cache
                                .write_wasm(&artifact.layer, &artifact.config.digest())
                                .await?;
                        }
                        MediaType::Other(name) if name == spin_oci::client::DATA_MEDIATYPE => {
                            log::debug!(
                                "<<< writing data layer to cache, near {:?}",
                                cache.manifests_dir()
                            );
                            cache
                                .write_data(&artifact.layer, &artifact.config.digest())
                                .await?;
                        }
                        MediaType::Other(name) if name == spin_oci::client::ARCHIVE_MEDIATYPE => {
                            log::debug!(
                                "<<< writing archive layer and unpacking contents to cache, near {:?}",
                                cache.manifests_dir()
                            );
                            handle_archive_layer(cache, &artifact.layer, &artifact.config.digest())
                                .await
                                .context("unable to unpack archive layer")?;
                        }
                        _ => {
                            log::debug!(
                                "<<< unknown media type {:?}",
                                artifact.config.media_type()
                            );
                        }
                    }
                }
                Ok(Source::Oci)
            }
        }
    }

    pub(crate) async fn to_locked_app(&self, cache: &Cache) -> Result<LockedApp> {
        let locked_app = match self {
            Source::File(source) => {
                // TODO: This should be configurable, see https://github.com/deislabs/containerd-wasm-shims/issues/166
                // TODO: ^^ Move aforementioned issue to this repo
                let files_mount_strategy = FilesMountStrategy::Direct;
                spin_loader::from_file(&source, files_mount_strategy, None).await
            }
            Source::Oci => {
                let working_dir = PathBuf::from("/");

                // TODO: resolve changes from this PR: https://github.com/spinframework/spin/pull/3361/changes
                let locked_content = tokio::fs::read("/spin.json")
                    .await
                    .with_context(|| format!("failed to read from \"/spin.json\""))?;
                // TODO: Support BareWasm (wkg) app source
                let mut locked_app = LockedApp::from_json(&locked_content)
                    .context("failed to decode locked app from \"/spin.json\"")?;
                for component in &mut locked_app.components {
                    loader::resolve_component_content_refs(&working_dir, component, cache)
                        .await
                        .with_context(|| {
                            format!("failed to resolve content for component {:?}", component.id)
                        })?;
                }
                Ok(locked_app)
            }
        }?;
        Ok(locked_app)
    }
}
