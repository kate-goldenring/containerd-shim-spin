// Adapted from spin_oci::loader

use anyhow::{anyhow, ensure, Context, Result};
use spin_app::locked::{ContentPath, ContentRef, LockedComponent};
use spin_loader::cache::Cache;
use std::path::Path;

pub async fn resolve_component_content_refs(
    working_dir: &Path,
    component: &mut LockedComponent,
    cache: &Cache,
) -> Result<()> {
    // Update wasm content path
    let wasm_digest = content_digest(&component.source.content)?;
    let wasm_path = cache.wasm_file(wasm_digest)?;
    component.source.content = content_ref(wasm_path)?;

    for dep in &mut component.dependencies.values_mut() {
        let dep_wasm_digest = content_digest(&dep.source.content)?;
        let dep_wasm_path = cache.wasm_file(dep_wasm_digest)?;
        dep.source.content = content_ref(dep_wasm_path)?;
    }

    if !component.files.is_empty() {
        let mount_dir = working_dir.join("assets").join(&component.id);
        for file in &mut component.files {
            ensure!(is_safe_to_join(&file.path), "invalid file mount {file:?}");
            let mount_path = mount_dir.join(&file.path);

            // Create parent directory
            let mount_parent = mount_path
                .parent()
                .with_context(|| format!("invalid mount path {mount_path:?}"))?;
            tokio::fs::create_dir_all(mount_parent)
                .await
                .with_context(|| format!("failed to create temporary mount path {mount_path:?}"))?;

            if let Some(content_bytes) = file.content.inline.as_deref() {
                // Write inline content to disk
                tokio::fs::write(&mount_path, content_bytes)
                    .await
                    .with_context(|| format!("failed to write inline content to {mount_path:?}"))?;
            } else {
                // Copy content
                let digest = content_digest(&file.content)?;
                let content_path = cache.data_file(digest)?;
                // TODO: parallelize
                tokio::fs::copy(&content_path, &mount_path)
                    .await
                    .with_context(|| {
                        format!("failed to copy {:?}->{mount_path:?}", content_path)
                    })?;
            }
        }

        component.files = vec![ContentPath {
            content: content_ref(mount_dir)?,
            path: "/".into(),
        }]
    }

    Ok(())
}

fn content_digest(content_ref: &ContentRef) -> Result<&str> {
    content_ref
        .digest
        .as_deref()
        .with_context(|| format!("content missing expected digest: {content_ref:?}"))
}

fn content_ref(path: impl AsRef<Path>) -> Result<ContentRef> {
    let path = std::fs::canonicalize(path)?;
    let url = url::Url::from_file_path(path).map_err(|_| anyhow!("couldn't build file URL"))?;
    Ok(ContentRef {
        source: Some(url.to_string()),
        ..Default::default()
    })
}

fn is_safe_to_join(path: impl AsRef<Path>) -> bool {
    // This could be loosened, but currently should always be true
    path.as_ref()
        .components()
        .all(|c| matches!(c, std::path::Component::Normal(_)))
}
