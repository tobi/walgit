use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use abi_stable::sabi_trait::TD_Opaque;
use abi_stable::std_types::{ROption, RResult, RString, RVec};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;
use tokio::runtime::{Handle, Runtime};
use walgit_store::{
    AccelTarget, BoxStream, ByteStream, DynStore, GetOptions, GetResult, ObjectMeta, ObjectStore,
    PutBody, PutOptions, Result, StoreError, Version,
};

use crate::abi::{self, Reply, StoreApi, StreamApi};
use crate::wire::{Accel, Meta, Operation, ReadResult, WireError, put_options};

fn json(value: &impl Serialize) -> Result<RVec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(StoreError::other)?;
    if bytes.len() > abi::MAX_METADATA {
        return Err(StoreError::InvalidArgument(
            "plugin metadata exceeds limit".into(),
        ));
    }
    Ok(bytes.into())
}
fn wire_error(error: StoreError) -> RString {
    serde_json::to_string(&WireError::from(error))
        .unwrap_or_else(|_| r#"{"error":"other","message":"plugin failure"}"#.into())
        .into()
}
fn decode<T: serde::de::DeserializeOwned>(buffer: &[u8]) -> Result<T> {
    if buffer.len() > abi::MAX_METADATA {
        return Err(StoreError::InvalidArgument(
            "plugin metadata exceeds limit".into(),
        ));
    }
    serde_json::from_slice(buffer).map_err(StoreError::other)
}

struct StreamState {
    body: ByteStream,
    pending: Bytes,
    runtime: Handle,
}
impl StreamApi for StreamState {
    fn poll(&mut self) -> RResult<ROption<RVec<u8>>, RString> {
        match catch_unwind(AssertUnwindSafe(|| {
            if self.pending.is_empty() {
                match self.runtime.block_on(self.body.next()) {
                    Some(Ok(bytes)) => self.pending = bytes,
                    Some(Err(error)) => return Err(error),
                    None => return Ok(ROption::RNone),
                }
            }
            let size = self.pending.len().min(abi::MAX_FRAME);
            Ok(ROption::RSome(self.pending.split_to(size).to_vec().into()))
        })) {
            Ok(result) => result.map_err(wire_error).into(),
            Err(_) => RResult::RErr(wire_error(StoreError::other(anyhow::anyhow!(
                "plugin stream panicked"
            )))),
        }
    }
}
fn expose_stream(body: ByteStream, runtime: Handle) -> abi::Stream {
    ROption::RSome(abi::StreamApi_TO::from_value(
        StreamState {
            body,
            pending: Bytes::new(),
            runtime,
        },
        TD_Opaque,
    ))
}

// RVec keeps its allocator's checked destruction machinery. Bytes holds that
// owner without copying the returned frame into a second Rust allocation.
fn receive_stream(stream: abi::Stream, endpoint: Option<Arc<abi::Store>>) -> ByteStream {
    Box::pin(futures::stream::try_unfold(
        (stream, endpoint),
        |(stream, endpoint)| async move {
            tokio::task::spawn_blocking(move || {
                let Some(mut stream) = stream.into_option() else {
                    return Ok(None);
                };
                let frame = stream.poll().into_result().map_err(|error| {
                    decode::<WireError>(error.as_bytes())
                        .map_or_else(std::convert::identity, StoreError::from)
                })?;
                let Some(frame) = frame.into_option() else {
                    return Ok(None);
                };
                if frame.len() > abi::MAX_FRAME {
                    return Err(StoreError::InvalidArgument(
                        "plugin frame exceeds limit".into(),
                    ));
                }
                Ok(Some((
                    Bytes::from_owner(frame),
                    (ROption::RSome(stream), endpoint),
                )))
            })
            .await
            .map_err(StoreError::other)?
        },
    ))
}

struct StoreState {
    store: DynStore,
    runtime: Handle,
    owned_runtime: Option<Runtime>,
}
impl Drop for StoreState {
    fn drop(&mut self) {
        if let Some(runtime) = self.owned_runtime.take() {
            runtime.shutdown_background();
        }
    }
}
impl StoreApi for StoreState {
    fn request(&self, metadata: RVec<u8>, body: abi::Stream) -> RResult<Reply, RString> {
        match catch_unwind(AssertUnwindSafe(|| {
            let operation: Operation = decode(&metadata)?;
            self.runtime.block_on(dispatch(self, operation, body))
        })) {
            Ok(result) => result.map_err(wire_error).into(),
            Err(_) => RResult::RErr(wire_error(StoreError::other(anyhow::anyhow!(
                "storage plugin panicked"
            )))),
        }
    }
    fn supports_compose(&self) -> bool {
        catch_unwind(AssertUnwindSafe(|| self.store.supports_compose())).unwrap_or(false)
    }
    fn compose_is_native(&self) -> bool {
        catch_unwind(AssertUnwindSafe(|| self.store.compose_is_native())).unwrap_or(false)
    }
}
pub fn expose(store: DynStore, runtime: Handle, owned_runtime: Option<Runtime>) -> abi::Store {
    abi::StoreApi_TO::from_value(
        StoreState {
            store,
            runtime,
            owned_runtime,
        },
        TD_Opaque,
    )
}

async fn dispatch(state: &StoreState, operation: Operation, body: abi::Stream) -> Result<Reply> {
    let store = &state.store;
    let mut reply = Reply::default();
    reply.metadata = match operation {
        Operation::Get { key, options } => match store.get(&key, options.into()).await? {
            GetResult::NotModified { version } => json(&ReadResult::NotModified {
                version: version.to_string(),
            })?,
            GetResult::Object { meta, body } => {
                reply.body = expose_stream(body, state.runtime.clone());
                json(&ReadResult::Object { meta: meta.into() })?
            }
        },
        Operation::Head { key } => json(&store.head(&key).await?.map(Meta::from))?,
        Operation::Put {
            key,
            len,
            mode,
            immutable,
            content_type,
        } => {
            let meta = store
                .put(
                    &key,
                    PutBody::Stream {
                        len,
                        stream: receive_stream(body, None),
                    },
                    put_options(mode, immutable, content_type.as_deref()),
                )
                .await?;
            return Ok(Reply {
                metadata: json(&Meta::from(meta))?,
                ..Reply::default()
            });
        }
        Operation::Delete { key, version } => {
            store.delete(&key, version.map(Version::new)).await?;
            json(&())?
        }
        Operation::List {
            prefix,
            start_after,
        } => {
            let lines = store.list(&prefix, start_after.as_deref()).map(|item| {
                let mut bytes =
                    serde_json::to_vec(&Meta::from(item?)).map_err(StoreError::other)?;
                bytes.push(b'\n');
                Ok(Bytes::from(bytes))
            });
            reply.body = expose_stream(Box::pin(lines), state.runtime.clone());
            json(&())?
        }
        Operation::ListPrefixes { prefix } => json(&store.list_prefixes(&prefix).await?)?,
        Operation::SignedGetUrl { key, ttl_secs } => json(
            &store
                .signed_get_url(&key, Duration::from_secs(ttl_secs))
                .await?,
        )?,
        Operation::AccelTarget { key } => json(&store.accel_target(&key).await.map(|a| Accel {
            url: a.url,
            authorization: a.authorization,
        }))?,
        Operation::Compose {
            key,
            sources,
            mode,
            immutable,
            content_type,
        } => json(&Meta::from(
            store
                .compose(
                    &key,
                    &sources,
                    put_options(mode, immutable, content_type.as_deref()),
                )
                .await?,
        ))?,
    };
    Ok(reply)
}

#[derive(Clone)]
pub struct RemoteStore {
    endpoint: Arc<abi::Store>,
}
impl RemoteStore {
    pub fn new(store: abi::Store) -> Self {
        Self {
            endpoint: Arc::new(store),
        }
    }
    async fn call(&self, operation: Operation, body: Option<ByteStream>) -> Result<Reply> {
        let metadata = json(&operation)?;
        let body = body.map_or_else(abi::Stream::default, |body| {
            expose_stream(body, Handle::current())
        });
        let endpoint = self.endpoint.clone();
        tokio::task::spawn_blocking(move || {
            endpoint
                .request(metadata, body)
                .into_result()
                .map_err(|error| {
                    decode::<WireError>(error.as_bytes())
                        .map_or_else(std::convert::identity, StoreError::from)
                })
        })
        .await
        .map_err(StoreError::other)?
    }
}

#[async_trait::async_trait]
impl ObjectStore for RemoteStore {
    fn backend(&self) -> &'static str {
        "plugin"
    }
    async fn get(&self, key: &str, options: GetOptions) -> Result<GetResult> {
        let reply = self
            .call(
                Operation::Get {
                    key: key.into(),
                    options: options.into(),
                },
                None,
            )
            .await?;
        Ok(match decode(&reply.metadata)? {
            ReadResult::NotModified { version } => GetResult::NotModified {
                version: Version::new(version),
            },
            ReadResult::Object { meta } => GetResult::Object {
                meta: meta.into(),
                body: receive_stream(reply.body, Some(self.endpoint.clone())),
            },
        })
    }
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let reply = self.call(Operation::Head { key: key.into() }, None).await?;
        Ok(decode::<Option<Meta>>(&reply.metadata)?.map(Into::into))
    }
    async fn put(&self, key: &str, body: PutBody, options: PutOptions) -> Result<ObjectMeta> {
        let (len, stream) = match body {
            PutBody::Bytes(bytes) => (bytes.len() as u64, walgit_store::util::once(bytes)),
            PutBody::Stream { len, stream } => (len, stream),
            PutBody::File(path) => (
                tokio::fs::metadata(&path)
                    .await
                    .map_err(StoreError::other)?
                    .len(),
                walgit_store::util::file_stream(path, None, abi::MAX_FRAME),
            ),
        };
        let reply = self
            .call(
                Operation::Put {
                    key: key.into(),
                    len,
                    mode: options.mode.into(),
                    immutable: options.immutable,
                    content_type: options.content_type.map(str::to_owned),
                },
                Some(stream),
            )
            .await?;
        Ok(decode::<Meta>(&reply.metadata)?.into())
    }
    async fn delete(&self, key: &str, version: Option<Version>) -> Result<()> {
        self.call(
            Operation::Delete {
                key: key.into(),
                version: version.map(|v| v.to_string()),
            },
            None,
        )
        .await?;
        Ok(())
    }
    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let this = self.clone();
        let operation = Operation::List {
            prefix: prefix.into(),
            start_after: start_after.map(str::to_owned),
        };
        // Newline framing permits the byte stream to split a JSON record.
        Box::pin(
            futures::stream::once(async move {
                let reply = this.call(operation, None).await?;
                let body = receive_stream(reply.body, Some(this.endpoint.clone()));
                Ok::<_, StoreError>(futures::stream::try_unfold(
                    (body, Vec::new()),
                    |(mut body, mut pending)| async move {
                        loop {
                            if let Some(end) = pending.iter().position(|b| *b == b'\n') {
                                let line: Vec<_> = pending.drain(..=end).collect();
                                let meta: Meta =
                                    serde_json::from_slice(&line).map_err(StoreError::other)?;
                                return Ok(Some((meta.into(), (body, pending))));
                            }
                            if pending.len() > abi::MAX_METADATA {
                                return Err(StoreError::InvalidArgument(
                                    "plugin list record exceeds limit".into(),
                                ));
                            }
                            match body.next().await {
                                Some(chunk) => pending.extend_from_slice(&chunk?),
                                None if pending.is_empty() => return Ok(None),
                                None => {
                                    return Err(StoreError::other(anyhow::anyhow!(
                                        "truncated plugin listing"
                                    )));
                                }
                            }
                        }
                    },
                ))
            })
            .try_flatten(),
        )
    }
    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let reply = self
            .call(
                Operation::ListPrefixes {
                    prefix: prefix.into(),
                },
                None,
            )
            .await?;
        decode(&reply.metadata)
    }
    async fn signed_get_url(&self, key: &str, ttl: Duration) -> Result<Option<String>> {
        let reply = self
            .call(
                Operation::SignedGetUrl {
                    key: key.into(),
                    ttl_secs: ttl.as_secs(),
                },
                None,
            )
            .await?;
        decode(&reply.metadata)
    }
    async fn accel_target(&self, key: &str) -> Option<AccelTarget> {
        let reply = self
            .call(Operation::AccelTarget { key: key.into() }, None)
            .await
            .ok()?;
        decode::<Option<Accel>>(&reply.metadata)
            .ok()?
            .map(|a| AccelTarget {
                url: a.url,
                authorization: a.authorization,
            })
    }
    fn supports_compose(&self) -> bool {
        self.endpoint.supports_compose()
    }
    fn compose_is_native(&self) -> bool {
        self.endpoint.compose_is_native()
    }
    async fn compose(
        &self,
        key: &str,
        sources: &[String],
        options: PutOptions,
    ) -> Result<ObjectMeta> {
        let reply = self
            .call(
                Operation::Compose {
                    key: key.into(),
                    sources: sources.into(),
                    mode: options.mode.into(),
                    immutable: options.immutable,
                    content_type: options.content_type.map(str::to_owned),
                },
                None,
            )
            .await?;
        Ok(decode::<Meta>(&reply.metadata)?.into())
    }
}

/// The SDK factory boundary contains panics; no application future crosses FFI.
pub fn export<F, Fut>(
    inner: abi::Store,
    config: RVec<u8>,
    factory: F,
) -> RResult<abi::Store, RString>
where
    F: FnOnce(DynStore, serde_json::Value) -> Fut,
    Fut: Future<Output = anyhow::Result<DynStore>>,
{
    let result = catch_unwind(AssertUnwindSafe(|| {
        let inner: DynStore = Arc::new(RemoteStore::new(inner));
        let options = decode(&config);
        drop(config);
        let config = options?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let store = runtime.block_on(factory(inner, config))?;
        Ok::<_, anyhow::Error>(expose(store, runtime.handle().clone(), Some(runtime)))
    }));
    match result {
        Ok(result) => result.map_err(|e| RString::from(e.to_string())).into(),
        Err(_) => RResult::RErr("storage plugin initialization panicked".into()),
    }
}
