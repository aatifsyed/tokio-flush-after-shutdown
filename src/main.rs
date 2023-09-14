use async_compression::tokio::write::ZstdEncoder;
use bytes::Bytes;
use futures::{stream, Sink, StreamExt as _};
use pin_project::{pin_project, pinned_drop};
use std::io;
use std::{
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::AsyncWrite;
use tokio_util::codec::{BytesCodec, Encoder, FramedWrite};

async fn _main() {
    let file = TracingFile::create("/dev/null").await.unwrap();
    // the bug goes away in this case:
    // let file = TracingAsyncWriteSink::new();
    let zstd_encoder = TracingZstdEncoder::new(file);
    stream::empty::<io::Result<Bytes>>()
        .forward(FramedWrite::new(zstd_encoder, BytesCodec::new()))
        .await
        .unwrap();
}

#[derive(Debug)]
struct TracingAsyncWriteSink {}

impl TracingAsyncWriteSink {
    fn new() -> Self {
        Self {}
    }
}

impl AsyncWrite for TracingAsyncWriteSink {
    #[tracing::instrument]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    #[tracing::instrument]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    #[tracing::instrument]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[pin_project]
#[derive(Debug)]
struct TracingFramedWrite<T, E> {
    #[pin]
    inner: FramedWrite<T, E>,
}

impl<T, E> TracingFramedWrite<T, E>
where
    T: AsyncWrite,
{
    fn new(inner: T, encoder: E) -> Self {
        Self {
            inner: FramedWrite::new(inner, encoder),
        }
    }
}

impl<T, I, E> Sink<I> for TracingFramedWrite<T, E>
where
    I: Debug,
    T: AsyncWrite + Debug,
    E: Encoder<I> + Debug,
    E::Error: From<std::io::Error> + Debug,
{
    type Error = E::Error;

    #[tracing::instrument]
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    #[tracing::instrument]
    fn start_send(self: Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        self.project().inner.start_send(item)
    }

    #[tracing::instrument]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    #[tracing::instrument]
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx)
    }
}

#[pin_project]
#[derive(Debug)]
struct TracingZstdEncoder<W> {
    #[pin]
    inner: ZstdEncoder<W>,
}

impl<W> TracingZstdEncoder<W>
where
    W: AsyncWrite,
{
    fn new(inner: W) -> Self {
        Self {
            inner: ZstdEncoder::new(inner),
        }
    }
}

impl<W> AsyncWrite for TracingZstdEncoder<W>
where
    W: AsyncWrite + Debug,
{
    #[tracing::instrument]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().inner.poll_write(cx, buf)
    }

    #[tracing::instrument]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    #[tracing::instrument]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_shutdown(cx)
    }
}

#[pin_project(PinnedDrop)]
#[derive(Debug)]
struct TracingFile {
    #[pin]
    inner: tokio::fs::File,
}

impl TracingFile {
    async fn create(path: &str) -> io::Result<Self> {
        let inner = tokio::fs::File::create(path).await?;
        Ok(Self { inner })
    }
}

#[pinned_drop]
impl PinnedDrop for TracingFile {
    #[tracing::instrument]
    fn drop(self: Pin<&mut Self>) {}
}

impl AsyncWrite for TracingFile {
    #[tracing::instrument]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().inner.poll_write(cx, buf)
    }

    #[tracing::instrument]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    #[tracing::instrument]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().inner.poll_shutdown(cx)
    }
}

#[tokio::main]
async fn main() {
    use tracing_subscriber::{
        filter::{filter_fn, LevelFilter},
        fmt::format::FmtSpan,
        layer::SubscriberExt as _,
        util::SubscriberInitExt as _,
    };
    let _guard = tracing_subscriber::fmt()
        // .pretty()
        .with_span_events(FmtSpan::FULL)
        .with_test_writer()
        .with_max_level(LevelFilter::TRACE)
        .finish()
        .with(filter_fn(|_metadata| true))
        .set_default();
    _main().await
}
