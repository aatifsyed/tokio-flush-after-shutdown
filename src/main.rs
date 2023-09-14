use async_compression::tokio::write::ZstdEncoder;
use bytes::Bytes;
use futures::{stream, StreamExt as _};
use std::io;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedWrite};

async fn _main() -> io::Result<()> {
    let file = File::create("/dev/null").await?;
    let zstd_encoder = ZstdEncoder::new(file);
    let bytes_sink = FramedWrite::new(zstd_encoder, BytesCodec::new());
    stream::empty::<io::Result<Bytes>>()
        .forward(bytes_sink)
        .await
}

#[tokio::main]
async fn main() -> io::Result<()> {
    _main().await
}
