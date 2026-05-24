pub type SerializerId = u32;

pub trait RemoteMessage: Send + 'static {
    const MANIFEST: &'static str;
    const VERSION: u16;
}
