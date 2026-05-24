//! Procedural macros for Kairo actor protocols.

use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn kairo_message(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_derive(KairoRemoteMessage, attributes(kairo))]
pub fn derive_kairo_remote_message(_item: TokenStream) -> TokenStream {
    TokenStream::new()
}
