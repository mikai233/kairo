//! Procedural macros for Kairo actor protocols.
//!
//! This crate keeps macro support deliberately narrow. Local actor messages do
//! not need macros or serialization, and remote wire compatibility remains an
//! explicit contract. [`KairoRemoteMessage`] derives only the
//! `kairo_serialization::RemoteMessage` manifest/version metadata from a
//! `#[kairo(...)]` attribute; it does not choose serializer ids, generate
//! codecs, register codecs, or infer wire metadata from Rust type names.
//!
//! ```
//! use kairo_actor_macros::KairoRemoteMessage;
//! use kairo_serialization::RemoteMessage;
//!
//! #[derive(KairoRemoteMessage)]
//! #[kairo(manifest = "kairo.example.Created", version = 1)]
//! struct Created {
//!     id: String,
//! }
//!
//! assert_eq!(Created::MANIFEST, "kairo.example.Created");
//! assert_eq!(Created::VERSION, 1);
//! ```
//!
//! The `manifest` must be a stable, non-empty string and `version` must fit in
//! `u16`. Remote payload encoding still belongs in an explicit
//! `MessageCodec<M>` implementation registered with `kairo-serialization`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, LitInt, LitStr, parse_macro_input};

/// Marker attribute reserved for future actor protocol metadata.
///
/// The attribute currently leaves the annotated item unchanged. Local actor
/// messages do not need macro-generated metadata, and remote-capable messages
/// should use [`KairoRemoteMessage`] for stable manifest/version metadata.
#[proc_macro_attribute]
pub fn kairo_message(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

/// Derives `kairo_serialization::RemoteMessage` metadata for a remote protocol type.
///
/// The derive reads `#[kairo(manifest = "...", version = N)]` attributes and
/// emits only the stable manifest and version constants. It does not generate
/// codecs, serializer ids, or registry calls, and it does not infer wire
/// metadata from Rust type names, enum discriminants, or memory layout.
#[proc_macro_derive(KairoRemoteMessage, attributes(kairo))]
pub fn derive_kairo_remote_message(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    match expand_remote_message(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand_remote_message(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "KairoRemoteMessage does not support generic message types yet",
        ));
    }

    let ident = input.ident;
    let metadata = RemoteMessageMetadata::parse(&input.attrs)?;
    let manifest = metadata.manifest;
    let version = metadata.version;

    Ok(quote! {
        impl ::kairo_serialization::RemoteMessage for #ident {
            const MANIFEST: &'static str = #manifest;
            const VERSION: u16 = #version;
        }
    })
}

struct RemoteMessageMetadata {
    manifest: String,
    version: u16,
}

impl RemoteMessageMetadata {
    fn parse(attrs: &[syn::Attribute]) -> syn::Result<Self> {
        let mut manifest = None;
        let mut version = None;

        for attr in attrs.iter().filter(|attr| attr.path().is_ident("kairo")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("manifest") {
                    if manifest.is_some() {
                        return Err(meta.error("duplicate manifest"));
                    }
                    let value = meta.value()?;
                    let lit: LitStr = value.parse()?;
                    let value = lit.value();
                    if value.trim().is_empty() {
                        return Err(meta.error("manifest must not be empty"));
                    }
                    manifest = Some(value);
                    Ok(())
                } else if meta.path.is_ident("version") {
                    if version.is_some() {
                        return Err(meta.error("duplicate version"));
                    }
                    let value = meta.value()?;
                    let lit: LitInt = value.parse()?;
                    version = Some(lit.base10_parse::<u16>()?);
                    Ok(())
                } else {
                    Err(meta.error("unsupported kairo remote message attribute"))
                }
            })?;
        }

        let Some(manifest) = manifest else {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "missing #[kairo(manifest = \"...\")]",
            ));
        };
        let Some(version) = version else {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "missing #[kairo(version = N)]",
            ));
        };

        Ok(Self { manifest, version })
    }
}
