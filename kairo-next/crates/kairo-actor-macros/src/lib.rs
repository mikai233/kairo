//! Procedural macros for Kairo actor protocols.

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, LitInt, LitStr, parse_macro_input};

#[proc_macro_attribute]
pub fn kairo_message(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

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
