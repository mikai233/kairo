use std::marker::PhantomData;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EntityTypeKey<M> {
    name: String,
    _message: PhantomData<fn(M)>,
}

impl<M> EntityTypeKey<M> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _message: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
