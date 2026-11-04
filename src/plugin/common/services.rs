//! Immutable after assembly, with service ownership retained by its registering plugin.
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};
#[derive(Default)]
pub struct Services {
    entries: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}
impl Services {
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: Arc<T>) -> Result<(), String> {
        let key = TypeId::of::<T>();
        if self.entries.contains_key(&key) {
            return Err(format!(
                "Duplicate plugin service: {}",
                std::any::type_name::<T>()
            ));
        }
        self.entries.insert(key, Box::new(value));
        Ok(())
    }
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&Arc<T>> {
        self.entries.get(&TypeId::of::<T>())?.downcast_ref()
    }
    pub fn require<T: Send + Sync + 'static>(&self) -> Result<&Arc<T>, crate::core::error::Error> {
        self.get::<T>()
            .ok_or_else(crate::core::error::Error::unsupported)
    }
    pub fn get_mut<T: Send + Sync + 'static>(&mut self) -> Option<&mut T> {
        Arc::get_mut(
            self.entries
                .get_mut(&TypeId::of::<T>())?
                .downcast_mut::<Arc<T>>()?,
        )
    }
}
