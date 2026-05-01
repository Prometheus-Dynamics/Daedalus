use std::marker::PhantomData;

use crate::TransportError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cpu<T>(pub T);

impl<T> Cpu<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for Cpu<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Cpu<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Gpu<T>(pub T);

impl<T> Gpu<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for Gpu<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Gpu<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub trait DeviceClass {
    const ID: &'static str;
    type Context: Send + Sync + 'static;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Device<D: DeviceClass, T> {
    value: T,
    _device: PhantomData<D>,
}

impl<D: DeviceClass, T> Device<D, T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            _device: PhantomData,
        }
    }

    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<D: DeviceClass, T> std::ops::Deref for Device<D, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<D: DeviceClass, T> std::ops::DerefMut for Device<D, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

pub trait TransferTo<D: DeviceClass>: Sized {
    type Resident: Send + Sync + 'static;

    fn transfer_to(&self, ctx: &D::Context) -> Result<Self::Resident, TransportError>;
}

pub trait TransferFrom<D: DeviceClass>: Sized {
    type Resident: Send + Sync + 'static;

    fn transfer_from(resident: &Self::Resident, ctx: &D::Context) -> Result<Self, TransportError>;
}

pub trait DeviceTransfer<D: DeviceClass>: TransferTo<D> + TransferFrom<D> {}

impl<D, T> DeviceTransfer<D> for T
where
    D: DeviceClass,
    T: TransferTo<D> + TransferFrom<D>,
{
}
