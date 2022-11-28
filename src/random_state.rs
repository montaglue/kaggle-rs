use rand::{rngs::ThreadRng, Rng};

#[derive(Default)]
pub struct RandomState {
    inner: ThreadRng,
}

impl RandomState {
    pub fn gen(&mut self) -> usize {
        self.inner.gen()
    }

    pub fn new(inner: ThreadRng) -> RandomState {
        Self {
            inner,
        }
    }
}
