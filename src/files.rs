use std::{env, path::PathBuf, fs};

pub fn cache() -> PathBuf {
    load("cache")
}

pub fn load(name: &str) -> PathBuf {
    let path = env::current_dir().unwrap().join(name);
        
    if !path.is_dir() {
        fs::create_dir(&path).unwrap();
    }

    path
}
