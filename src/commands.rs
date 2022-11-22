use std::{process::Command, path::PathBuf};

pub fn download(dir: &PathBuf, name: &str){
    Command::new("kaggle")
        .arg("competitions")
        .arg("download")
        .arg("-c")
        .arg(name)
        .current_dir(dir)
        .output()
        .unwrap();
}

pub fn unzip(file: &PathBuf, to: &PathBuf) {
    Command::new("unzip")
        .arg(file)
        .arg("-d")
        .arg(to)
        .output()
        .unwrap();
}