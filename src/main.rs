use dataset::Dataset;

mod dataset;
mod files;
mod commands;

fn main() {
    println!("{:?}", Dataset::load("titanic".to_string()));
}
