use kaggle_rs::{prelude::*, random_state::RandomState};

use rusty_machine::analysis::score::*;
use rusty_machine::learning;
use rusty_machine::learning::gp;
use rusty_machine::learning::optim::grad_desc::GradientDesc;
use rusty_machine::learning::SupModel;
use rusty_machine::prelude::{Matrix, Vector};


pub fn embed(data: Dataset) -> (Matrix<f64>, Vector<f64>) {
    todo!()
}

fn main() {
    let dataset = Dataset::load_train("titanic");

    let mut random_state = RandomState::default();
    let (test, train) = dataset.split(None, None, &mut random_state);

    let training_data_matrix = todo!();
    let training_targets = todo!();
    let test_data_matrix = todo!();

    let mut model = gp::GaussianProcess::default();
    model.noise = 1f64;
    model.train(&training_data_matrix, &training_targets).unwrap();
    let outputs = model.predict(&test_data_matrix).unwrap();


    println!("{:?}", test);
}
