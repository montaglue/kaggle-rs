use std::mem::take;

use crate::{files::{cache, load}, commands::{download, unzip}, random_state::RandomState};

#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Float(f64),
    Str(String),
}

impl Value {
    pub fn int(self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(i),
            Value::Float(_) => None,
            Value::Str(_) => None,
        }
    }

    pub fn float(self) -> Option<f64> {
        match self {
            Value::Int(_) => None,
            Value::Float(f) => Some(f),
            Value::Str(_) => None,
        }
    }

    pub fn str(self) -> Option<String> {
        match self {
            Value::Int(_) => None,
            Value::Float(_) => None,
            Value::Str(s) => Some(s),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ValueVec {
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
    Str(Vec<Option<String>>),
}

impl ValueVec {
    pub fn new(kind: Kind) -> Self {
        match kind {
            Kind::Int => ValueVec::Int(Vec::new()),
            Kind::Float => ValueVec::Float(Vec::new()),
            Kind::Str => ValueVec::Str(Vec::new()),
        }
    }

    pub fn push(&mut self, elem: Option<Value>) {
        match self {
            ValueVec::Int(vec) => vec.push(elem.and_then(Value::int)),
            ValueVec::Float(vec) => vec.push(elem.and_then(Value::float)),
            ValueVec::Str(vec) => vec.push(elem.and_then(Value::str)),
        }
    }

    pub fn swap(&mut self, a: usize, b: usize) {
        match self {
            ValueVec::Int(vec) => vec.swap(a, b),
            ValueVec::Float(vec) => vec.swap(a, b),
            ValueVec::Str(vec) => vec.swap(a, b),
        }
    }

    pub fn split_off(&mut self, at: usize) -> Self {
        match self {
            ValueVec::Int(vec) => ValueVec::Int(vec.split_off(at)),
            ValueVec::Float(vec) => ValueVec::Float(vec.split_off(at)),
            ValueVec::Str(vec) => ValueVec::Str(vec.split_off(at)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Kind {
    Int,
    Float,
    Str,
}

#[derive(Debug, Clone)]
pub struct Dataset {
    pub name: String,
    pub schema: Vec<(String, Kind)>,
    pub data: Vec<ValueVec>,
    pub train: bool,
    pub has_target: bool,
}

impl Dataset {
    pub(crate) fn download(name: &str) {
        let data_dir = load(&name);

        if data_dir.read_dir().unwrap().next().is_some() {
            return;
        }

        let cache = cache();
        let file = cache.join(&name);

        if file.is_file() {
            return;
        }

        download(&cache, &name);
        unzip(&file, &data_dir);
    }

    pub fn load(dataset_name: &str, file_name: &str) -> Self {
        Self::download(dataset_name);
        
        let train = load(dataset_name).join(file_name);

        let mut reader =  csv::Reader::from_path(train).unwrap();

        let mut schema = Vec::new();
        for field in reader.headers().unwrap() {
            schema.push((field.to_string(), None));
        }
        
        let mut data = vec![ValueVec::new(Kind::Str); schema.len()];
        for record in reader.records() {
            let record = record.unwrap();

            for i in 0..schema.len() {
                if record[i].is_empty() {
                    data[i].push(None);
                    continue;
                }

                data[i].push(Some(Value::Str(record[i].to_string())));

                match schema[i].1 {
                    None | Some(Kind::Int) => {
                        if let Ok(_) = record[i].parse::<i64>() {
                            schema[i].1 = Some(Kind::Int);
                        } else if let Ok(_) = record[i].parse::<f64>() {
                            schema[i].1 = Some(Kind::Float);
                        } else {
                            schema[i].1 = Some(Kind::Str);
                        }
                    },
                    Some(Kind::Float) => {
                        if let Ok(_) = record[i].parse::<f64>() {
                            schema[i].1 = Some(Kind::Float);
                        } else {
                            schema[i].1 = Some(Kind::Str);
                        }
                    },
                    _ => {},
                }
            }
        }

        let schema = schema
            .into_iter()
            .map(|(a, b)| (a, b.unwrap_or(Kind::Str)))
            .collect::<Vec<_>>();


        for i in 0..schema.len() {
            match schema[i].1 {
                Kind::Int => {
                    match &mut data[i] {
                        ValueVec::Str(raw) => {
                            let raw = take(raw);
                            let vec = raw.into_iter().map(|x| x.map(|x| x.parse().unwrap())).collect();
                            data[i] = ValueVec::Int(vec);
                        },
                        _ => unreachable!(),
                    }
                },
                Kind::Float => {
                    match &mut data[i] {
                        ValueVec::Str(raw) => {
                            let raw = take(raw);
                            let vec = raw.into_iter().map(|x| x.map(|x| x.parse().unwrap())).collect();
                            data[i] = ValueVec::Float(vec);
                        },
                        _ => unreachable!(),
                    }
                },
                _ => (),
            }
        }

        Self {
            name: dataset_name.to_string(),
            schema,
            data,
            train: file_name == "train.csv",
            has_target: file_name == "train.csv",
        }
    } 

    pub fn load_test(name: &str) -> Self {
        Self::load(name, "test.csv")
    }

    pub fn load_train(name: &str) -> Self {
        Self::load(name, "train.csv")
    }

    pub fn set_target(&mut self, target: &str) {
        if self.has_target {
            for (i, col) in self.schema.iter().enumerate() {
                if &col.0 == target {
                    let len = self.schema.len();
                    self.schema.swap(i, len - 1);
                    self.data.swap(i, len - 1);
                    return;
                }
            }
        }
    }

    pub fn target(&self) -> ValueVec {
        self.data.last().unwrap().clone()
    }

    pub fn remove_nones(&mut self) {
        let mut mask = vec![false; self.len()];
        for field in &self.data {
            match field {
                ValueVec::Int(vec) => {
                    for (i, val) in vec.iter().enumerate() {
                        mask[i] |= val.is_none();
                    }
                },
                ValueVec::Float(vec) => {
                    for (i, val) in vec.iter().enumerate() {
                        mask[i] |= val.is_none();
                    }
                },
                ValueVec::Str(vec) => {
                    for (i, val) in vec.iter().enumerate() {
                        mask[i] |= val.is_none();
                    }
                },
            }
        }

        for field in &mut self.data {
            match field {
                ValueVec::Int(vec) => 
                    *vec = vec
                        .iter()
                        .enumerate()
                        .filter_map(|(i, x)| if mask[i] { Some(*x) } else { None })
                        .collect(),
                ValueVec::Float(vec) => 
                    *vec = vec
                        .iter()
                        .enumerate()
                        .filter_map(|(i, x)| if mask[i] { Some(*x) } else { None })
                        .collect(),
                ValueVec::Str(vec) => {
                    let values = take(vec);
                    *vec = values
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, x)| if mask[i] { Some(x) } else { None })
                        .collect();
                },
            }
        }
    } 

    pub fn replace_nones(&mut self, field: String, value: Value) {
        if let Some((index, _)) = self.schema.iter().enumerate().find(|(i, x)| &x.0 == &field) {
            todo!()
        }
    }

    pub fn len(&self) -> usize {
        match &self.data[0] {
            ValueVec::Int(vec) => vec.len(),
            ValueVec::Float(vec) => vec.len(),
            ValueVec::Str(vec) => vec.len(),
        }
    }

    pub fn split(mut self, test_size: Option<f64>, train_size: Option<f64>, random_state: &mut RandomState) -> (Self, Self) {
        assert!(self.train, "you can't split test dataset");

        let len = self.len();

        let test = match (test_size, train_size) {
            (Some(test), _) => {
                if test < 1.0 {
                    (len as f64 * test) as usize
                } else {
                    test as usize
                }
            },
            (_, Some(train)) => {
                if train < 1.0 {
                    (len as f64 * (1.0 - train)) as usize
                } else {
                    len - train as usize
                }
            },
            _ => {
                len * 7 / 10
            },
        };

        for i in 0..test {
            let index = random_state.gen() % (len - i);
            for col in self.data.iter_mut() {
                col.swap(index, len - i - 1);
            }
        }


        let train = len - test;

        let mut new_data = Vec::new();
        for col in self.data.iter_mut() {
            new_data.push(col.split_off(train));
        }

        let name = self.name;
        self.name = name.clone() + "_train";
        
        let test = Self {
            name: name + "_test",
            schema: self.schema.clone(),
            data: new_data,
            train: false,
            has_target: self.has_target,
        };

        (self, test)
    }
}
