use std::{collections::HashMap, mem::take};

use crate::{files::{cache, load}, commands::{download, unzip}};

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
}

#[derive(Debug, Clone)]
pub enum Kind {
    Int,
    Float,
    Str,
}

#[derive(Debug, Clone)]
pub struct Dataset {
    name: String,
    schema: Vec<(String, Kind)>,
    data: Vec<ValueVec>,
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

    pub fn load(name: String) -> Self {
        Self::download(&name);
        
        let train = load(&name).join("train.csv");

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
            name,
            schema,
            data,
        }
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

    pub fn embed() -> (u32, u32, Box<[f64]>) {
        todo!()
    }
}
