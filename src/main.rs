use core::f32;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, BufWriter};
use std::string::FromUtf16Error;

use rayon::iter::ParallelIterator;
use rayon::str::ParallelString;
//use tokio::fs::File;
//use tokio::io::{AsyncBufReadExt, BufReader};

use std::fs::{self, File};
use std::io::Write;

#[derive(Debug)]
struct Record {
    min: f32,
    max: f32,
    avg: f32,
    count: f32,
}

impl Default for Record {
    fn default() -> Self {
        Self {
            min: f32::MAX,
            max: f32::MIN,
            avg: 0.0,
            count: 0.0,
        }
    }
}
// TODO: To be optimized !!
fn main() {
    let content = fs::read_to_string("measurements-10000000.txt").unwrap();

    let res = content
        .par_lines()
        .filter_map(|line| {
            let mut split = line.split(";");
            let first = split.next()?;
            let second = split.next()?.parse::<f32>().ok()?;
            Some((first, second))
        })
        .fold(HashMap::<_, Record>::new, |mut acc, (station, degree)| {
            let rec = acc.entry(station).or_insert(Record::default());

            if degree > rec.max {
                rec.max = degree
            }

            if degree < rec.min {
                rec.min = degree
            }
            rec.count += 1.0;
            rec.avg = (1.0 / rec.count) * ((rec.count - 1.0) * rec.avg + degree);
            acc
        })
        .reduce(HashMap::new, |mut acc, other| {
            other.into_iter().for_each(|(k, v)| {
                acc.entry(k)
                    .and_modify(|rec| {
                        if v.min < rec.min {
                            rec.min = v.min;
                        }

                        if v.max > rec.max {
                            rec.max = v.max;
                        }

                        rec.avg = (rec.count * rec.avg + v.count * v.avg) / (v.count + rec.count);
                        rec.count += v.count;
                    })
                    .or_insert(v);
            });
            acc
        });

    let mut file = BufWriter::new(File::create("output.csv").unwrap());
    res.into_iter().for_each(
        |(
            k,
            Record {
                min,
                max,
                avg,
                count: _,
            },
        )| {
            writeln!(file, "{k};{min};{max};{avg:.1}").unwrap();
        },
    );
}
