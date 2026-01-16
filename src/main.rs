use core::f32;
use std::collections::HashMap;
use std::io::BufWriter;
use std::path::Path;
use std::thread;

use memchr::{memchr, memchr_iter, memchr2};
use memmap2::Mmap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use std::fs::File;
use std::io::Write;

#[derive(Debug)]
struct Record {
    min: f32,
    max: f32,
    sum: f32,
    count: f32,
}

impl Record {
    fn recv(&mut self, degree: f32) {
        if self.max < degree {
            self.max = degree
        }

        if self.min > degree {
            self.min = degree
        }

        self.sum += degree;
        self.count += 1.0;
    }

    fn merge(&mut self, other: &Record) {
        if self.max < other.max {
            self.max = other.max
        }

        if self.min > other.min {
            self.min = other.min
        }

        self.sum += other.sum;
        self.count += other.count;
    }
}
impl Default for Record {
    fn default() -> Self {
        Self {
            min: f32::MAX,
            max: f32::MIN,
            sum: 0.0,
            count: 0.0,
        }
    }
}

struct MappedFile {
    map: Mmap,
}

impl MappedFile {
    fn new(path: impl AsRef<Path>) -> Option<Self> {
        let file = File::open(path).ok()?;
        let map = unsafe { memmap2::Mmap::map(&file).ok()? };
        Some(Self { map })
    }

    fn get(&self) -> &[u8] {
        &self.map[..]
    }
}

#[inline]
fn read_line(slice: &[u8]) -> (&[u8], &[u8]) {
    //    let slice = str::from_utf8(slice).ok().unwrap();

    memchr::memchr(b'\n', slice)
        .map(|idx| {
            let (line, rem) = slice.split_at(idx);
            let rem = rem.get(1..).unwrap();
            (line, rem)
        })
        .unwrap_or((slice, b"".as_slice()))
}

#[inline]
fn read_values(slice: &[u8]) -> Option<(&[u8], f32)> {
    let colon = memchr(b';', slice)?;
    let (name, number) = slice.split_at_checked(colon)?;
    let number = fast_float2::parse(&number[1..]).ok()?;
    Some((name, number))
}

#[test]
fn test_read_line() {
    assert_eq!(read_line(b"a\nb\nc"), (b"a".as_slice(), b"b\nc".as_slice()));
    assert_eq!(read_line(b"a\nb"), (b"a".as_slice(), b"b".as_slice()));
    assert_eq!(read_line(b"\n"), (b"".as_slice(), b"".as_slice()));
    assert_eq!(read_line(b""), (b"".as_slice(), b"".as_slice()));
}

#[test]
#[should_panic]
fn test_read_values() {
    assert_eq!(read_values(b"A;5.0"), Some((b"A".as_slice(), 5.0)));
    read_values(b";5.0");
    read_values(b"A;");
    read_values(b";");
    read_values(b"");
}

#[inline]
fn evenly_divide_slice(slice: &[u8], n: usize) -> Vec<&[u8]> {
    let chunk_size = slice.len() / n;
    let mut ret = Vec::with_capacity(n);
    let mut remaining = slice;

    while !remaining.is_empty() {
        let addition = remaining
            .get(chunk_size - 1)
            .filter(|&&ch| ch != b'\n')
            .map(|_| memchr::memchr(b'\n', &remaining[chunk_size..]).unwrap_or(remaining.len()))
            .unwrap_or(0);

        let (split, rem) = remaining
            .split_at_checked(chunk_size + addition + 1)
            .unwrap_or((remaining, b"".as_slice()));

        ret.push(split);
        remaining = rem;
    }
    ret
}

fn main() {
    let threads: usize = thread::available_parallelism().unwrap().into();
    let file = MappedFile::new(r#"D:\measurements-1000000000.txt"#).unwrap();
    let ret = evenly_divide_slice(file.get(), 4 * threads);
    let output = ret
        .par_iter()
        .fold(
            || HashMap::with_capacity(1000),
            |mut hmap, &val| {
                let mut remaining = val;
                while !remaining.is_empty() {
                    let line;
                    (line, remaining) = read_line(remaining);
                    let Some((name, degree)) = read_values(line) else {
                        continue;
                    };
                    let entry: &mut Record = hmap.entry(name).or_default();
                    entry.recv(degree);
                }
                hmap
            },
        )
        .reduce(
            || HashMap::with_capacity(1000),
            |mut mother, child| {
                for (k, v) in child {
                    let entry = mother.entry(k).or_default();
                    entry.merge(&v);
                }
                mother
            },
        );

    let mut out_file = BufWriter::new(File::create("output.csv").unwrap());

    output.iter().for_each(
        |(
            name,
            Record {
                min,
                max,
                sum,
                count,
            },
        )| {
            writeln!(
                out_file,
                "{},{min},{max},{}",
                str::from_utf8(name).unwrap(),
                sum / count
            )
            .unwrap();
        },
    );
}
